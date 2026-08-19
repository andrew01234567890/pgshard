// Package vstream serves pgshard.v1.VStream on the router: it fans the
// per-shard change streams of one named stream (read through the shards'
// poolers) into a single event stream with vector positions, forwards acks
// to each shard's slot and follows primaries across failovers.
package vstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// Server implements pgshard.v1.VStream.
type Server struct {
	pgshardv1.UnimplementedVStreamServer
	Topology Topology
	Catalog  Catalog
	// Controller creates and drops streams; nil answers Unimplemented.
	Controller pgshardv1.ControllerClient
	Logger     *slog.Logger
	// BufferUnits is how many assembled transactions a shard may run ahead
	// of the consumer; zero means 16.
	BufferUnits int
	// ReconnectWindow bounds how long a shard stream may stay broken before
	// the stream ends with SHARD_UNAVAILABLE; zero means 30s.
	ReconnectWindow time.Duration
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// Create implements VStream.Create through the controller.
func (s *Server) Create(ctx context.Context, req *pgshardv1.CreateVStreamRequest) (*pgshardv1.CreateVStreamResponse, error) {
	if !catalog.ValidStreamName(req.GetStream()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream name %q", req.GetStream())
	}
	if s.Controller == nil {
		return nil, status.Error(codes.Unimplemented, "stream creation needs a controller endpoint (--controller)")
	}
	r, err := s.Controller.CreateStream(ctx, &pgshardv1.CreateStreamRequest{Stream: req.GetStream(), Database: req.GetDatabase(), TwoPhase: req.GetTwoPhase()})
	if err != nil {
		return nil, err
	}
	resp := &pgshardv1.CreateVStreamResponse{Error: r.GetError()}
	for _, sl := range r.GetSlots() {
		resp.Slots = append(resp.Slots, &pgshardv1.VStreamSlot{Shard: sl.GetShard(), Slot: sl.GetSlot(), ConfirmedFlushLsn: sl.GetLsn()})
	}
	return resp, nil
}

// Drop implements VStream.Drop through the controller.
func (s *Server) Drop(ctx context.Context, req *pgshardv1.DropVStreamRequest) (*pgshardv1.DropVStreamResponse, error) {
	if s.Controller == nil {
		return nil, status.Error(codes.Unimplemented, "stream removal needs a controller endpoint (--controller)")
	}
	r, err := s.Controller.DropStream(ctx, &pgshardv1.DropStreamRequest{Stream: req.GetStream()})
	if err != nil {
		return nil, err
	}
	return &pgshardv1.DropVStreamResponse{Error: r.GetError()}, nil
}

// List implements VStream.List from the catalog.
func (s *Server) List(ctx context.Context, _ *pgshardv1.ListVStreamsRequest) (*pgshardv1.ListVStreamsResponse, error) {
	streams, statuses, err := s.Catalog.List(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	slots := map[string][]*pgshardv1.VStreamSlot{}
	for _, st := range statuses {
		slots[st.Stream] = append(slots[st.Stream], &pgshardv1.VStreamSlot{Shard: &pgshardv1.ShardRef{ShardSet: st.ShardSet, ShardId: uint32(st.ShardID)},
			Slot: st.Slot, WalStatus: st.WALStatus, ConfirmedFlushLsn: st.ConfirmedFlushLSN, Active: st.Active})
	}
	resp := &pgshardv1.ListVStreamsResponse{}
	for _, st := range streams {
		resp.Streams = append(resp.Streams, &pgshardv1.VStreamInfo{Stream: st.Name, Database: st.Database, TwoPhase: st.TwoPhase, State: st.State, Slots: slots[st.Name]})
	}
	return resp, nil
}

// Ack implements VStream.Ack: every shard in the position is acked on its
// pooler, which requires an open Stream reader on that pooler.
func (s *Server) Ack(ctx context.Context, req *pgshardv1.VStreamAckRequest) (*pgshardv1.VStreamAckResponse, error) {
	if !catalog.ValidStreamName(req.GetStream()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid stream name %q", req.GetStream())
	}
	for sh, lsn := range positionFrom(req.GetPosition()) {
		if err := s.ackShard(ctx, req.GetStream(), sh, lsn); err != nil {
			return &pgshardv1.VStreamAckResponse{Error: &pgshardv1.Error{Message: fmt.Sprintf("shard %s/%d: %v", sh.Set, sh.ID, err)}}, nil
		}
	}
	return &pgshardv1.VStreamAckResponse{}, nil
}

func (s *Server) ackShard(ctx context.Context, stream string, sh router.Shard, lsn uint64) error {
	client, err := s.Topology.Client(sh)
	if err != nil {
		return err
	}
	r, err := client.Ack(ctx, &pgshardv1.AckRequest{Stream: stream, Lsn: lsn})
	if err != nil {
		return err
	}
	if r.GetError() != nil {
		return errors.New(r.GetError().GetMessage())
	}
	return nil
}

// Stream implements VStream.Stream.
func (s *Server) Stream(srv pgshardv1.VStream_StreamServer) error {
	first, err := srv.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "the first request must be a start")
	}
	ctx, cancel := context.WithCancel(srv.Context())
	defer cancel()
	def, err := s.Catalog.Lookup(ctx, start.GetStream())
	if errors.Is(err, ErrUnknownStream) {
		return status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	opts := resolveOptions(start.GetOptions())
	if opts.twoPhase && !def.TwoPhase {
		return status.Errorf(codes.FailedPrecondition, "stream %q was not created with two_phase", def.Name)
	}
	set := start.GetOptions().GetShardSet()
	if set == "" {
		set = router.DefaultShardSet
	}
	shards := sortedShards(s.Topology.Shards(set))
	if len(shards) == 0 {
		return status.Errorf(codes.FailedPrecondition, "shard set %q has no serving shards", set)
	}
	gen := s.Topology.Generation()
	send := lockedSender(srv.Send)
	if pg := start.GetPosition().GetShardMapGeneration(); pg != 0 && pg != gen {
		m := &merger{shards: shards, topo: s.Topology, generation: pg, opts: opts, send: send}
		return m.resharded()
	}
	startPos := positionFrom(start.GetPosition())
	copying := copyStateFrom(start.GetPosition())
	buffer := s.BufferUnits
	if buffer <= 0 {
		buffer = 16
	}
	window := s.ReconnectWindow
	if window <= 0 {
		window = 30 * time.Second
	}
	inputs := map[router.Shard]chan *unit{}
	ready := make(chan struct{}, 1)
	var wg sync.WaitGroup
	for _, sh := range shards {
		ch := make(chan *unit, buffer)
		inputs[sh] = ch
		r := &reader{shard: sh, stream: def.Name, database: def.Database, twoPhase: opts.twoPhase, topo: s.Topology,
			out: ch, ready: ready, window: window, delivered: startPos[sh]}
		if st, ok := copying[sh]; ok {
			r.copy = copyPhaseFrom(st, opts.copyBatch)
		} else if opts.copy && startPos[sh] == 0 {
			r.copy = copyPhaseFrom(nil, opts.copyBatch)
			copying[sh] = r.copy.state(sh)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.run(ctx)
		}()
	}
	ackers := newAckers(ctx, &wg, func(sh router.Shard, lsn uint64) error { return s.ackShard(ctx, def.Name, sh, lsn) }, s.logger())
	acks := make(chan *pgshardv1.VPosition, 16)
	go func() {
		for {
			req, err := srv.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					cancel()
				}
				return
			}
			if pos := req.GetAck(); pos != nil {
				select {
				case acks <- pos:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	m := &merger{shards: shards, inputs: inputs, ready: ready, acks: acks, acker: ackers.request, send: send,
		topo: s.Topology, generation: gen, opts: opts, position: startPos, copying: copying}
	err = m.run(ctx)
	cancel()
	wg.Wait()
	return err
}

func lockedSender(send func(*pgshardv1.VEvent) error) func(*pgshardv1.VEvent) error {
	var mu sync.Mutex
	return func(ev *pgshardv1.VEvent) error {
		mu.Lock()
		defer mu.Unlock()
		return send(ev)
	}
}

// ackers forwards acks per shard from one goroutine each, coalescing to
// the newest requested LSN so a slow pooler never queues stale acks.
type ackers struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	ack    func(router.Shard, uint64) error
	logger *slog.Logger
	mu     sync.Mutex
	want   map[router.Shard]uint64
	wake   map[router.Shard]chan struct{}
}

func newAckers(ctx context.Context, wg *sync.WaitGroup, ack func(router.Shard, uint64) error, logger *slog.Logger) *ackers {
	return &ackers{ctx: ctx, wg: wg, ack: ack, logger: logger, want: map[router.Shard]uint64{}, wake: map[router.Shard]chan struct{}{}}
}

func (a *ackers) request(sh router.Shard, lsn uint64) {
	a.mu.Lock()
	if lsn <= a.want[sh] {
		a.mu.Unlock()
		return
	}
	a.want[sh] = lsn
	wake, ok := a.wake[sh]
	if !ok {
		wake = make(chan struct{}, 1)
		a.wake[sh] = wake
		a.wg.Add(1)
		go a.loop(sh, wake)
	}
	a.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (a *ackers) loop(sh router.Shard, wake chan struct{}) {
	defer a.wg.Done()
	var done uint64
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-wake:
		}
		for {
			a.mu.Lock()
			lsn := a.want[sh]
			a.mu.Unlock()
			if lsn <= done {
				break
			}
			if err := a.ack(sh, lsn); err != nil {
				if a.ctx.Err() != nil {
					return
				}
				a.logger.Warn("vstream: ack failed", "shard", fmt.Sprintf("%s/%d", sh.Set, sh.ID), "lsn", lsn, "err", err)
				break
			}
			done = lsn
		}
	}
}
