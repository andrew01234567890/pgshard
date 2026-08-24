package pooler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/metrics"
	"github.com/andrew01234567890/pgshard/internal/pgrepl"
)

// Config wires a Server.
type Config struct {
	Pool   *Pool
	Source Source
	// Dialer is used for out-of-band CancelRequest connections.
	Dialer Dialer
	// Database is the PostgreSQL database every backend connects to; the
	// wire identity carries only the role, so the shard has one database.
	Database string
	Logger   *slog.Logger
	// HealthInterval spaces Health stream updates; zero means 1s.
	HealthInterval time.Duration
	// ReserveTimeout releases a reserved session that has had no Execute
	// stream for this long (its router went away without Release); zero
	// means 5m.
	ReserveTimeout time.Duration
	// Stream configures the change-stream RPCs.
	Stream StreamConfig
	// Metrics receives pooler metric events; nil disables them.
	Metrics *metrics.Pooler
}

func (s *Server) notePrepared(hit bool) {
	if s.cfg.Metrics == nil {
		return
	}
	if hit {
		s.cfg.Metrics.PreparedHits.Inc()
	} else {
		s.cfg.Metrics.PreparedMisses.Inc()
	}
}

func (s *Server) noteStreamEnd(end pgrepl.LSN, r *streamReader) {
	if s.cfg.Metrics == nil {
		return
	}
	lag := uint64(end) - min(uint64(end), r.acked.Load())
	s.cfg.Metrics.StreamLagBytes.Set(float64(lag))
}

// Server implements the Pooler gRPC service.
type Server struct {
	pgshardv1.UnimplementedPoolerServer
	cfg Config

	mu       sync.Mutex
	sessions map[string]*session
	// readers holds the one admitted change-stream reader per slot.
	readers  map[string]*streamReader
	draining atomic.Bool
	closed   atomic.Bool
}

// session is the pooler-side state for one router session.
type session struct {
	id       string
	database string
	role     string
	reserved bool
	attached bool
	// detached is closed when the current Execute stream ends; Release
	// waits on it when it arrives while the stream is still attached.
	detached chan struct{}
	// detachedAt is when a reserved session lost its Execute stream.
	detachedAt time.Time
	// b is the backend currently held; nil when the session is between
	// stateless batches.
	b *Backend
}

// NewServer builds a Server; Register attaches it to a gRPC server.
func NewServer(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = time.Second
	}
	if cfg.ReserveTimeout <= 0 {
		cfg.ReserveTimeout = 5 * time.Minute
	}
	return &Server{cfg: cfg, sessions: map[string]*session{}}
}

// Register registers the service on g.
func (s *Server) Register(g *grpc.Server) { pgshardv1.RegisterPoolerServer(g, s) }

var errUnavailable = status.Error(codes.Unavailable, "pooler is draining")

func (s *Server) session(id string) *session {
	s.expireReservations(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	if se, ok := s.sessions[id]; ok {
		return se
	}
	se := &session{id: id}
	s.sessions[id] = se
	return se
}

func (s *Server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *Server) forget(se *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[se.id] == se {
		delete(s.sessions, se.id)
	}
}

// Execute relays pgwire-shaped messages for one router session.
func (s *Server) Execute(stream pgshardv1.Pooler_ExecuteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.User == nil || first.User.Username == "" {
		return status.Error(codes.InvalidArgument, "first Execute message must carry the user identity")
	}
	if first.SessionId == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}
	ck, sk := append([]byte(nil), first.User.ScramClientKey...), append([]byte(nil), first.User.ScramServerKey...)
	zero(first.User.ScramClientKey)
	zero(first.User.ScramServerKey)
	if s.draining.Load() {
		return errUnavailable
	}
	se := s.session(first.SessionId)
	s.mu.Lock()
	if se.attached {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "session already has an Execute stream")
	}
	se.attached = true
	se.detached = make(chan struct{})
	se.detachedAt = time.Time{}
	se.role = first.User.Username
	se.database = s.cfg.Database
	s.mu.Unlock()
	defer s.detach(se)
	// Zeroise the working key copies before the session is detached, so a
	// caller that observes the session gone also observes the zeroed keys.
	defer func() { zero(ck); zero(sk) }()

	rs := &relay{srv: s, se: se, stream: stream, ck: ck, sk: sk}
	req := first
	for {
		if err := rs.handle(stream.Context(), req); err != nil {
			return err
		}
		req, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server) detach(se *session) {
	s.mu.Lock()
	se.attached = false
	b := se.b
	keep := se.reserved
	if keep {
		se.detachedAt = time.Now()
	} else {
		se.b = nil
	}
	detached := se.detached
	s.mu.Unlock()
	if !keep {
		s.recycle(b, true)
		s.forget(se)
	}
	close(detached)
}

// recycle returns b to the pool clean: an open transaction is rolled back,
// and DISCARD ALL resets it when a session held it (GUCs may be staged) or
// it still holds statements. A backend that cannot be cleaned is discarded.
func (s *Server) recycle(b *Backend, resetSession bool) {
	if b == nil {
		return
	}
	if b.hasUnflushed() {
		s.cfg.Pool.Discard(b)
		return
	}
	if !b.idle() {
		if err := b.simpleQuery("ROLLBACK"); err != nil {
			s.cfg.Pool.Discard(b)
			return
		}
	}
	if resetSession || len(b.prepared) > 0 {
		if err := b.simpleQuery("DISCARD ALL"); err != nil {
			s.cfg.Pool.Discard(b)
			return
		}
		b.prepared = nil
	}
	s.cfg.Pool.Release(b)
}

type relay struct {
	srv    *Server
	se     *session
	stream pgshardv1.Pooler_ExecuteServer
	ck, sk []byte
	// closes queues, in order, what to do with each CloseComplete the
	// backend will send for the current batch.
	closes []closeAction
	// parsed names the statements the current batch parsed; they become
	// certain when the batch ends without an error.
	parsed   []string
	batchErr bool
}

func (r *relay) send(msg *pgshardv1.ExecuteResponse) error {
	msg.SessionId = r.se.id
	return r.stream.Send(msg)
}

func (r *relay) refuse(e *pgshardv1.Error) error {
	// A refused request must not leave buffered, never-flushed messages
	// behind: a later Sync, Release (ROLLBACK/DISCARD ALL) or reuse would
	// flush them into PostgreSQL after the client was told they failed.
	r.dropUnflushed()
	if err := r.send(errorResponse(r.se.id, e)); err != nil {
		return err
	}
	st := byte('I')
	if b := r.backend(); b != nil {
		st = b.txStatus
	}
	return r.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_ReadyForQuery{
		ReadyForQuery: &pgshardv1.ReadyForQuery{TxnStatus: txnStatus(st)}}})
}

// dropUnflushed discards a held backend whose pipeline still holds unflushed
// messages, so those messages can never reach PostgreSQL.
func (r *relay) dropUnflushed() {
	b := r.backend()
	if b == nil || !b.hasUnflushed() {
		return
	}
	r.srv.cfg.Pool.Discard(b)
	r.setBackend(nil)
	// The discarded backend owed replies the injected closes were waiting
	// for; a fresh backend must start with a clean reply queue.
	r.closes, r.parsed, r.batchErr = nil, nil, false
}

func (r *relay) backend() *Backend {
	r.srv.mu.Lock()
	defer r.srv.mu.Unlock()
	return r.se.b
}

func (r *relay) reserved() bool {
	r.srv.mu.Lock()
	defer r.srv.mu.Unlock()
	return r.se.reserved
}

// setBackend hands b to the session. It reports false, leaving the session
// empty, when a Drain began after b was acquired: Drain releases only what
// sessions held when it looked, so a backend adopted afterwards would run
// on a pool that is closing.
func (r *relay) setBackend(b *Backend) bool {
	r.srv.mu.Lock()
	defer r.srv.mu.Unlock()
	if b != nil && r.srv.draining.Load() {
		return false
	}
	r.se.b = b
	return true
}

func (r *relay) handle(ctx context.Context, req *pgshardv1.ExecuteRequest) error {
	if req.SessionId != r.se.id {
		return status.Error(codes.InvalidArgument, "session_id changed mid-stream")
	}
	if e := fence(r.srv.cfg.Source.View(), req.Generation); e != nil {
		return r.refuse(e)
	}
	if _, ok := req.Message.(*pgshardv1.ExecuteRequest_Cancel); ok {
		if b := r.backend(); b != nil {
			if err := b.cancel(ctx, r.srv.cfg.Dialer); err != nil {
				r.srv.cfg.Logger.Warn("cancel failed", "session", r.se.id, "err", err)
			}
		}
		return nil
	}
	fm, err := toFrontend(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	b := r.backend()
	if b == nil {
		if r.srv.draining.Load() {
			return r.refuse(&pgshardv1.Error{Sqlstate: "57P03", Message: "pooler is draining"})
		}
		b, err = r.srv.cfg.Pool.Acquire(ctx, r.se.database, r.se.role, r.ck, r.sk)
		if err != nil {
			r.srv.cfg.Logger.Warn("acquire failed", "session", r.se.id, "role", r.se.role, "err", err)
			return r.refuse(&pgshardv1.Error{Sqlstate: "53300", Message: "no backend available: " + err.Error()})
		}
		if !r.setBackend(b) {
			r.srv.cfg.Pool.Discard(b)
			return r.refuse(&pgshardv1.Error{Sqlstate: "57P03", Message: "pooler is draining"})
		}
	}
	r.forward(b, fm)
	if !flushesBackend(req) {
		return nil
	}
	if err := b.flush(); err != nil {
		return r.backendLost(b, err)
	}
	return r.pump(b)
}

// forward buffers fm on b, keeping the backend's prepared-statement set in
// step: a Parse of a name the backend certainly holds with the same SQL is
// answered without reaching PostgreSQL, a name it may hold is closed first,
// and a Close or a DEALLOCATE-like simple query leaves the name in doubt.
func (r *relay) forward(b *Backend, fm pgproto3.FrontendMessage) {
	switch m := fm.(type) {
	case *pgproto3.Parse:
		if touchesPrepared(m.Query) {
			b.prepared.doubtAll()
		}
		if m.Name == "" {
			break
		}
		fp := statementFingerprint(m)
		if b.prepared.holds(m.Name, fp) {
			r.srv.notePrepared(true)
			b.send(&pgproto3.Close{ObjectType: 'P', Name: noopPortal})
			r.closes = append(r.closes, closeAsParse)
			return
		}
		r.srv.notePrepared(false)
		if b.prepared.mayHold(m.Name) {
			b.send(&pgproto3.Close{ObjectType: 'S', Name: m.Name})
			r.closes = append(r.closes, closeInjected)
		}
		if b.prepared == nil {
			b.prepared = preparedSet{}
		}
		b.prepared[m.Name] = preparedState{fingerprint: fp}
		r.parsed = append(r.parsed, m.Name)
	case *pgproto3.Close:
		if m.ObjectType == 'S' && m.Name != "" {
			if _, ok := b.prepared[m.Name]; ok {
				b.prepared[m.Name] = preparedState{}
			}
		}
		r.closes = append(r.closes, closeForwarded)
	case *pgproto3.Query:
		if touchesPrepared(m.String) {
			b.prepared.doubtAll()
		}
	}
	b.send(fm)
}

// endBatch settles the batch's bookkeeping at ReadyForQuery.
func (r *relay) endBatch(b *Backend) {
	if !r.batchErr {
		for _, name := range r.parsed {
			if st, ok := b.prepared[name]; ok && st.fingerprint != "" {
				st.certain = true
				b.prepared[name] = st
			}
		}
	}
	r.closes, r.parsed, r.batchErr = r.closes[:0], r.parsed[:0], false
}

// pump forwards backend responses until the batch ends: ReadyForQuery, or a
// COPY-in start where the router must speak next.
func (r *relay) pump(b *Backend) error {
	for {
		msg, err := b.receive()
		if err != nil {
			return r.backendLost(b, err)
		}
		switch msg.(type) {
		case *pgproto3.ErrorResponse:
			r.batchErr = true
		case *pgproto3.CloseComplete:
			if len(r.closes) > 0 {
				kind := r.closes[0]
				r.closes = r.closes[1:]
				switch kind {
				case closeInjected:
					continue
				case closeAsParse:
					msg = &pgproto3.ParseComplete{}
				}
			}
		}
		if resp := toResponse(msg); resp != nil {
			if err := r.send(resp); err != nil {
				return err
			}
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			r.endBatch(b)
			if !r.reserved() && b.idle() {
				r.setBackend(nil)
				r.srv.recycle(b, false)
			}
			return nil
		case *pgproto3.CopyInResponse, *pgproto3.CopyBothResponse:
			return nil
		}
	}
}

func (r *relay) backendLost(b *Backend, cause error) error {
	r.closes, r.parsed, r.batchErr = nil, nil, false
	r.setBackend(nil)
	r.srv.cfg.Pool.Discard(b)
	r.srv.cfg.Logger.Warn("backend connection lost", "session", r.se.id, "err", cause)
	return r.refuse(&pgshardv1.Error{Sqlstate: "08006", Message: "backend connection lost: " + cause.Error()})
}

// Reserve pins the session's backend for session-bound work. A session with
// no backend held yet is marked reserved and pins the next one it acquires.
func (s *Server) Reserve(_ context.Context, req *pgshardv1.ReserveRequest) (*pgshardv1.ReserveResponse, error) {
	if s.draining.Load() {
		return nil, errUnavailable
	}
	if e := fence(s.cfg.Source.View(), req.Generation); e != nil {
		return &pgshardv1.ReserveResponse{Error: e}, nil
	}
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	se := s.session(req.SessionId)
	s.mu.Lock()
	defer s.mu.Unlock()
	se.reserved = true
	if !se.attached {
		se.detachedAt = time.Now()
	}
	var pid int32
	if se.b != nil {
		pid = int32(se.b.pid)
	}
	return &pgshardv1.ReserveResponse{BackendPid: pid}, nil
}

// Release unpins a reserved session: any open transaction is rolled back,
// session state is discarded, and the backend returns to its pool.
func (s *Server) Release(ctx context.Context, req *pgshardv1.ReleaseRequest) (*pgshardv1.ReleaseResponse, error) {
	se := s.lookup(req.SessionId)
	if se == nil {
		return &pgshardv1.ReleaseResponse{}, nil
	}
	s.mu.Lock()
	if se.attached {
		// The router tore its stream down without waiting; the backend is
		// recycled when that stream detaches, and the caller waits for it
		// so a following Reserve never finds the old backend still held.
		se.reserved = false
		detached := se.detached
		s.mu.Unlock()
		select {
		case <-detached:
			return &pgshardv1.ReleaseResponse{}, nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	b := se.b
	se.b, se.reserved = nil, false
	s.mu.Unlock()
	s.forget(se)
	s.recycle(b, true)
	return &pgshardv1.ReleaseResponse{}, nil
}

// expireReservations releases reserved sessions whose Execute stream has
// been gone for ReserveTimeout: their router died without Release and
// would otherwise hold the backend and the session entry forever.
func (s *Server) expireReservations(now time.Time) {
	var expired []*session
	s.mu.Lock()
	for id, se := range s.sessions {
		if se.reserved && !se.attached && !se.detachedAt.IsZero() && now.Sub(se.detachedAt) >= s.cfg.ReserveTimeout {
			delete(s.sessions, id)
			expired = append(expired, se)
		}
	}
	s.mu.Unlock()
	for _, se := range expired {
		s.cfg.Logger.Warn("releasing reservation with no stream", "session", se.id, "role", se.role)
		s.recycle(se.b, true)
	}
}

// Cancel interrupts the statement running on the session's backend.
func (s *Server) Cancel(ctx context.Context, req *pgshardv1.CancelRequest) (*pgshardv1.CancelResponse, error) {
	se := s.lookup(req.SessionId)
	if se == nil {
		return &pgshardv1.CancelResponse{}, nil
	}
	s.mu.Lock()
	b := se.b
	s.mu.Unlock()
	if b != nil {
		if err := b.cancel(ctx, s.cfg.Dialer); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}
	return &pgshardv1.CancelResponse{}, nil
}

// Health streams the Source view until the client goes away.
func (s *Server) Health(_ *pgshardv1.HealthRequest, stream pgshardv1.Pooler_HealthServer) error {
	t := time.NewTicker(s.cfg.HealthInterval)
	defer t.Stop()
	for {
		s.expireReservations(time.Now())
		v := s.cfg.Source.View()
		if err := stream.Send(&pgshardv1.HealthStatus{Role: v.Role, ReplayLagBytes: v.LagBytes,
			Epoch: v.Epoch, Generation: v.Generation, Serving: v.Serving && !s.draining.Load()}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-t.C:
		}
	}
}

// Drain stops admitting new sessions and reservations, waits until no
// session holds a backend (in-flight transactions finish) or ctx expires,
// then closes every backend and the pool.
func (s *Server) Drain(ctx context.Context) error {
	s.draining.Store(true)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	var werr error
wait:
	for s.held() > 0 {
		select {
		case <-ctx.Done():
			werr = fmt.Errorf("drain: %d sessions still held a backend: %w", s.held(), ctx.Err())
			break wait
		case <-t.C:
		}
	}
	s.mu.Lock()
	var held []*Backend
	for id, se := range s.sessions {
		if se.b != nil {
			held = append(held, se.b)
			se.b = nil
		}
		delete(s.sessions, id)
	}
	s.mu.Unlock()
	for _, b := range held {
		s.cfg.Pool.Discard(b)
	}
	s.cfg.Pool.Close()
	s.closed.Store(true)
	return werr
}

func (s *Server) held() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, se := range s.sessions {
		if se.b != nil {
			n++
		}
	}
	return n
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
