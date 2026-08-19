package pooler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgoutput"
	"github.com/andrew01234567890/pgshard/internal/pgrepl"
)

// StreamConfig wires the change-stream RPCs of a Server.
type StreamConfig struct {
	// DSN of the replication connection (replication=database is added);
	// empty disables Stream.
	DSN string
	// Shard is the group name used to derive slot names from stream names.
	Shard string
	// Heartbeat is the idle interval between Keepalive batches; zero means 5s.
	Heartbeat time.Duration
	// ReceiveTimeout bounds one wait for server data so acks and heartbeats
	// are serviced; zero means 250ms.
	ReceiveTimeout time.Duration
	// MaxBatchBytes caps a batch; zero means 64 KiB.
	MaxBatchBytes int
}

// streamReader is the one admitted reader of a slot.
type streamReader struct {
	acked     atomic.Uint64
	flushed   atomic.Uint64
	delivered atomic.Uint64
	wake      chan struct{}
}

func (s *Server) streamDefaults() StreamConfig {
	c := s.cfg.Stream
	if c.Heartbeat <= 0 {
		c.Heartbeat = 5 * time.Second
	}
	if c.ReceiveTimeout <= 0 {
		c.ReceiveTimeout = 250 * time.Millisecond
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 64 << 10
	}
	return c
}

func (s *Server) slotOf(slot, stream string) (string, error) {
	if slot != "" {
		return slot, nil
	}
	if !catalog.ValidStreamName(stream) {
		return "", status.Errorf(codes.InvalidArgument, "slot or a valid stream name is required")
	}
	return catalog.StreamSlotName(stream, s.cfg.Stream.Shard), nil
}

func (s *Server) claimSlot(slot string) (*streamReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readers == nil {
		s.readers = map[string]*streamReader{}
	}
	if _, busy := s.readers[slot]; busy {
		return nil, status.Errorf(codes.FailedPrecondition, "slot %s already has an active reader", slot)
	}
	r := &streamReader{wake: make(chan struct{}, 1)}
	s.readers[slot] = r
	return r, nil
}

func (s *Server) releaseSlot(slot string, r *streamReader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readers[slot] == r {
		delete(s.readers, slot)
	}
}

// Stream implements Pooler.Stream.
func (s *Server) Stream(req *pgshardv1.StreamRequest, srv pgshardv1.Pooler_StreamServer) error {
	return s.runStream(srv.Context(), req, srv.Send, false)
}

// StreamChanges implements Pooler.StreamChanges: the same stream, one event
// per message.
func (s *Server) StreamChanges(req *pgshardv1.StreamRequest, srv pgshardv1.Pooler_StreamChangesServer) error {
	return s.runStream(srv.Context(), req, func(b *pgshardv1.ChangeBatch) error {
		for _, ev := range b.Events {
			if err := srv.Send(ev); err != nil {
				return err
			}
		}
		return nil
	}, true)
}

// Ack implements Pooler.Ack: it hands the position to the slot's reader and
// waits until the reader has reported it to the server. Positions beyond the
// last delivered batch end are clamped so confirmed_flush never overtakes
// what the client has actually seen.
func (s *Server) Ack(ctx context.Context, req *pgshardv1.AckRequest) (*pgshardv1.AckResponse, error) {
	slot, err := s.slotOf(req.GetSlot(), req.GetStream())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	r := s.readers[slot]
	s.mu.Unlock()
	if r == nil {
		return &pgshardv1.AckResponse{Error: &pgshardv1.Error{Sqlstate: "55000", Message: "slot " + slot + " has no active reader"}}, nil
	}
	lsn := min(req.GetLsn(), r.delivered.Load())
	for {
		cur := r.acked.Load()
		if lsn <= cur || r.acked.CompareAndSwap(cur, lsn) {
			break
		}
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
	deadline := time.Now().Add(10 * time.Second)
	for r.flushed.Load() < lsn {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return &pgshardv1.AckResponse{Error: &pgshardv1.Error{Sqlstate: "57014", Message: "ack not confirmed in time"}}, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return &pgshardv1.AckResponse{}, nil
}

func (s *Server) runStream(ctx context.Context, req *pgshardv1.StreamRequest, emit func(*pgshardv1.ChangeBatch) error, perEvent bool) error {
	cfg := s.streamDefaults()
	if cfg.DSN == "" {
		return status.Error(codes.FailedPrecondition, "change streams are not configured on this pooler")
	}
	if s.draining.Load() {
		return errUnavailable
	}
	slot, err := s.slotOf(req.GetSlot(), req.GetStream())
	if err != nil {
		return err
	}
	reader, err := s.claimSlot(slot)
	if err != nil {
		return err
	}
	defer s.releaseSlot(slot, reader)

	pc, err := pgconn.ParseConfig(cfg.DSN)
	if err != nil {
		return status.Errorf(codes.Internal, "stream dsn: %v", err)
	}
	if req.GetDatabase() != "" {
		pc.Database = req.GetDatabase()
	}
	conn, err := pgrepl.ConnectConfig(ctx, pc)
	if err != nil {
		return status.Errorf(codes.Unavailable, "replication connection: %v", err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	publication := req.GetPublication()
	if publication == "" {
		publication = "pgshard_all"
	}
	options := map[string]string{}
	for k, v := range req.GetOptions() {
		options[k] = v
	}
	options["proto_version"] = "4"
	options["publication_names"] = publication
	options["streaming"] = "parallel"
	options["messages"] = "on"
	if err := conn.StartReplication(ctx, slot, pgrepl.LSN(req.GetStartLsn()), options); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return status.Errorf(codes.FailedPrecondition, "start replication: %s (%s)", pgErr.Message, pgErr.Code)
		}
		return status.Errorf(codes.Unavailable, "start replication: %v", err)
	}
	deliver := func(batch *pgshardv1.ChangeBatch) error {
		if err := emit(batch); err != nil {
			return err
		}
		for {
			cur := reader.delivered.Load()
			if batch.GetEndLsn() <= cur || reader.delivered.CompareAndSwap(cur, batch.GetEndLsn()) {
				return nil
			}
		}
	}
	b := &batcher{emit: deliver, perEvent: perEvent, max: cfg.MaxBatchBytes}
	if req.GetBatchBytes() > 0 && int(req.GetBatchBytes()) < b.max {
		b.max = int(req.GetBatchBytes())
	}
	dec := pgoutput.NewDecoder()
	lastSent := time.Now()
	lastStatus := time.Now()
	var serverEnd pgrepl.LSN
	sendStatus := func() error {
		lsn := reader.acked.Load()
		if err := conn.SendStandbyStatus(pgrepl.StandbyStatus{Written: pgrepl.LSN(lsn), Flushed: pgrepl.LSN(lsn), Applied: pgrepl.LSN(lsn)}); err != nil {
			return err
		}
		reader.flushed.Store(lsn)
		lastStatus = time.Now()
		return nil
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-reader.wake:
			if err := sendStatus(); err != nil {
				return status.Errorf(codes.Unavailable, "standby status: %v", err)
			}
		default:
		}
		rctx, cancel := context.WithTimeout(ctx, cfg.ReceiveTimeout)
		msg, err := conn.Receive(rctx)
		cancel()
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return nil
		case pgrepl.IsTimeout(err):
			if reader.flushed.Load() < reader.acked.Load() || time.Since(lastStatus) > 10*time.Second {
				if err := sendStatus(); err != nil {
					return status.Errorf(codes.Unavailable, "standby status: %v", err)
				}
			}
			if b.empty() && time.Since(lastSent) >= cfg.Heartbeat {
				if err := b.sendNow(&pgshardv1.ChangeEvent{Lsn: uint64(serverEnd), Event: &pgshardv1.ChangeEvent_Keepalive_{Keepalive: &pgshardv1.ChangeEvent_Keepalive{WalEnd: uint64(serverEnd)}}}, uint64(serverEnd)); err != nil {
					return err
				}
				lastSent = time.Now()
			}
			continue
		case errors.Is(err, pgrepl.ErrStreamEnded):
			return status.Error(codes.Aborted, "replication stream ended")
		default:
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				return status.Errorf(codes.Aborted, "replication: %s (%s)", pgErr.Message, pgErr.Code)
			}
			return status.Errorf(codes.Unavailable, "replication: %v", err)
		}
		switch m := msg.(type) {
		case *pgrepl.PrimaryKeepalive:
			serverEnd = m.ServerWALEnd
			if m.ReplyRequested {
				if err := sendStatus(); err != nil {
					return status.Errorf(codes.Unavailable, "standby status: %v", err)
				}
			}
		case *pgrepl.XLogData:
			serverEnd = m.ServerWALEnd
			d, err := dec.Decode(m.Data)
			if err != nil {
				return status.Errorf(codes.Internal, "decode at %s: %v", m.WALStart, err)
			}
			ev, boundary, err := convert(dec, d, uint64(m.WALStart))
			if err != nil {
				return status.Errorf(codes.Internal, "convert at %s: %v", m.WALStart, err)
			}
			if ev != nil {
				if err := b.add(ev, uint64(m.ServerWALEnd), boundary); err != nil {
					return err
				}
				lastSent = time.Now()
			}
		}
	}
}

// batcher accumulates events until a transaction boundary or the size cap.
type batcher struct {
	emit     func(*pgshardv1.ChangeBatch) error
	perEvent bool
	max      int
	events   []*pgshardv1.ChangeEvent
	bytes    int
	endLSN   uint64
}

func (b *batcher) empty() bool { return len(b.events) == 0 }

func (b *batcher) add(ev *pgshardv1.ChangeEvent, endLSN uint64, boundary bool) error {
	b.events = append(b.events, ev)
	b.bytes += eventBytes(ev)
	if endLSN > b.endLSN {
		b.endLSN = endLSN
	}
	if b.perEvent || boundary || b.bytes >= b.max {
		return b.flush()
	}
	return nil
}

func (b *batcher) sendNow(ev *pgshardv1.ChangeEvent, endLSN uint64) error {
	return b.emit(&pgshardv1.ChangeBatch{Events: []*pgshardv1.ChangeEvent{ev}, EndLsn: endLSN})
}

func (b *batcher) flush() error {
	if len(b.events) == 0 {
		return nil
	}
	batch := &pgshardv1.ChangeBatch{Events: b.events, EndLsn: b.endLSN}
	b.events = nil
	b.bytes = 0
	b.endLSN = 0
	return b.emit(batch)
}

func eventBytes(ev *pgshardv1.ChangeEvent) int {
	n := 16
	if row := ev.GetRow(); row != nil {
		for _, v := range row.Old {
			n += len(v.Data) + 2
		}
		for _, v := range row.New {
			n += len(v.Data) + 2
		}
	}
	if m := ev.GetMessage(); m != nil {
		n += len(m.Content)
	}
	return n
}

// convert maps a decoded message to a ChangeEvent and reports whether it
// ends a unit the consumer may be handed (commit, prepare, stream segment
// end or a non-transactional message). Type messages carry nothing a
// consumer needs and are dropped; everything else becomes an event.
func convert(dec *pgoutput.Decoder, m pgoutput.Message, lsn uint64) (*pgshardv1.ChangeEvent, bool, error) {
	ev := &pgshardv1.ChangeEvent{Lsn: lsn}
	switch v := m.(type) {
	case *pgoutput.Begin:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_Begin_{Begin: &pgshardv1.ChangeEvent_Begin{Xid: v.Xid, FinalLsn: v.FinalLSN}}
	case *pgoutput.Commit:
		ev.Event = &pgshardv1.ChangeEvent_Commit_{Commit: &pgshardv1.ChangeEvent_Commit{CommitLsn: v.CommitLSN, EndLsn: v.EndLSN}}
		return ev, true, nil
	case *pgoutput.Origin:
		ev.Event = &pgshardv1.ChangeEvent_Origin_{Origin: &pgshardv1.ChangeEvent_Origin{Name: v.Name, CommitLsn: v.CommitLSN}}
	case *pgoutput.Relation:
		ev.Xid = v.Xid
		rel := &pgshardv1.ChangeEvent_Relation{RelationId: v.ID, Schema: v.Namespace, Table: v.Name, ReplicaIdentity: string(v.ReplicaIdentity)}
		for _, c := range v.Columns {
			rel.Columns = append(rel.Columns, &pgshardv1.ChangeEvent_Relation_Column{Name: c.Name, TypeOid: c.TypeOID, TypeModifier: c.TypeMod, Key: c.Key})
		}
		ev.Event = &pgshardv1.ChangeEvent_Relation_{Relation: rel}
	case *pgoutput.Type:
		return nil, false, nil
	case *pgoutput.Insert:
		ev.Xid = v.Xid
		row, err := rowEvent(dec, v.RelationID, pgshardv1.ChangeEvent_Row_KIND_INSERT, nil, false, &v.New)
		if err != nil {
			return nil, false, err
		}
		ev.Event = &pgshardv1.ChangeEvent_Row_{Row: row}
	case *pgoutput.Update:
		ev.Xid = v.Xid
		old, isKey := v.Old, false
		if v.Key != nil {
			old, isKey = v.Key, true
		}
		row, err := rowEvent(dec, v.RelationID, pgshardv1.ChangeEvent_Row_KIND_UPDATE, old, isKey, &v.New)
		if err != nil {
			return nil, false, err
		}
		ev.Event = &pgshardv1.ChangeEvent_Row_{Row: row}
	case *pgoutput.Delete:
		ev.Xid = v.Xid
		old, isKey := v.Old, false
		if v.Key != nil {
			old, isKey = v.Key, true
		}
		row, err := rowEvent(dec, v.RelationID, pgshardv1.ChangeEvent_Row_KIND_DELETE, old, isKey, nil)
		if err != nil {
			return nil, false, err
		}
		ev.Event = &pgshardv1.ChangeEvent_Row_{Row: row}
	case *pgoutput.Truncate:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_Truncate_{Truncate: &pgshardv1.ChangeEvent_Truncate{RelationIds: v.RelationIDs, Cascade: v.Cascade, RestartIdentity: v.RestartIdentity}}
	case *pgoutput.LogicalMessage:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_Message_{Message: &pgshardv1.ChangeEvent_Message{Prefix: v.Prefix, Content: v.Content, Transactional: v.Transactional}}
		return ev, !v.Transactional, nil
	case *pgoutput.StreamStart:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_StreamStart_{StreamStart: &pgshardv1.ChangeEvent_StreamStart{Xid: v.Xid, FirstSegment: v.FirstSegment}}
	case *pgoutput.StreamStop:
		ev.Event = &pgshardv1.ChangeEvent_StreamStop_{StreamStop: &pgshardv1.ChangeEvent_StreamStop{}}
		return ev, true, nil
	case *pgoutput.StreamCommit:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_StreamCommit_{StreamCommit: &pgshardv1.ChangeEvent_StreamCommit{Xid: v.Xid, CommitLsn: v.CommitLSN, EndLsn: v.EndLSN}}
		return ev, true, nil
	case *pgoutput.StreamAbort:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_StreamAbort_{StreamAbort: &pgshardv1.ChangeEvent_StreamAbort{Xid: v.Xid, Subxid: v.SubXid, AbortLsn: v.AbortLSN}}
		return ev, true, nil
	case *pgoutput.BeginPrepare:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_BeginPrepare_{BeginPrepare: &pgshardv1.ChangeEvent_BeginPrepare{Gid: v.Gid, Xid: v.Xid, PrepareLsn: v.PrepareLSN}}
	case *pgoutput.Prepare:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_Prepare_{Prepare: &pgshardv1.ChangeEvent_Prepare{Gid: v.Gid, PrepareLsn: v.PrepareLSN}}
		return ev, true, nil
	case *pgoutput.CommitPrepared:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_CommitPrepared_{CommitPrepared: &pgshardv1.ChangeEvent_CommitPrepared{Gid: v.Gid, CommitLsn: v.CommitLSN}}
		return ev, true, nil
	case *pgoutput.RollbackPrepared:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_RollbackPrepared_{RollbackPrepared: &pgshardv1.ChangeEvent_RollbackPrepared{Gid: v.Gid, RollbackLsn: v.RollbackEndLSN}}
		return ev, true, nil
	case *pgoutput.StreamPrepare:
		ev.Xid = v.Xid
		ev.Event = &pgshardv1.ChangeEvent_StreamPrepare_{StreamPrepare: &pgshardv1.ChangeEvent_StreamPrepare{Gid: v.Gid, Xid: v.Xid, PrepareLsn: v.PrepareLSN}}
		return ev, true, nil
	default:
		return nil, false, fmt.Errorf("unhandled pgoutput message %T", m)
	}
	return ev, false, nil
}

func rowEvent(dec *pgoutput.Decoder, relID uint32, kind pgshardv1.ChangeEvent_Row_Kind, old *pgoutput.Tuple, oldIsKey bool, newT *pgoutput.Tuple) (*pgshardv1.ChangeEvent_Row, error) {
	rel, ok := dec.Relation(relID)
	if !ok {
		return nil, fmt.Errorf("change for unknown relation %d", relID)
	}
	row := &pgshardv1.ChangeEvent_Row{Schema: rel.Namespace, Table: rel.Name, Kind: kind, RelationId: relID, OldIsKey: oldIsKey}
	if old != nil {
		row.Old, _ = values(old)
	}
	if newT != nil {
		row.New, row.UnchangedToast = values(newT)
	}
	return row, nil
}

func values(t *pgoutput.Tuple) ([]*pgshardv1.Value, []uint32) {
	out := make([]*pgshardv1.Value, len(t.Columns))
	var unchanged []uint32
	for i, c := range t.Columns {
		switch c.Kind {
		case pgoutput.ColumnNull:
			out[i] = &pgshardv1.Value{Null: true}
		case pgoutput.ColumnUnchanged:
			out[i] = &pgshardv1.Value{}
			unchanged = append(unchanged, uint32(i))
		default:
			out[i] = &pgshardv1.Value{Data: c.Data}
		}
	}
	return out, unchanged
}
