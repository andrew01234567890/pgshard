package pooler

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
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
	// Database is the default PostgreSQL database for sessions whose first
	// Execute message does not name one.
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
	// discarded remembers the sessions an expiry rolled a transaction back
	// on, so the router that returns is told rather than handed a fresh
	// session; see noteDiscarded.
	discarded map[string]time.Time
	// nextExpiry is when the earliest detached reservation falls due, or
	// zero when none is waiting. Every Execute stream used to scan the
	// whole session map to find out, so establishing N sessions cost N
	// squared checks on the one mutex that also serializes them.
	nextExpiry time.Time
	// readers holds the one admitted change-stream reader per slot.
	readers  map[string]*streamReader
	draining atomic.Bool
	closed   atomic.Bool

	// detachUnlocked runs in tests between detach releasing the lock and
	// recycling the backend.
	detachUnlocked func()
}

// session is the pooler-side state for one router session.
type session struct {
	id       string
	database string
	role     string
	reserved bool
	attached bool
	// cred pins the SCRAM credential digest and role that first attached;
	// every later attach must present the same pair before it may reach a
	// retained backend.
	cred    [32]byte
	credSet bool
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

// attachSession finds or creates the session and marks it attached in one
// critical section: a lookup that released the lock before attaching could
// hold a session a concurrent detach has already forgotten while a new
// Execute registers a fresh one under the same id.
func (s *Server) attachSession(id, role, database string, cred [32]byte) (*session, error) {
	s.expireReservations(time.Now())
	s.mu.Lock()
	se, ok := s.sessions[id]
	if !ok {
		se = &session{id: id}
		s.sessions[id] = se
	}
	if se.credSet {
		roleOK := subtle.ConstantTimeCompare([]byte(role), []byte(se.role))
		credOK := subtle.ConstantTimeCompare(cred[:], se.cred[:])
		if roleOK&credOK != 1 {
			// Reject and tear the session down: a caller holding only the
			// session id must never reach the backend the real credentials
			// authenticated.
			b := se.b
			se.b, se.reserved = nil, false
			if s.sessions[se.id] == se {
				delete(s.sessions, se.id)
			}
			s.mu.Unlock()
			if b != nil {
				s.cfg.Pool.Discard(b)
			}
			return nil, status.Error(codes.PermissionDenied, "session credentials do not match")
		}
	}
	defer s.mu.Unlock()
	if !ok && s.takeDiscarded(id, time.Now()) {
		// A session id the pooler expired with a transaction open. The
		// fresh one above is exactly what must not be handed back without
		// a word, so it goes again and the router is told why.
		delete(s.sessions, id)
		return nil, status.Errorf(codes.Aborted,
			"session %s was released after %s without an Execute stream and its transaction was rolled back", id, s.cfg.ReserveTimeout)
	}
	if se.attached {
		return nil, status.Error(codes.FailedPrecondition, "session already has an Execute stream")
	}
	if database == "" {
		database = s.cfg.Database
	}
	if se.b != nil && se.database != database {
		return nil, status.Error(codes.FailedPrecondition, "session already holds a backend for another database")
	}
	se.cred, se.credSet = cred, true
	se.attached = true
	se.detached = make(chan struct{})
	se.detachedAt = time.Time{}
	se.role = role
	se.database = database
	return se, nil
}

func (s *Server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
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
	if len(ck) != 32 || len(sk) != 32 {
		zero(ck)
		zero(sk)
		return status.Error(codes.InvalidArgument, "SCRAM client and server keys must be 32 bytes")
	}
	if s.draining.Load() {
		return errUnavailable
	}
	se, err := s.attachSession(first.SessionId, first.User.Username, first.Database, sessionCred(first.User.Username, ck, sk))
	if err != nil {
		zero(ck)
		zero(sk)
		return err
	}
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
		s.noteExpiry(se.detachedAt)
	} else {
		se.b = nil
		// Forget under the same lock: dropping it after unlocking would
		// let a new Execute for the same session id attach to this entry
		// and then be forgotten with it.
		if s.sessions[se.id] == se {
			delete(s.sessions, se.id)
		}
	}
	detached := se.detached
	s.mu.Unlock()
	if s.detachUnlocked != nil {
		s.detachUnlocked()
	}
	if !keep {
		s.recycle(b)
	}
	close(detached)
}

// recycle returns b to the pool clean: an open transaction is rolled back,
// and DISCARD ALL resets it when a session held it (GUCs may be staged) or
// it still holds statements. A backend that cannot be cleaned is discarded.
func (s *Server) recycle(b *Backend) {
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
	// Every handoff resets, not only the ones the router recognised as
	// stateful. The router pins on syntax, and a statement's effect on the
	// backend need not be visible in its syntax: SELECT set_config(...,
	// false) sets a GUC, pg_advisory_lock() takes a lock the backend holds,
	// and a user function does whatever it does -- all three parse as
	// ordinary reads. Whatever they left would otherwise be inherited by
	// the next logical session of the same role, which is a leak between
	// clients rather than a lapse in tidiness.
	//
	// DISCARD ALL covers it: verified on PostgreSQL 18 that it both
	// releases advisory locks (it runs pg_advisory_unlock_all) and resets
	// GUCs to their defaults.
	//
	// The cost is one round trip per backend handoff, which is what
	// PgBouncer's server_reset_query pays for the same reason.
	if err := b.simpleQuery("DISCARD ALL"); err != nil {
		s.cfg.Pool.Discard(b)
		return
	}
	b.prepared = nil
	b.sqlPrepared = false
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
	// awaiting counts the backend replies still owed for messages sent
	// since the last flush. A Flush produces no ReadyForQuery, so this is
	// the only way to know when its answers have all arrived; it includes
	// the Close messages the pooler injects, which the backend answers
	// like any other.
	awaiting int
	// copyBytes counts CopyData buffered for the backend since the last
	// write to it. COPY IN produces no reply until it ends, so nothing
	// else would move the upload out of memory before CopyDone.
	copyBytes int
	// packed is set once a request asks for packed rows and answers every
	// row of the session that way. A Value submessage per column cost an
	// allocation and a length-delimited frame each way, per column, per
	// row, and rows are the one message a result has many of.
	packed bool
}

// flushEveryCopyBytes bounds how much of a COPY IN upload the pooler holds
// before handing it to PostgreSQL.
const flushEveryCopyBytes = 64 << 10

// expect sends a frontend message the backend answers with exactly one
// terminating reply, and records that the reply is owed.
func (r *relay) expect(b *Backend, fm pgproto3.FrontendMessage) {
	b.send(fm)
	r.awaiting++
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
	// Latched rather than read per message: the router sets it on the
	// messages it originates, and a row can arrive while the relay is
	// answering something else.
	r.packed = r.packed || req.PackedRows
	view := r.srv.cfg.Source.View()
	if e := fence(view, req.Generation); e != nil {
		return r.refuse(e)
	}
	if e := fenceMigrating(view, req); e != nil {
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
			r.srv.cfg.Logger.Warn("acquire failed", "session", r.se.id, "role", r.se.role, "database", r.se.database, "err", err)
			var se *startupError
			if errors.As(err, &se) {
				return r.refuse(&pgshardv1.Error{Sqlstate: se.code, Message: se.message})
			}
			return r.refuse(&pgshardv1.Error{Sqlstate: "53300", Message: "no backend available: " + err.Error()})
		}
		if !r.setBackend(b) {
			r.srv.cfg.Pool.Discard(b)
			return r.refuse(&pgshardv1.Error{Sqlstate: "57P03", Message: "pooler is draining"})
		}
	}
	if _, isFlush := req.Message.(*pgshardv1.ExecuteRequest_Flush); isFlush {
		// Flush itself is answered by nothing, so it is not counted.
		b.send(fm)
		if err := b.flush(); err != nil {
			return r.backendLost(b, err)
		}
		return r.pumpFlush(b)
	}
	r.forward(b, fm)
	if !flushesBackend(req) {
		if m, ok := req.Message.(*pgshardv1.ExecuteRequest_CopyData); ok {
			r.copyBytes += len(m.CopyData.Data)
			if r.copyBytes >= flushEveryCopyBytes {
				r.copyBytes = 0
				if err := b.flush(); err != nil {
					return r.backendLost(b, err)
				}
			}
		}
		return nil
	}
	r.copyBytes = 0
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
		if createsPrepared(m.Query) {
			b.sqlPrepared = true
		}
		if m.Name == "" {
			break
		}
		fp := statementFingerprint(m)
		if !b.sqlPrepared && b.prepared.holds(m.Name, fp) {
			r.srv.notePrepared(true)
			r.expect(b, &pgproto3.Close{ObjectType: 'P', Name: noopPortal})
			r.closes = append(r.closes, closeAsParse)
			return
		}
		r.srv.notePrepared(false)
		if b.prepared.mayHold(m.Name) || b.sqlPrepared {
			r.expect(b, &pgproto3.Close{ObjectType: 'S', Name: m.Name})
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
		if createsPrepared(m.String) {
			b.sqlPrepared = true
		}
	}
	r.expect(b, fm)
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
	// A Sync batch is answered to its ReadyForQuery whatever the count
	// says, so anything still owed belongs to the batch that just ended.
	r.closes, r.parsed, r.batchErr, r.awaiting = r.closes[:0], r.parsed[:0], false, 0
}

// pump forwards backend responses until the batch ends: ReadyForQuery, or a
// COPY-in start where the router must speak next.
// terminatesMessage reports whether msg is the last reply to one forwarded
// frontend message. ParameterDescription is not one: a Describe of a
// statement sends it first and then the row shape.
func terminatesMessage(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ParseComplete, *pgproto3.BindComplete, *pgproto3.CloseComplete,
		*pgproto3.NoData, *pgproto3.RowDescription, *pgproto3.CommandComplete,
		*pgproto3.EmptyQueryResponse, *pgproto3.PortalSuspended,
		*pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse:
		return true
	}
	return false
}

// pumpFlush relays the answers to one Flush. There is no ReadyForQuery to
// stop at -- that is what distinguishes Flush from Sync -- so it stops when
// every forwarded message has been answered, and tells the router by
// sending FlushComplete. An error stops it too: PostgreSQL discards the
// rest of the batch until Sync, so the outstanding replies never come.
func (r *relay) pumpFlush(b *Backend) error {
	for r.awaiting > 0 {
		msg, err := b.receive()
		if err != nil {
			return r.backendLost(b, err)
		}
		if _, isErr := msg.(*pgproto3.ErrorResponse); isErr {
			r.batchErr = true
			r.awaiting = 0
		} else if terminatesMessage(msg) {
			r.awaiting--
		}
		if cc, isClose := msg.(*pgproto3.CloseComplete); isClose && len(r.closes) > 0 {
			_ = cc
			kind := r.closes[0]
			r.closes = r.closes[1:]
			switch kind {
			case closeInjected:
				continue
			case closeAsParse:
				msg = &pgproto3.ParseComplete{}
			}
		}
		if resp := toResponse(msg, r.packed); resp != nil {
			if err := r.send(resp); err != nil {
				return err
			}
		}
		// COPY takes the stream over until the client ends it, exactly as
		// it does for a Sync batch.
		switch msg.(type) {
		case *pgproto3.CopyInResponse, *pgproto3.CopyBothResponse:
			return nil
		}
	}
	return r.send(&pgshardv1.ExecuteResponse{Message: &pgshardv1.ExecuteResponse_FlushComplete{FlushComplete: &pgshardv1.FlushComplete{}}})
}

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
		if resp := toResponse(msg, r.packed); resp != nil {
			if err := r.send(resp); err != nil {
				return err
			}
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			r.endBatch(b)
			if !r.reserved() && b.idle() {
				r.setBackend(nil)
				r.srv.recycle(b)
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
		s.noteExpiry(se.detachedAt)
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
	if s.sessions[se.id] == se {
		delete(s.sessions, se.id)
	}
	s.mu.Unlock()
	s.recycle(b)
	return &pgshardv1.ReleaseResponse{}, nil
}

// expireReservations releases reserved sessions whose Execute stream has
// been gone for ReserveTimeout: their router died without Release and
// would otherwise hold the backend and the session entry forever.
// noteExpiry records that a reservation detached at t, so a later pass
// knows whether anything can be due without walking the sessions. Call
// with mu held.
func (s *Server) noteExpiry(t time.Time) {
	due := t.Add(s.cfg.ReserveTimeout)
	if s.nextExpiry.IsZero() || due.Before(s.nextExpiry) {
		s.nextExpiry = due
	}
}

func (s *Server) expireReservations(now time.Time) {
	type claim struct {
		se *session
		b  *Backend
	}
	var expired []claim
	s.mu.Lock()
	// Nothing is due: the common case on a stream that arrives while the
	// pooler is busy, and the one that used to walk every session.
	if s.nextExpiry.IsZero() || now.Before(s.nextExpiry) {
		s.mu.Unlock()
		return
	}
	s.nextExpiry = time.Time{}
	for id, se := range s.sessions {
		if !se.reserved || se.attached || se.detachedAt.IsZero() {
			continue
		}
		if now.Sub(se.detachedAt) >= s.cfg.ReserveTimeout {
			delete(s.sessions, id)
			// An expiry that rolls back an open transaction has to be
			// tellable afterwards: without this the next attach gets a
			// fresh session and the client carries on as though its
			// transaction were still open, finding out only when the next
			// statement behaves as if nothing had begun.
			if se.b != nil && !se.b.idle() {
				s.noteDiscarded(id, now)
			}
			// Claim the backend under the lock so a Release racing this
			// expiry finds nothing left to recycle.
			expired = append(expired, claim{se: se, b: se.b})
			se.b, se.reserved = nil, false
			continue
		}
		// Still waiting: it sets when the next pass has work to do.
		s.noteExpiry(se.detachedAt)
	}
	s.mu.Unlock()
	for _, c := range expired {
		s.cfg.Logger.Warn("releasing reservation with no stream", "session", c.se.id, "role", c.se.role)
		s.recycle(c.b)
	}
}

// discardedRetention is how long an expiry that took a transaction with it
// is remembered, so the router that comes back is told rather than handed a
// fresh session. It is a bound on memory, not a promise: a router that
// returns later than this is in the same position as one whose pooler
// restarted.
const discardedRetention = 30 * time.Minute

// discardedCap bounds the record. A pooler cannot be made to grow without
// limit by a caller inventing session ids, so the oldest entries go first.
const discardedCap = 4096

// noteDiscarded records that this session's transaction was rolled back by
// an expiry. Called with s.mu held.
func (s *Server) noteDiscarded(id string, now time.Time) {
	if s.discarded == nil {
		s.discarded = map[string]time.Time{}
	}
	if len(s.discarded) >= discardedCap {
		oldest, at := "", time.Time{}
		for k, v := range s.discarded {
			if at.IsZero() || v.Before(at) {
				oldest, at = k, v
			}
		}
		delete(s.discarded, oldest)
	}
	s.discarded[id] = now
}

// takeDiscarded reports whether this session was expired with a transaction
// open, and forgets it: the caller is being told now. Called with s.mu held.
func (s *Server) takeDiscarded(id string, now time.Time) bool {
	at, ok := s.discarded[id]
	if !ok {
		return false
	}
	delete(s.discarded, id)
	return now.Sub(at) < discardedRetention
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

// sessionCred binds a session to the role and SCRAM keys that attached it.
func sessionCred(role string, ck, sk []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(role))
	h.Write([]byte{0})
	h.Write(ck)
	h.Write([]byte{0})
	h.Write(sk)
	var d [32]byte
	h.Sum(d[:0])
	return d
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
