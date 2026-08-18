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
}

// Server implements the Pooler gRPC service.
type Server struct {
	pgshardv1.UnimplementedPoolerServer
	cfg Config

	mu       sync.Mutex
	sessions map[string]*session
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
	return &Server{cfg: cfg, sessions: map[string]*session{}}
}

// Register registers the service on g.
func (s *Server) Register(g *grpc.Server) { pgshardv1.RegisterPoolerServer(g, s) }

var errUnavailable = status.Error(codes.Unavailable, "pooler is draining")

func (s *Server) session(id string) *session {
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
	defer zero(ck)
	defer zero(sk)
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
	se.role = first.User.Username
	se.database = s.cfg.Database
	s.mu.Unlock()
	defer s.detach(se)

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
	if !keep {
		se.b = nil
	}
	s.mu.Unlock()
	if keep {
		return
	}
	if b != nil {
		s.cfg.Pool.Release(b)
	}
	s.forget(se)
}

type relay struct {
	srv    *Server
	se     *session
	stream pgshardv1.Pooler_ExecuteServer
	ck, sk []byte
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

func (r *relay) setBackend(b *Backend) {
	r.srv.mu.Lock()
	defer r.srv.mu.Unlock()
	r.se.b = b
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
		r.setBackend(b)
	}
	b.send(fm)
	if !flushesBackend(req) {
		return nil
	}
	if err := b.flush(); err != nil {
		return r.backendLost(b, err)
	}
	return r.pump(b)
}

// pump forwards backend responses until the batch ends: ReadyForQuery, or a
// COPY-in start where the router must speak next.
func (r *relay) pump(b *Backend) error {
	for {
		msg, err := b.receive()
		if err != nil {
			return r.backendLost(b, err)
		}
		if resp := toResponse(msg); resp != nil {
			if err := r.send(resp); err != nil {
				return err
			}
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery:
			if !r.reserved() && b.idle() {
				r.setBackend(nil)
				r.srv.cfg.Pool.Release(b)
			}
			return nil
		case *pgproto3.CopyInResponse, *pgproto3.CopyBothResponse:
			return nil
		}
	}
}

func (r *relay) backendLost(b *Backend, cause error) error {
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
	var pid int32
	if se.b != nil {
		pid = int32(se.b.pid)
	}
	return &pgshardv1.ReserveResponse{BackendPid: pid}, nil
}

// Release unpins a reserved session: any open transaction is rolled back,
// session state is discarded, and the backend returns to its pool.
func (s *Server) Release(_ context.Context, req *pgshardv1.ReleaseRequest) (*pgshardv1.ReleaseResponse, error) {
	se := s.lookup(req.SessionId)
	if se == nil {
		return &pgshardv1.ReleaseResponse{}, nil
	}
	s.mu.Lock()
	if se.attached {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "session still has an Execute stream")
	}
	b := se.b
	se.b, se.reserved = nil, false
	s.mu.Unlock()
	s.forget(se)
	if b == nil {
		return &pgshardv1.ReleaseResponse{}, nil
	}
	if b.hasUnflushed() {
		s.cfg.Pool.Discard(b)
		return &pgshardv1.ReleaseResponse{}, nil
	}
	if !b.idle() {
		if err := b.simpleQuery("ROLLBACK"); err != nil {
			s.cfg.Pool.Discard(b)
			return &pgshardv1.ReleaseResponse{}, nil
		}
	}
	if err := b.simpleQuery("DISCARD ALL"); err != nil {
		s.cfg.Pool.Discard(b)
		return &pgshardv1.ReleaseResponse{}, nil
	}
	s.cfg.Pool.Release(b)
	return &pgshardv1.ReleaseResponse{}, nil
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
