package pgwire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Cancel-key layouts. Protocol 3.2 keys carry the router instance prefix and
// the connection id followed by random bytes; 3.0 keys are limited to the
// 4-byte process id (used for the connection id) and a 4-byte secret.
const (
	CancelKeyLen32 = 32
	CancelKeyLen30 = 4
)

// CancelKey identifies a session in a CancelRequest.
type CancelKey struct {
	// PID is the BackendKeyData process id (protocol 3.0 and 3.2 alike).
	PID uint32
	// Secret is the variable-length secret key.
	Secret []byte
}

// CancelHandler receives every CancelRequest. Handlers for keys that belong
// to another router instance forward them; local keys are honoured by
// calling Server.CancelLocal.
type CancelHandler func(ctx context.Context, key CancelKey)

// Config configures a Server.
type Config struct {
	Authenticator Authenticator
	// NewExecutor is called once per authenticated session.
	NewExecutor func(SessionInfo) (Executor, error)
	// TLSConfig enables SSLRequest and direct TLS when non-nil.
	TLSConfig *tls.Config
	// AllowPlaintext keeps accepting startup packets that never negotiated
	// TLS even though TLSConfig is set. Configuring a certificate means
	// the deployment expects the wire to be protected, and a client that
	// simply omits SSLRequest would otherwise send its SCRAM exchange, its
	// SQL and its results in the clear. Development only.
	AllowPlaintext bool
	// ServerVersion is reported as the server_version parameter.
	ServerVersion string
	// Parameters overrides or extends the default ParameterStatus set.
	Parameters map[string]string
	// CancelHandler overrides the default local-only cancel dispatch.
	CancelHandler CancelHandler
	// InstanceID prefixes protocol 3.2 cancel keys; zero draws a random one.
	InstanceID uint32
	// StartupTimeout bounds the pre-authentication phase (TLS negotiation,
	// startup packet, authentication exchange); zero means 10s, negative
	// disables the bound.
	StartupTimeout time.Duration
	// MaxStartupConns caps connections in the pre-authentication phase so a
	// flood of half-open startups cannot exhaust the server; connections
	// past the cap are refused with 53300. Zero means 100, negative
	// disables the cap.
	MaxStartupConns int
	Logger          *slog.Logger
}

// Server accepts PostgreSQL client connections.
type Server struct {
	cfg        Config
	instanceID uint32
	logger     *slog.Logger

	startupSem chan struct{}
	// shutdownCh is closed by Shutdown so blocking pre-auth work (catalog
	// lookups) is cancelled instead of holding startup slots.
	shutdownCh   chan struct{}
	shutdownOnce sync.Once

	mu       sync.Mutex
	sessions map[uint64]*session
	closing  bool
	nextID   atomic.Uint64
	wg       sync.WaitGroup
	listener net.Listener
}

// NewServer validates cfg and returns a Server ready to Serve.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Authenticator == nil {
		return nil, errors.New("pgwire: Config.Authenticator is required")
	}
	if cfg.NewExecutor == nil {
		return nil, errors.New("pgwire: Config.NewExecutor is required")
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "18.0 (pgshard)"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.TLSConfig != nil {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
		if len(cfg.TLSConfig.NextProtos) == 0 {
			cfg.TLSConfig.NextProtos = []string{"postgresql"}
		}
	}
	id := cfg.InstanceID
	for id == 0 {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		id = binary.BigEndian.Uint32(b[:])
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 10 * time.Second
	}
	if cfg.MaxStartupConns == 0 {
		cfg.MaxStartupConns = 100
	}
	srv := &Server{cfg: cfg, instanceID: id, logger: cfg.Logger, sessions: map[uint64]*session{}, shutdownCh: make(chan struct{})}
	if cfg.MaxStartupConns > 0 {
		srv.startupSem = make(chan struct{}, cfg.MaxStartupConns)
	}
	return srv, nil
}

// acquireStartup claims a pre-authentication slot; false means the cap is
// reached and the connection must be refused.
func (s *Server) acquireStartup() bool {
	if s.startupSem == nil {
		return true
	}
	select {
	case s.startupSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseStartup() {
	if s.startupSem != nil {
		<-s.startupSem
	}
}

// InstanceID returns the prefix embedded in protocol 3.2 cancel keys.
func (s *Server) InstanceID() uint32 { return s.instanceID }

// Serve accepts connections on l until Shutdown is called or l fails.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errors.New("pgwire: server is shut down")
	}
	s.listener = l
	s.mu.Unlock()
	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *Server) handle(conn net.Conn) {
	sess := newSession(s, conn, s.nextID.Add(1))
	defer sess.close()
	if !s.register(sess) {
		return
	}
	defer s.unregister(sess)
	sess.run()
}

func (s *Server) register(sess *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.sessions[sess.id] = sess
	return true
}

func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess.id)
	s.mu.Unlock()
}

// Shutdown stops accepting, terminates idle sessions with 57P01 and waits for
// active queries to finish until ctx expires, then closes everything.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() { close(s.shutdownCh) })
	s.mu.Lock()
	s.closing = true
	l := s.listener
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
	for _, sess := range sessions {
		sess.drain()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		for _, sess := range sessions {
			sess.forceClose()
		}
		<-done
		return ctx.Err()
	}
}

// Sessions reports the number of live sessions.
func (s *Server) Sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func (s *Server) newCancelKey(sessionID uint64, protocolVersion uint32) (CancelKey, error) {
	pid := uint32(sessionID)
	n := CancelKeyLen30
	if protocolVersion >= ProtocolVersion32 {
		n = CancelKeyLen32
	}
	secret := make([]byte, n)
	if _, err := rand.Read(secret); err != nil {
		return CancelKey{}, err
	}
	if n == CancelKeyLen32 {
		binary.BigEndian.PutUint32(secret[0:4], s.instanceID)
		binary.BigEndian.PutUint32(secret[4:8], pid)
	}
	return CancelKey{PID: pid, Secret: secret}, nil
}

// OwnsCancelKey reports whether a protocol 3.2 key was minted by this
// instance. 3.0 keys carry no prefix and always report true.
func (s *Server) OwnsCancelKey(key CancelKey) bool {
	if len(key.Secret) != CancelKeyLen32 {
		return true
	}
	return binary.BigEndian.Uint32(key.Secret[0:4]) == s.instanceID
}

// CancelLocal cancels the running query of the local session identified by
// key, if any. It reports whether a matching session was found.
func (s *Server) CancelLocal(key CancelKey) bool {
	s.mu.Lock()
	sess := s.sessions[uint64(key.PID)]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.cancelQuery(key.Secret)
}

func (s *Server) dispatchCancel(ctx context.Context, key CancelKey) {
	if s.cfg.CancelHandler != nil {
		s.cfg.CancelHandler(ctx, key)
		return
	}
	s.CancelLocal(key)
}

func keysEqual(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}
