package pgwire

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrCopyFail is returned by CopyInStream.Next when the client aborts.
var ErrCopyFail = errors.New("pgwire: COPY failed by client")

const maxMessageBodyLen = 1 << 30

type session struct {
	server *Server
	id     uint64
	conn   net.Conn
	reader *bufio.Reader
	be     *pgproto3.Backend

	protocolVersion uint32
	info            SessionInfo
	exec            Executor
	cancelKey       CancelKey

	mu          sync.Mutex
	active      bool
	draining    bool
	closed      bool
	queryCancel context.CancelFunc
	// inTxn mirrors the executor's transaction status for drain decisions;
	// it is only written by the session goroutine.
	inTxn bool

	// skipToSync is set after an extended-protocol error until Sync arrives.
	skipToSync bool
	dataRows   int
	// copyIn is the active COPY FROM STDIN stream, if any.
	copyIn *copyInStream
}

func newSession(s *Server, conn net.Conn, id uint64) *session {
	sess := &session{server: s, id: id, conn: conn}
	sess.resetIO(conn)
	return sess
}

func (s *session) resetIO(conn net.Conn) {
	s.conn = conn
	s.reader = bufio.NewReader(conn)
	s.be = pgproto3.NewBackend(s.reader, conn)
	s.be.SetMaxBodyLen(maxMessageBodyLen)
}

func (s *session) send(msgs ...pgproto3.BackendMessage) error {
	for _, m := range msgs {
		s.be.Send(m)
	}
	return s.be.Flush()
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	exec := s.exec
	s.mu.Unlock()
	_ = s.conn.Close()
	if exec != nil {
		exec.Release()
	}
}

func (s *session) forceClose() {
	s.mu.Lock()
	if s.queryCancel != nil {
		s.queryCancel()
	}
	s.mu.Unlock()
	_ = s.conn.Close()
}

// drain asks the session to end: idle sessions are terminated immediately,
// active ones and sessions inside a transaction block once the transaction
// (or the current statement, outside a block) has finished.
func (s *session) drain() {
	s.mu.Lock()
	s.draining = true
	idle := !s.active && !s.inTxn && !s.closed
	s.mu.Unlock()
	if idle {
		// The session goroutine is blocked in Receive, so write the error
		// directly rather than through its buffered backend.
		er := toErrorResponse(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
		er.Severity, er.SeverityUnlocalized = "FATAL", "FATAL"
		if buf, err := er.Encode(nil); err == nil {
			_, _ = s.conn.Write(buf)
		}
		s.close()
	}
}

func (s *session) terminate(err error) {
	er := toErrorResponse(err)
	er.Severity, er.SeverityUnlocalized = "FATAL", "FATAL"
	_ = s.send(er)
	s.close()
}

// beginMessage marks the session active; it reports false if the session is
// draining outside a transaction block and must stop.
func (s *session) beginMessage() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if (s.draining && !s.inTxn) || s.closed {
		return false
	}
	s.active = true
	return true
}

func (s *session) endMessage() {
	s.mu.Lock()
	s.active = false
	s.queryCancel = nil
	s.mu.Unlock()
}

func (s *session) queryContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	s.mu.Lock()
	s.queryCancel = cancel
	s.mu.Unlock()
	return ctx, cancel
}

func (s *session) cancelQuery(secret []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !keysEqual(secret, s.cancelKey.Secret) {
		return false
	}
	if s.queryCancel != nil {
		s.queryCancel()
	}
	return true
}

func (s *session) run() {
	log := s.server.logger.With("session", s.id, "remote", s.conn.RemoteAddr().String())
	ctx := context.Background()
	if !s.beginMessage() {
		return
	}
	err := s.startup(ctx)
	s.endMessage()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, errCancelRequest) {
			log.Debug("startup failed", "err", err)
		}
		return
	}
	defer func() {
		s.mu.Lock()
		s.active = false
		s.mu.Unlock()
	}()
	for {
		msg, err := s.be.Receive()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
				s.terminate(Errorf(CodeProtocolViolation, "invalid frontend message: %v", err))
			}
			return
		}
		if !s.beginMessage() {
			s.terminate(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
			return
		}
		cont, err := s.dispatch(ctx, msg)
		s.endMessage()
		if err != nil {
			log.Debug("session ended", "err", err)
			return
		}
		if !cont {
			return
		}
		s.mu.Lock()
		s.inTxn = s.exec != nil && s.exec.TransactionStatus() != TxIdle
		terminate := s.draining && !s.inTxn
		s.mu.Unlock()
		if terminate {
			s.terminate(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
			return
		}
	}
}

var errCancelRequest = errors.New("cancel request handled")

func (s *session) startup(ctx context.Context) error {
	if s.server.cfg.TLSConfig != nil {
		if b, err := s.reader.Peek(1); err == nil && b[0] == 0x16 {
			if err := s.upgradeTLS(); err != nil {
				return err
			}
		}
	}
	var pkt *startupPacket
	for {
		var err error
		pkt, err = readStartupPacket(s.reader)
		if err != nil {
			return err
		}
		switch pkt.kind {
		case startupSSLRequest:
			if _, isTLS := s.conn.(*tls.Conn); isTLS {
				return errors.New("SSLRequest inside an established TLS connection")
			}
			if s.server.cfg.TLSConfig == nil {
				if _, err := s.conn.Write([]byte{'N'}); err != nil {
					return err
				}
				continue
			}
			if _, err := s.conn.Write([]byte{'S'}); err != nil {
				return err
			}
			if err := s.upgradeTLS(); err != nil {
				return err
			}
			continue
		case startupGSSEncRequest:
			if _, err := s.conn.Write([]byte{'N'}); err != nil {
				return err
			}
			continue
		case startupCancelRequest:
			s.server.dispatchCancel(ctx, CancelKey{PID: pkt.cancelPID, Secret: pkt.cancelKey})
			return errCancelRequest
		case startupMessage:
		}
		break
	}
	if pkt.major() != 3 {
		s.terminate(Errorf(CodeProtocolViolation, "unsupported frontend protocol %d.%d: server supports 3.0 to 3.2", pkt.major(), pkt.minor()))
		return fmt.Errorf("unsupported protocol %d.%d", pkt.major(), pkt.minor())
	}
	var unsupported []string
	for k := range pkt.params {
		if len(k) > 5 && k[:5] == "_pq_." {
			unsupported = append(unsupported, k)
			delete(pkt.params, k)
		}
	}
	sort.Strings(unsupported)
	s.protocolVersion = pkt.protocolVersion
	if s.protocolVersion > ProtocolVersionLatest {
		s.protocolVersion = ProtocolVersionLatest
	}
	if pkt.protocolVersion > ProtocolVersionLatest || len(unsupported) > 0 {
		if err := s.send(&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: ProtocolVersionLatest, UnrecognizedOptions: unsupported}); err != nil {
			return err
		}
	}
	user := pkt.params["user"]
	if user == "" {
		s.terminate(Errorf(CodeInvalidAuthorization, "no PostgreSQL user name specified in startup packet"))
		return errors.New("startup without user")
	}
	db := pkt.params["database"]
	if db == "" {
		db = user
	}
	result, err := s.server.cfg.Authenticator.Authenticate(ctx, pkt.params, authExchange{s})
	if err != nil {
		var pe *Error
		if !errors.As(err, &pe) {
			err = Errorf(CodeInvalidAuthorization, "authentication failed: %v", err)
		}
		s.terminate(err)
		return err
	}
	s.info = SessionInfo{
		ID: s.id, User: user, Database: db, Params: pkt.params,
		ProtocolVersion: s.protocolVersion, Auth: result, RemoteAddr: s.conn.RemoteAddr().String(),
	}
	exec, err := s.server.cfg.NewExecutor(s.info)
	if err != nil {
		s.terminate(err)
		return err
	}
	s.mu.Lock()
	s.exec = exec
	s.mu.Unlock()
	key, err := s.server.newCancelKey(s.id, s.protocolVersion)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cancelKey = key
	s.mu.Unlock()
	s.be.Send(&pgproto3.AuthenticationOk{})
	_ = s.be.SetAuthType(pgproto3.AuthTypeOk)
	for _, kv := range s.parameterStatus() {
		s.be.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
	}
	s.be.Send(&pgproto3.BackendKeyData{ProcessID: key.PID, SecretKey: key.Secret})
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: byte(exec.TransactionStatus())})
	return s.be.Flush()
}

// bufferedConn lets TLS consume bytes the startup reader already buffered.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (s *session) upgradeTLS() error {
	tc := tls.Server(bufferedConn{Conn: s.conn, r: s.reader}, s.server.cfg.TLSConfig)
	if err := tc.Handshake(); err != nil {
		return err
	}
	s.resetIO(tc)
	return nil
}

func (s *session) parameterStatus() [][2]string {
	params := map[string]string{
		"server_version":                s.server.cfg.ServerVersion,
		"server_encoding":               "UTF8",
		"client_encoding":               "UTF8",
		"DateStyle":                     "ISO, MDY",
		"integer_datetimes":             "on",
		"standard_conforming_strings":   "on",
		"TimeZone":                      "UTC",
		"IntervalStyle":                 "postgres",
		"is_superuser":                  "off",
		"session_authorization":         s.info.User,
		"application_name":              s.info.Params["application_name"],
		"default_transaction_read_only": "off",
		"in_hot_standby":                "off",
		"scram_iterations":              fmt.Sprint(DefaultSCRAMIterations),
	}
	if s.protocolVersion >= ProtocolVersion32 {
		params["search_path"] = `"$user", public`
	}
	for k, v := range s.server.cfg.Parameters {
		params[k] = v
	}
	out := make([][2]string, 0, len(params))
	for k, v := range params {
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

type authExchange struct{ s *session }

func (a authExchange) Request(msg pgproto3.BackendMessage, authType uint32) (pgproto3.FrontendMessage, error) {
	if err := a.s.be.SetAuthType(authType); err != nil {
		return nil, err
	}
	if err := a.s.send(msg); err != nil {
		return nil, err
	}
	if authType == pgproto3.AuthTypeSASLFinal || authType == pgproto3.AuthTypeOk {
		return nil, nil
	}
	reply, err := a.s.be.Receive()
	if err != nil {
		return nil, err
	}
	if _, isTerminate := reply.(*pgproto3.Terminate); isTerminate {
		return nil, io.EOF
	}
	return reply, nil
}

func (s *session) readyForQuery() error {
	return s.send(&pgproto3.ReadyForQuery{TxStatus: byte(s.exec.TransactionStatus())})
}

func (s *session) reportError(err error) {
	if errors.Is(err, context.Canceled) {
		err = Errorf(CodeQueryCanceled, "canceling statement due to user request")
	}
	s.be.Send(toErrorResponse(err))
}

// dispatch handles one frontend message; it returns cont=false when the
// session should end normally and a non-nil error on I/O failure.
func (s *session) dispatch(ctx context.Context, msg pgproto3.FrontendMessage) (bool, error) {
	if s.skipToSync {
		switch msg.(type) {
		case *pgproto3.Sync, *pgproto3.Terminate:
		default:
			return true, nil
		}
	}
	w := &resultWriter{s: s}
	switch m := msg.(type) {
	case *pgproto3.Terminate:
		return false, nil
	case *pgproto3.Query:
		return true, s.simpleQuery(ctx, m.String, w)
	case *pgproto3.FunctionCall:
		s.reportError(Errorf(CodeFeatureNotSupported, "the function call sub-protocol is not supported"))
		return true, s.readyForQuery()
	case *pgproto3.Parse:
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Parse(qctx, m.Name, m.Query, m.ParameterOIDs)
		cancel()
		if err != nil {
			return true, s.extendedError(err)
		}
		s.be.Send(&pgproto3.ParseComplete{})
	case *pgproto3.Bind:
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Bind(qctx, m.DestinationPortal, m.PreparedStatement, m.ParameterFormatCodes, m.Parameters, m.ResultFormatCodes)
		cancel()
		if err != nil {
			return true, s.extendedError(err)
		}
		s.be.Send(&pgproto3.BindComplete{})
	case *pgproto3.Describe:
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Describe(qctx, DescribeKind(m.ObjectType), m.Name, w)
		cancel()
		if err != nil {
			return true, s.extendedError(err)
		}
	case *pgproto3.Execute:
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Execute(qctx, m.Portal, int32(m.MaxRows), w)
		cancel()
		if err != nil {
			return true, s.extendedError(err)
		}
	case *pgproto3.Close:
		if err := s.exec.Close(ctx, DescribeKind(m.ObjectType), m.Name); err != nil {
			return true, s.extendedError(err)
		}
		s.be.Send(&pgproto3.CloseComplete{})
	case *pgproto3.Flush:
		return true, s.be.Flush()
	case *pgproto3.Sync:
		s.skipToSync = false
		if err := s.exec.Sync(ctx); err != nil {
			s.reportError(err)
		}
		return true, s.readyForQuery()
	case *pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
		// Accepted and ignored outside a COPY, per the protocol.
	default:
		s.reportError(Errorf(CodeProtocolViolation, "unexpected message %T", msg))
		return true, s.readyForQuery()
	}
	return true, nil
}

func (s *session) extendedError(err error) error {
	s.skipToSync = true
	s.reportError(err)
	return s.be.Flush()
}

func (s *session) simpleQuery(ctx context.Context, sql string, w *resultWriter) error {
	n, err := countStatements(sql)
	switch {
	case err != nil:
		s.reportError(Errorf(CodeSyntaxError, "%v", err))
		return s.readyForQuery()
	case n > 1:
		s.reportError(Errorf(CodeFeatureNotSupported, "multi-statement simple queries are not supported"))
		return s.readyForQuery()
	case n == 0:
		s.be.Send(&pgproto3.EmptyQueryResponse{})
		return s.readyForQuery()
	}
	qctx, cancel := s.queryContext(ctx)
	err = s.exec.SimpleQuery(qctx, sql, w)
	cancel()
	// Any CopyData still in flight after an aborted COPY is ignored by dispatch.
	s.copyIn = nil
	if err != nil {
		if w.ioErr != nil {
			return w.ioErr
		}
		s.reportError(err)
	}
	return s.readyForQuery()
}
