package pgwire

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrCopyFail is returned by CopyInStream.Next when the client aborts.
var ErrCopyFail = errors.New("pgwire: COPY failed by client")

// DefaultMaxMessageBodyLen bounds one frontend message body when Config
// leaves it unset. PostgreSQL's own limit is 1 GiB, but a router is not a
// backend: it never needs to hold a 1 GiB Bind or Query, and pgproto3
// allocates the whole declared length before a single body byte arrives, so
// the ceiling is what one authenticated session can make the router allocate
// from a five-byte header. 64 MiB is far above any realistic Query, Bind or
// CopyData chunk and three orders of magnitude below the old ceiling.
const DefaultMaxMessageBodyLen = 64 << 20

// preAuthMaxMessageBodyLen bounds a frontend message body before the client
// has authenticated. The ceiling above is what one AUTHENTICATED session can
// make the router allocate from a five-byte header; before authentication
// nobody has earned that, and a client that has sent nothing but a startup
// packet could declare a 64 MiB SASL body and have it allocated, times every
// startup slot the router allows at once.
//
// Nothing legitimate is near it: a startup packet, a password message and a
// SCRAM exchange are hundreds of bytes. The limit is raised to the full one
// the moment authentication succeeds.
const preAuthMaxMessageBodyLen = 64 << 10

type session struct {
	server *Server
	id     uint64
	conn   net.Conn
	reader *bufio.Reader
	be     *pgproto3.Backend

	// rows holds DataRow frames encoded for this session, and beDirty says
	// the pgproto3 backend has bytes of its own waiting. At most one of the
	// two is non-empty at a time; see sendMsg.
	rows    []byte
	beDirty bool

	protocolVersion uint32
	info            SessionInfo
	exec            Executor
	cancelKey       CancelKey

	mu       sync.Mutex
	active   bool
	draining bool
	// drainSaw records the flags drain() decided from, and whether it took
	// the idle branch. The state a failing shutdown test can read
	// afterwards says where things ended, not what drain() saw when it
	// chose, and those have disagreed.
	drainSaw string
	// revoked latches a revocation so a session still authenticating cannot
	// finish startup and begin serving.
	revoked bool
	// serving is set once startup has finished. Before that a revocation
	// only latches: telling a half-authenticated client that its role may
	// no longer log in would say whether the role exists at all, which the
	// mock SCRAM exchange goes to some trouble to hide.
	serving     bool
	closed      bool
	queryCancel context.CancelFunc
	// queryCtx is the context queryCancel cancels, kept so a COPY parked in
	// a read can tell a cancellation from a plain timeout.
	queryCtx context.Context
	// inTxn mirrors the executor's transaction status for drain decisions;
	// it is only written by the session goroutine.
	inTxn bool

	// skipToSync is set after an extended-protocol error until Sync arrives.
	skipToSync bool
	// queued is what has been handed to the backend buffer since the last
	// flush, in bytes on the wire.
	queued int
	// copyIn is the active COPY FROM STDIN stream, if any. It is written by
	// the session goroutine and read by a cancel arriving on another, so it
	// is guarded like the rest of that group.
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
	s.be.SetMaxBodyLen(min(preAuthMaxMessageBodyLen, s.maxBodyLen()))
}

// maxBodyLen is the configured ceiling for an authenticated session.
func (s *session) maxBodyLen() int {
	if n := s.server.cfg.MaxMessageBodyLen; n > 0 {
		return n
	}
	return DefaultMaxMessageBodyLen
}

func (s *session) send(msgs ...pgproto3.BackendMessage) error {
	for _, m := range msgs {
		if err := s.sendMsg(m); err != nil {
			return err
		}
	}
	return s.flush()
}

// maxRowSlab bounds what a session keeps between results. Rows are encoded
// into a slab so they do not pass through pgproto3's write buffer, which is
// thrown away above a kilobyte on every flush; keeping the slab is the point
// of it, and keeping an arbitrarily large one would trade a copy for
// resident memory on every session that ever returned a wide result.
const maxRowSlab = 64 << 10

// sendMsg buffers one message for the client, after anything already
// encoded into the row slab.
//
// Only one of the two buffers may hold pending bytes at a time, and that is
// what keeps the order: a message sent while rows are pending writes the
// rows first, and a row appended while the backend has something buffered
// flushes the backend first. Nothing else may call s.be.Send once a session
// is serving results.
func (s *session) sendMsg(m pgproto3.BackendMessage) error {
	if err := s.writeRows(); err != nil {
		return err
	}
	s.be.Send(m)
	s.beDirty = true
	return nil
}

// appendRow encodes a DataRow into the slab: 'D', the frame length, the
// column count, then a length and the bytes of each column, with -1 for a
// NULL. pgproto3 builds the same frame, but through a buffer it discards
// above 1 KiB, so every result over a kilobyte grew and copied its way back
// up on every batch.
func (s *session) appendRow(values [][]byte) error {
	if s.beDirty {
		if err := s.flushBackend(); err != nil {
			return err
		}
	}
	n := 6
	for _, v := range values {
		n += 4 + len(v)
	}
	s.rows = append(s.rows, 'D')
	s.rows = binary.BigEndian.AppendUint32(s.rows, uint32(n))
	s.rows = binary.BigEndian.AppendUint16(s.rows, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			s.rows = binary.BigEndian.AppendUint32(s.rows, math.MaxUint32) // -1
			continue
		}
		s.rows = binary.BigEndian.AppendUint32(s.rows, uint32(len(v)))
		s.rows = append(s.rows, v...)
	}
	return nil
}

// writeRows puts the encoded rows on the wire and keeps the slab, unless it
// has grown past what a session should hold on to.
func (s *session) writeRows() error {
	if len(s.rows) == 0 {
		return nil
	}
	_, err := s.conn.Write(s.rows)
	if cap(s.rows) > maxRowSlab {
		s.rows = nil
	} else {
		s.rows = s.rows[:0]
	}
	return err
}

func (s *session) flushBackend() error {
	s.beDirty = false
	return s.be.Flush()
}

// flush writes everything pending and forgets what it owed.
func (s *session) flush() error {
	s.queued = 0
	if err := s.writeRows(); err != nil {
		return err
	}
	return s.flushBackend()
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

// user reports the authenticated role, empty until startup has finished.
func (s *session) user() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info.User
}

func (s *session) forceClose() {
	s.mu.Lock()
	if s.queryCancel != nil {
		s.queryCancel()
	}
	s.mu.Unlock()
	_ = s.conn.Close()
}

// latchRevoked marks the session revoked and stops whatever it is running,
// without touching the socket. Latching is what actually prevents further
// work, so a sweep does this to every session first and only then spends
// time on the courtesy writes; otherwise one unresponsive client delays the
// latch of every session behind it, and those keep executing meanwhile.
// It reports whether this call is the one that has to end the session.
func (s *session) latchRevoked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revoked || s.closed {
		return false
	}
	s.revoked, s.draining = true, true
	if s.queryCancel != nil {
		s.queryCancel()
	}
	// Still authenticating: startup fails it as a bad credential, so
	// nothing is written here.
	return s.serving
}

// endRevoked tells the client why and closes the socket. It closes the
// connection only: the executor belongs to the session's own goroutine and
// is not safe to touch from here, and closing the socket unblocks that
// goroutine, whose deferred close releases it.
func (s *session) endRevoked() {
	er := toErrorResponse(Errorf(CodeAdminShutdown, "terminating connection because the role may no longer log in"))
	er.Severity, er.SeverityUnlocalized = "FATAL", "FATAL"
	if buf, err := er.Encode(nil); err == nil {
		// Bounded: the session goroutine may be blocked flushing to a
		// client that has stopped reading, and this write would queue
		// behind it. One refresh goroutine revokes every session, so it
		// must never wait on one of them.
		_ = s.conn.SetWriteDeadline(time.Now().Add(revokeWriteTimeout))
		_, _ = s.conn.Write(buf)
		_ = s.conn.SetWriteDeadline(time.Time{})
	}
	_ = s.conn.Close()
}

// revokeWriteTimeout bounds the courtesy FATAL a revocation sends.
const revokeWriteTimeout = 2 * time.Second

// revokedNow reports whether a revocation landed on this session, so a
// startup that was in flight hands its executor straight back instead of
// serving a role that may no longer log in.
func (s *session) revokedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revoked
}

// drain asks the session to end: idle sessions are terminated immediately,
// active ones and sessions inside a transaction block once the transaction
// (or the current statement, outside a block) has finished.
func (s *session) drain() {
	s.mu.Lock()
	s.draining = true
	// A session is registered on the server before startup runs, so drain
	// can reach one still authenticating. Writing the FATAL to its socket
	// then puts a second writer on a connection the startup path is still
	// sending through, and the client reads the two interleaved: the
	// terminate it was about to be sent lands inside its startup exchange
	// and is gone by the time it looks for it. A session that has not
	// finished startup ends through the check below instead.
	idle := s.serving && !s.active && !s.inTxn && !s.closed
	s.drainSaw = fmt.Sprintf("serving=%v active=%v inTxn=%v closed=%v -> idle=%v",
		s.serving, s.active, s.inTxn, s.closed, idle)
	s.mu.Unlock()
	if idle {
		// The session goroutine is blocked in Receive, so write the error
		// directly rather than through its buffered backend.
		er := toErrorResponse(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
		er.Severity, er.SeverityUnlocalized = "FATAL", "FATAL"
		if buf, err := er.Encode(nil); err == nil {
			// Bounded for the same reason the startup refusal is: the
			// goodbye is a courtesy and the close is the part that
			// matters, so a peer that has stopped reading must not be
			// able to hold a drain open by never taking it. This one
			// runs on the shutdown path, where the wait would be the
			// whole server's.
			_ = s.conn.SetWriteDeadline(time.Now().Add(refusalWriteTimeout))
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
	// A revoked session takes nothing further, transaction or not: a drain
	// lets an open transaction finish, but a role that may no longer log in
	// does not get to keep working through one.
	if s.revoked || (s.draining && !s.inTxn) || s.closed {
		return false
	}
	s.active = true
	return true
}

func (s *session) endMessage() {
	s.mu.Lock()
	s.active = false
	s.queryCancel, s.queryCtx = nil, nil
	s.mu.Unlock()
}

func (s *session) queryContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	s.mu.Lock()
	s.queryCancel, s.queryCtx = cancel, ctx
	revoked := s.revoked
	s.mu.Unlock()
	if revoked {
		// Revoked between the message starting and the context being
		// installed: revoke had no cancel to call, so this one would
		// otherwise run to completion on a session that is already gone.
		cancel()
	}
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
	// A COPY FROM STDIN is parked in a socket read waiting for the client's
	// next CopyData, and cancelling the query context does not wake it. The
	// client that issued the cancel is commonly waiting for the result
	// before sending anything more, so the two wait for each other and the
	// backend and session are held until one of them gives up. A read
	// deadline in the past ends that read; Next sees the cancelled context,
	// clears the deadline and fails the COPY.
	if s.copyIn != nil {
		_ = s.conn.SetReadDeadline(time.Now())
	}
	return true
}

// setCopyIn records the COPY stream in flight, or clears it.
//
// A cancel that arrives before the COPY is registered finds no stream to
// wake: cancelQuery sees a nil copyIn, sets no deadline, and the read that
// starts a moment later parks for ever waiting for a client that is itself
// waiting for the cancellation's result. That is the same standoff
// cancelQuery exists to break, arriving in the other order, so it gets the
// same answer here.
//
// It is a race the executor loses more often the busier the process is,
// which is why it showed up in CI -- where the whole tree runs four
// packages at a time under the race detector -- and not when the test was
// run on its own.
func (s *session) setCopyIn(c *copyInStream) {
	s.mu.Lock()
	s.copyIn = c
	cancelled := c != nil && s.queryCtx != nil && s.queryCtx.Err() != nil
	s.mu.Unlock()
	if cancelled {
		_ = s.conn.SetReadDeadline(time.Now())
	}
}

// queryCancelled reports whether the statement in flight has been cancelled.
func (s *session) queryCancelled() bool {
	s.mu.Lock()
	ctx := s.queryCtx
	s.mu.Unlock()
	return ctx != nil && ctx.Err() != nil
}

// refusalWriteTimeout bounds a refusal written before the startup deadline
// exists. It is generous: the message is a few hundred bytes, so anything
// slower than this is a peer that is not reading.
const refusalWriteTimeout = 5 * time.Second

func (s *session) run() {
	log := s.server.logger.With("session", s.id, "remote", s.conn.RemoteAddr().String())
	ctx := context.Background()
	if !s.beginMessage() {
		return
	}
	if !s.server.acquireStartup() {
		s.endMessage()
		// Bounded: this path runs before the startup deadline is set, and a
		// peer that never reads would otherwise hold the goroutine and the
		// socket on a write nobody is taking -- which is a way to keep a
		// server busy with connections it has just refused.
		_ = s.conn.SetWriteDeadline(time.Now().Add(refusalWriteTimeout))
		s.terminate(Errorf(CodeTooManyConnections, "sorry, too many clients already (startup)"))
		return
	}
	err := func() error {
		defer s.server.releaseStartup()
		sctx, cancel := context.WithCancel(ctx)
		defer cancel()
		if d := s.server.cfg.StartupTimeout; d > 0 {
			var tcancel context.CancelFunc
			sctx, tcancel = context.WithTimeout(sctx, d)
			defer tcancel()
			_ = s.conn.SetDeadline(time.Now().Add(d))
		}
		// Shutdown must cancel blocking pre-auth work (catalog lookups),
		// not just idle sockets.
		go func() {
			select {
			case <-s.server.shutdownCh:
				cancel()
			case <-sctx.Done():
			}
		}()
		return s.startup(sctx)
	}()
	if err == nil {
		// s.conn may have been replaced by the TLS upgrade; clearing the
		// deadline through it reaches the underlying connection.
		_ = s.conn.SetDeadline(time.Time{})
	}
	s.endMessage()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, errCancelRequest) {
			log.Debug("startup failed", "err", err)
		}
		return
	}
	// The whole of startup counts as one message, so active stays set until
	// the endMessage just above -- while serving is set inside startup, at
	// its end. A drain landing in that window sees a session that is
	// serving and active, takes neither branch, and leaves only the flag.
	// The loop below would not look again until the client sent something,
	// and an idle client has no reason to, so the session waited for a
	// terminate that never came. Now that active is clear, the flag is
	// this session's to act on.
	s.mu.Lock()
	drained := s.draining && !s.inTxn
	s.mu.Unlock()
	if drained {
		s.terminate(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
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

// errCancelRequest is returned before requireTLS runs, so a cancel is
// answered without TLS even on a server that requires it. That matches
// PostgreSQL: the postmaster handles CancelRequest before any hba or SSL
// check, and libpq before 17 sends cancels without negotiating.
var errCancelRequest = errors.New("cancel request handled")

// requireTLS refuses a session that reached the startup message without
// negotiating TLS on a server that has a certificate. The refusal is worth
// making explicit rather than trusting every client to ask: a downgrade is
// silent otherwise.
func (s *session) requireTLS() error {
	if s.server.cfg.TLSConfig == nil || s.server.cfg.AllowPlaintext {
		return nil
	}
	if _, isTLS := s.conn.(*tls.Conn); isTLS {
		return nil
	}
	s.terminate(Errorf(CodeInvalidAuthorization, "the server requires TLS; the connection did not request it"))
	return errors.New("plaintext startup refused: server requires TLS")
}

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
	if err := s.requireTLS(); err != nil {
		return err
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
	// Publish the claimed role before authenticating. SCRAM reads the
	// cached verifier up front, so a revocation that lands while the
	// exchange is still in flight would otherwise skip this session - its
	// role is not known yet - and it would go on to serve on a verifier
	// that has since been withdrawn.
	s.mu.Lock()
	s.info.User = user
	s.mu.Unlock()
	result, err := s.server.cfg.Authenticator.Authenticate(ctx, pkt.params, authExchange{s})
	if err != nil {
		if s.revokedNow() {
			// A revoked session ends as a revoked session, whatever the
			// exchange happened to be failing on at the time. Relaying the
			// authenticator's own message here would tell the client that
			// the role is NOLOGIN or its password expired -- a description
			// of a role the cluster has just withdrawn.
			s.terminate(Errorf(CodeInvalidPassword, "password authentication failed"))
			return errors.New("session revoked during authentication")
		}
		var pe *Error
		if !errors.As(err, &pe) {
			err = Errorf(CodeInvalidAuthorization, "authentication failed: %v", err)
		}
		s.terminate(err)
		return err
	}
	info := SessionInfo{
		ID: s.id, User: user, Database: db, Params: pkt.params,
		ProtocolVersion: s.protocolVersion, Auth: result, RemoteAddr: s.conn.RemoteAddr().String(),
	}
	// The session is already registered on the server, so anything scanning
	// sessions by role can read this concurrently.
	s.mu.Lock()
	s.info = info
	s.mu.Unlock()
	if s.revokedNow() {
		// Indistinguishable from any other authentication failure.
		s.terminate(Errorf(CodeInvalidPassword, "password authentication failed"))
		return errors.New("session revoked during authentication")
	}
	exec, err := s.server.cfg.NewExecutor(info)
	if err != nil {
		s.terminate(err)
		return err
	}
	s.mu.Lock()
	revoked := s.revoked
	if !revoked {
		s.exec = exec
	}
	s.mu.Unlock()
	if revoked {
		// The revocation pass ran between the check above and here; hand
		// the executor back rather than leaking it, and end the session
		// the way a failed authentication ends.
		exec.Release()
		s.terminate(Errorf(CodeInvalidPassword, "password authentication failed"))
		return errors.New("session revoked during authentication")
	}
	key, err := s.server.newCancelKey(s.id, s.protocolVersion)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cancelKey = key
	s.mu.Unlock()
	s.be.Send(&pgproto3.AuthenticationOk{})
	_ = s.be.SetAuthType(pgproto3.AuthTypeOk)
	// Authenticated: this session may now declare the full body length.
	s.be.SetMaxBodyLen(s.maxBodyLen())
	for _, kv := range s.parameterStatus() {
		s.be.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
	}
	s.be.Send(&pgproto3.BackendKeyData{ProcessID: key.PID, SecretKey: key.Secret})
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: byte(exec.TransactionStatus())})
	if err := s.flush(); err != nil {
		return err
	}
	// From here a revocation ends the session outright rather than only
	// latching: the client is authenticated, so telling it why gives
	// nothing away.
	s.mu.Lock()
	// A drain that arrived during startup left only the flag: it is this
	// session's own job to act on it, now that nothing else is writing to
	// the connection.
	if s.draining && !s.inTxn {
		s.mu.Unlock()
		s.terminate(Errorf(CodeAdminShutdown, "terminating connection due to administrator command"))
		return errors.New("session drained during startup")
	}
	s.serving = true
	late := s.revoked
	if late && s.queryCancel != nil {
		s.queryCancel()
	}
	s.mu.Unlock()
	if late {
		// Revoked during the last moments of startup: the latch was set
		// while it could not be acted on, so act on it now.
		s.endRevoked()
	}
	return nil
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
		// Flush must produce the answers to what has been staged, not only
		// push bytes already written: a pipelined client sends Execute
		// then Flush and waits, so buffering until Sync hangs it.
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Flush(qctx, w)
		cancel()
		if err != nil {
			s.reportError(err)
			s.skipToSync = true
		}
		return true, s.flush()
	case *pgproto3.Sync:
		s.skipToSync = false
		// Sync is where an extended-protocol batch actually runs, so it
		// needs the cancellable context the other steps use; otherwise a
		// cancel or a revocation waits for the statement rather than
		// stopping it.
		qctx, cancel := s.queryContext(ctx)
		err := s.exec.Sync(qctx)
		cancel()
		if err != nil {
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
	return s.flush()
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
	s.setCopyIn(nil)
	if err != nil {
		if w.ioErr != nil {
			return w.ioErr
		}
		s.reportError(err)
	}
	return s.readyForQuery()
}
