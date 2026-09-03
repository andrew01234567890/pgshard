package pooler

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Dialer opens raw connections to the shard's PostgreSQL. Address is a
// "host:port" or a unix socket path (starting with '/').
type Dialer struct {
	Address string
	Timeout time.Duration
	// TLS, when set, upgrades TCP connections with SSLRequest before the
	// startup message (sslmode require, or verify-full when the config
	// verifies the server). A backend that declines TLS is refused. Unix
	// socket connections are never upgraded.
	TLS *tls.Config
}

func (d Dialer) dial(ctx context.Context) (net.Conn, error) {
	network := "tcp"
	if len(d.Address) > 0 && d.Address[0] == '/' {
		network = "unix"
	}
	nd := net.Dialer{Timeout: d.Timeout}
	conn, err := nd.DialContext(ctx, network, d.Address)
	if err != nil || network != "tcp" || d.TLS == nil {
		return conn, err
	}
	tc, err := upgradeTLS(ctx, conn, d.TLS)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tc, nil
}

// upgradeTLS sends SSLRequest and starts TLS on conn once the backend
// answers 'S'.
func upgradeTLS(ctx context.Context, conn net.Conn, cfg *tls.Config) (net.Conn, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	fe := pgproto3.NewFrontend(bufio.NewReader(conn), conn)
	fe.Send(&pgproto3.SSLRequest{})
	if err := fe.Flush(); err != nil {
		return nil, err
	}
	var answer [1]byte
	if _, err := conn.Read(answer[:]); err != nil {
		return nil, fmt.Errorf("backend SSLRequest: %w", err)
	}
	if answer[0] != 'S' {
		return nil, errors.New("backend declined TLS")
	}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("backend TLS handshake: %w", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return tc, nil
}

// Backend is one authenticated PostgreSQL connection driven over pgproto3.
type Backend struct {
	conn     net.Conn
	fe       *pgproto3.Frontend
	role     string
	database string
	pid      uint32
	secret   []byte
	born     time.Time
	lastUsed time.Time
	txStatus byte
	broken   bool
	// released is true while the pool owns the backend rather than a
	// caller: set when it is returned, cleared when it is handed out. It
	// is what makes a second return a no-op instead of a double free.
	released bool
	// credDigest fingerprints the SCRAM keys that authenticated this
	// backend; an idle backend is only handed to a session presenting the
	// same keys.
	credDigest [32]byte
	// unflushed counts messages buffered in fe but not yet written.
	unflushed int
	// prepared names the statements this connection may hold; the set
	// outlives the session that parsed them so a reused backend is never
	// asked to PREPARE a name it already has.
	prepared preparedSet
	// sqlPrepared is set when the backend may hold statements created by a
	// SQL-level PREPARE the pooler did not parse the name out of; every
	// extended-protocol Parse then closes its name first, and the backend
	// is reset with DISCARD ALL before reuse.
	sqlPrepared bool
}

// dialBackend performs startup and SCRAM-SHA-256 with forwarded keys. It
// does not retain the keys.
func dialBackend(ctx context.Context, d Dialer, database, role string, clientKey, serverKey []byte) (*Backend, error) {
	conn, err := d.dial(ctx)
	if err != nil {
		return nil, err
	}
	b := &Backend{conn: conn, role: role, database: database, born: time.Now(), txStatus: 'I'}
	b.fe = pgproto3.NewFrontend(bufio.NewReader(conn), conn)
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := b.authenticate(database, role, clientKey, serverKey); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	b.lastUsed = time.Now()
	return b, nil
}

func (b *Backend) authenticate(database, role string, clientKey, serverKey []byte) error {
	b.fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": role, "database": database, "application_name": "pgshard-pooler"}})
	if err := b.fe.Flush(); err != nil {
		return err
	}
	var sc *scramClient
	verified := false
	for {
		msg, err := b.fe.Receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
			if !verified {
				return errors.New("backend accepted the connection without a verified SCRAM-SHA-256 exchange")
			}
		case *pgproto3.AuthenticationSASL:
			if !contains(m.AuthMechanisms, "SCRAM-SHA-256") {
				return fmt.Errorf("backend offers %v, want SCRAM-SHA-256", m.AuthMechanisms)
			}
			sc, err = newSCRAMClient(role, clientKey, serverKey)
			if err != nil {
				return err
			}
			b.fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: sc.clientFirstMessage()})
			if err := b.fe.Flush(); err != nil {
				return err
			}
		case *pgproto3.AuthenticationSASLContinue:
			if sc == nil {
				return errors.New("backend sent SASLContinue before SASL")
			}
			final, err := sc.clientFinalMessage(m.Data)
			if err != nil {
				return err
			}
			b.fe.Send(&pgproto3.SASLResponse{Data: final})
			if err := b.fe.Flush(); err != nil {
				return err
			}
		case *pgproto3.AuthenticationSASLFinal:
			if sc == nil {
				return errors.New("backend sent SASLFinal before SASL")
			}
			if err := sc.verifyServerFinal(m.Data); err != nil {
				return err
			}
			verified = true
		case *pgproto3.AuthenticationCleartextPassword, *pgproto3.AuthenticationMD5Password:
			return errors.New("backend requested password authentication; only SCRAM-SHA-256 is supported")
		case *pgproto3.BackendKeyData:
			b.pid, b.secret = m.ProcessID, m.SecretKey
		case *pgproto3.ParameterStatus, *pgproto3.NoticeResponse:
		case *pgproto3.ReadyForQuery:
			b.txStatus = m.TxStatus
			return nil
		case *pgproto3.ErrorResponse:
			return &startupError{code: m.Code, message: m.Message}
		default:
			return fmt.Errorf("unexpected startup message %T", msg)
		}
	}
}

// startupError is a refusal PostgreSQL sent during connection startup, such
// as 3D000 for a database that does not exist; it keeps the SQLSTATE so the
// pooler can report it to the router verbatim.
type startupError struct {
	code    string
	message string
}

func (e *startupError) Error() string {
	return fmt.Sprintf("backend refused connection: %s: %s", e.code, e.message)
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// send buffers msg in the frontend; nothing reaches PostgreSQL until flush.
func (b *Backend) send(msg pgproto3.FrontendMessage) {
	b.fe.Send(msg)
	b.unflushed++
}

// hasUnflushed reports whether buffered messages have not been written yet.
// A backend in that state must never be reused or drained with a
// simple query: that would flush the pending pipeline into PostgreSQL.
func (b *Backend) hasUnflushed() bool { return b.unflushed > 0 }

func (b *Backend) flush() error {
	if err := b.fe.Flush(); err != nil {
		b.broken = true
		return err
	}
	b.unflushed = 0
	return nil
}

func (b *Backend) receive() (pgproto3.BackendMessage, error) {
	msg, err := b.fe.Receive()
	if err != nil {
		b.broken = true
		return nil, err
	}
	if r, ok := msg.(*pgproto3.ReadyForQuery); ok {
		b.txStatus = r.TxStatus
		b.lastUsed = time.Now()
	}
	return msg, nil
}

func (b *Backend) idle() bool { return b.txStatus == 'I' }

// resetTimeout bounds the reset statements a backend runs on its way back
// to the pool.
//
// ROLLBACK and DISCARD ALL take under a millisecond on a backend that is
// answering. One that is not answering is the case this exists for: the
// reset queues behind whatever the backend is still doing, and recycle
// waits for it with nothing able to end the wait -- holding the pooler
// session, the goroutine, and the drain that a shutdown is waiting on.
//
// Generous rather than tight, because the only cost of waiting is the
// wait: a backend that misses this is discarded rather than reused, which
// is what should happen to one that cannot answer ROLLBACK in ten seconds.
const resetTimeout = 10 * time.Second

// simpleQuery runs sql and drains responses; used for reset commands.
func (b *Backend) simpleQuery(sql string) error {
	return b.simpleQueryWithin(resetTimeout, sql)
}

// simpleQueryWithin is simpleQuery with a deadline on the socket. The
// deadline is cleared on the way out, so a backend that answered in time
// goes back to the pool without one; a backend that did not is broken and
// gets discarded by its caller, so what its socket carries stops mattering.
func (b *Backend) simpleQueryWithin(d time.Duration, sql string) error {
	// Captured, because close() sets b.conn to nil and the deferred clear
	// would then be a nil dereference on the path that already failed.
	if conn := b.conn; conn != nil {
		_ = conn.SetDeadline(time.Now().Add(d))
		defer func() { _ = conn.SetDeadline(time.Time{}) }()
	}
	return b.runSimpleQuery(sql)
}

func (b *Backend) runSimpleQuery(sql string) error {
	b.send(&pgproto3.Query{String: sql})
	if err := b.flush(); err != nil {
		return err
	}
	var qerr error
	for {
		msg, err := b.receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			qerr = fmt.Errorf("%s: %s", m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			return qerr
		}
	}
}

// cancel sends a CancelRequest for this backend over a fresh connection.
func (b *Backend) cancel(ctx context.Context, d Dialer) error {
	conn, err := d.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	buf, err := (&pgproto3.CancelRequest{ProcessID: b.pid, SecretKey: b.secret}).Encode(nil)
	if err != nil {
		return err
	}
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Read(make([]byte, 1))
	return nil
}

func (b *Backend) close() {
	if b.conn == nil {
		return
	}
	b.fe.Send(&pgproto3.Terminate{})
	_ = b.fe.Flush()
	_ = b.conn.Close()
	b.conn = nil
}
