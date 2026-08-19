package pooler

import (
	"bufio"
	"context"
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
}

func (d Dialer) dial(ctx context.Context) (net.Conn, error) {
	network := "tcp"
	if len(d.Address) > 0 && d.Address[0] == '/' {
		network = "unix"
	}
	nd := net.Dialer{Timeout: d.Timeout}
	return nd.DialContext(ctx, network, d.Address)
}

// Backend is one authenticated PostgreSQL connection driven over pgproto3.
type Backend struct {
	conn     net.Conn
	fe       *pgproto3.Frontend
	role     string
	pid      uint32
	secret   []byte
	born     time.Time
	lastUsed time.Time
	txStatus byte
	broken   bool
	// unflushed counts messages buffered in fe but not yet written.
	unflushed int
	// prepared names the statements this connection may hold; the set
	// outlives the session that parsed them so a reused backend is never
	// asked to PREPARE a name it already has.
	prepared preparedSet
}

// dialBackend performs startup and SCRAM-SHA-256 with forwarded keys. It
// does not retain the keys.
func dialBackend(ctx context.Context, d Dialer, database, role string, clientKey, serverKey []byte) (*Backend, error) {
	conn, err := d.dial(ctx)
	if err != nil {
		return nil, err
	}
	b := &Backend{conn: conn, role: role, born: time.Now(), txStatus: 'I'}
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
	for {
		msg, err := b.fe.Receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.AuthenticationOk:
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
		case *pgproto3.AuthenticationCleartextPassword, *pgproto3.AuthenticationMD5Password:
			return errors.New("backend requested password authentication; only SCRAM-SHA-256 is supported")
		case *pgproto3.BackendKeyData:
			b.pid, b.secret = m.ProcessID, m.SecretKey
		case *pgproto3.ParameterStatus, *pgproto3.NoticeResponse:
		case *pgproto3.ReadyForQuery:
			b.txStatus = m.TxStatus
			return nil
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("backend refused connection: %s: %s", m.Code, m.Message)
		default:
			return fmt.Errorf("unexpected startup message %T", msg)
		}
	}
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

// simpleQuery runs sql and drains responses; used for reset commands.
func (b *Backend) simpleQuery(sql string) error {
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
