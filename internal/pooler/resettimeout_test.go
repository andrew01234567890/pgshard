package pooler

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestAResetOnAWedgedBackendGivesUp: recycle runs ROLLBACK and DISCARD ALL
// on a backend's way back to the pool. On a backend that has stopped
// answering, those queue behind whatever it is still doing and the wait
// had nothing to end it -- holding the pooler session, its goroutine, and
// the drain a shutdown waits on.
func TestAResetOnAWedgedBackendGivesUp(t *testing.T) {
	// A peer that accepts the connection and then says nothing, which is
	// what a backend stuck inside a statement looks like from here.
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close(); _ = c2.Close() }()
	b := &Backend{conn: c1, fe: pgproto3.NewFrontend(bufio.NewReader(c1), c1)}

	start := time.Now()
	err := b.simpleQueryWithin(100*time.Millisecond, "ROLLBACK")
	took := time.Since(start)
	if err == nil {
		t.Fatal("a reset on a backend that never answers must fail, not succeed")
	}
	if took > 2*time.Second {
		t.Fatalf("the reset took %v; the deadline did not bound it", took)
	}
	// Broken, so its caller discards it rather than handing it to the next
	// session: a backend that could not be reset has state nobody knows.
	if !b.broken {
		t.Fatal("a backend that failed its reset must be marked broken")
	}
}

// TestAResponsiveBackendKeepsNoDeadline: the deadline is for the reset,
// not for the connection. One left behind would fire in the middle of some
// later client's query, which is a far stranger failure than the one this
// prevents.
func TestAResponsiveBackendKeepsNoDeadline(t *testing.T) {
	l := listen(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		be := pgproto3.NewBackend(bufio.NewReader(conn), conn)
		for {
			msg, err := be.Receive()
			if err != nil {
				return
			}
			if _, ok := msg.(*pgproto3.Query); ok {
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
			}
		}
	}()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	b := &Backend{conn: conn, fe: pgproto3.NewFrontend(bufio.NewReader(conn), conn)}
	if err := b.simpleQueryWithin(5*time.Second, "ROLLBACK"); err != nil {
		t.Fatalf("a responsive backend must reset: %v", err)
	}
	if b.broken {
		t.Fatal("a backend that answered must not be marked broken")
	}
	// The deadline is cleared, so a later read blocks rather than expiring.
	// Reading with nothing to read must time out on the test's terms, not
	// on a deadline the reset left behind.
	_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	start := time.Now()
	_, _ = conn.Read(make([]byte, 1))
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Fatalf("a read returned after %v; the reset's deadline was left on the connection", took)
	}
}

func TestTheResetBoundIsTheOneRecycleUses(t *testing.T) {
	if resetTimeout < time.Second || resetTimeout > time.Minute {
		t.Fatalf("resetTimeout is %v: long enough to be generous, short enough to bound a drain", resetTimeout)
	}
}
