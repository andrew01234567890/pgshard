package pgwire

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// deadlineConn records the write deadlines set on it and never lets a write
// through, which is the peer the refusal path has to survive.
type deadlineConn struct {
	net.Conn
	mu        sync.Mutex
	deadlines []time.Time
	blocked   chan struct{}
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return nil
}

func (c *deadlineConn) Write([]byte) (int, error) {
	<-c.blocked
	return 0, os.ErrDeadlineExceeded
}

func (c *deadlineConn) wroteUnderADeadline() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range c.deadlines {
		if !d.IsZero() {
			return true
		}
	}
	return false
}

// TestTheStartupRefusalIsWrittenUnderADeadline: the connection cap refuses
// before the startup deadline exists, so the courtesy FATAL it writes has
// to carry a deadline of its own. Without one a peer that never reads holds
// the goroutine and the socket on a write nobody is taking -- a way to keep
// a server busy with the connections it has just refused.
func TestTheStartupRefusalIsWrittenUnderADeadline(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	blocked := make(chan struct{})
	dc := &deadlineConn{Conn: server, blocked: blocked}

	srv, err := NewServer(Config{MaxStartupConns: 1, Authenticator: TrustAuthenticator{},
		NewExecutor: func(SessionInfo) (Executor, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	// Take the only startup slot, so the session under test is refused.
	if !srv.acquireStartup() {
		t.Fatal("the first startup slot must be free")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handle(dc)
	}()

	// The write blocks until we let it fail, as a peer that never reads
	// would. The session must still finish.
	close(blocked)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the refusal never returned")
	}
	if !dc.wroteUnderADeadline() {
		t.Errorf("the refusal was written with no deadline: %v", dc.deadlines)
	}
}
