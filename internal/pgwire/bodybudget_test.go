package pgwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/sync/semaphore"
)

// The ceiling alone bounded nothing useful: pgproto3 allocates a body in
// full the moment the five-byte header declares its size, so the memory one
// router could be made to commit was the ceiling times the session count,
// and a session parked on a header it never finished sending cost the
// maximum for as long as it stayed.
func TestADeclaredBodyIsChargedBeforeItIsAllocated(t *testing.T) {
	const budget = 4 << 20
	ts := startServer(t, Config{MaxMessageBodyBudget: budget, MaxMessageBodyLen: budget})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	// A header for three megabytes, and then nothing.
	c.sendHeader('Q', 3<<20)

	waitFor(t, func() bool { return !hasFree(ts, 2<<20) })
	if !hasFree(ts, 1<<20) {
		t.Fatal("the budget charged more than the body declared")
	}

	// And it comes back when the session goes, however it goes.
	c.close()
	waitFor(t, func() bool { return hasFree(ts, budget) })
}

// Nothing an ordinary session sends is near the floor, so it never waits for
// the budget and never holds any: that is what makes an idle session free.
func TestASmallMessageIsNotCharged(t *testing.T) {
	const budget = 1 << 20
	ts := startServer(t, Config{MaxMessageBodyBudget: budget})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if !hasFree(ts, budget) {
		t.Fatal("an ordinary statement took budget it should never have needed")
	}
}

// A client that declares a large body and then stops sending is what the
// budget exists to survive, and also what would defeat it: it holds memory
// on its own say-so while nobody else can have it.
func TestASessionThatStopsSendingLosesItsConnectionAndItsBudget(t *testing.T) {
	old := bodyStallTimeout
	bodyStallTimeout = 200 * time.Millisecond
	defer func() { bodyStallTimeout = old }()

	const budget = 4 << 20
	ts := startServer(t, Config{MaxMessageBodyBudget: budget, MaxMessageBodyLen: budget})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.sendHeader('Q', 3<<20)

	waitFor(t, func() bool { return !hasFree(ts, 2<<20) })
	waitFor(t, func() bool { return hasFree(ts, budget) })
}

// Framing every message by hand is only safe if it reassembles them
// exactly: a body large enough to be charged has to arrive at the executor
// whole, in one message, and the session has to keep working afterwards.
func TestALargeBodyStillArrivesWhole(t *testing.T) {
	ts := startServer(t, Config{MaxMessageBodyBudget: 8 << 20, MaxMessageBodyLen: 8 << 20})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &recordingExecutor{Executor: NewFakeExecutor(), seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	big := "select 1 -- " + strings.Repeat("x", 2<<20)
	c.send(&pgproto3.Query{String: big})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if len(seen) != 1 || seen[0] != big {
		t.Fatalf("the executor saw %d statement(s) of length %d; want one of %d", len(seen), lengthOfFirst(seen), len(big))
	}

	// The stream is still in step for the next message.
	c.send(&pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("the session did not survive a large body")
	}
}

func lengthOfFirst(s []string) int {
	if len(s) == 0 {
		return 0
	}
	return len(s[0])
}

// hasFree reports whether n bytes of budget are available, without keeping
// them: a poll that held what it found would starve the budget it is asking
// about, and would report "charged" however the code behaved.
func hasFree(ts *testServer, n int64) bool {
	if !ts.bodyBudget.TryAcquire(n) {
		return false
	}
	ts.bodyBudget.Release(n)
	return true
}

// sendHeader writes a message header declaring a body of n bytes and sends
// none of it: what a client parked on a declaration looks like from here.
func (c *rawClient) sendHeader(kind byte, n int) {
	c.t.Helper()
	var hdr [5]byte
	hdr[0] = kind
	binary.BigEndian.PutUint32(hdr[1:], uint32(n+4))
	if _, err := c.conn.Write(hdr[:]); err != nil {
		c.t.Fatal(err)
	}
}

func (c *rawClient) close() {
	c.t.Helper()
	if err := c.conn.Close(); err != nil {
		c.t.Fatal(err)
	}
}

// waitForBudget polls cond for up to five seconds; the charge and its
// release both happen on the session's goroutine, not the test's.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Before a session authenticates the reader is a pipe: it hands bytes on
// untouched and charges nothing, whatever they declare.
func TestAnUnarmedReaderChargesNothing(t *testing.T) {
	budget := semaphore.NewWeighted(4 << 20)
	var buf bytes.Buffer
	buf.WriteByte('Q')
	_ = binary.Write(&buf, binary.BigEndian, uint32((3<<20)+4))
	buf.Write(bytes.Repeat([]byte("x"), 8))

	f := newFramedReader(bufio.NewReader(&buf), nil, budget)
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 13 || got[0] != 'Q' {
		t.Fatalf("read %d bytes starting %q; want the stream unchanged", len(got), got[:1])
	}
	if !budget.TryAcquire(4 << 20) {
		t.Fatal("an unauthenticated session took budget")
	}
}
