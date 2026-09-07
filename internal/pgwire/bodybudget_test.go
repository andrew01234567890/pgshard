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
	c.sendQueryHeader(3 << 20)

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
	c.sendQueryHeader(3 << 20)

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
	if !ts.bodyBudget.sem.TryAcquire(n) {
		return false
	}
	ts.bodyBudget.sem.Release(n)
	return true
}

// sendQueryHeader writes a Query header declaring a body of n bytes and
// sends none of it: what a client parked on a declaration looks like from
// here.
func (c *rawClient) sendQueryHeader(n int) {
	c.t.Helper()
	var hdr [5]byte
	hdr[0] = 'Q'
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

// Before a session authenticates the reader still frames -- starting to
// frame part-way through a stream would take whatever it found for a header
// -- but it charges nothing, whatever the message declares.
func TestAnUnauthenticatedSessionIsFramedButNotCharged(t *testing.T) {
	budget := newBodyBudget(4<<20, 4<<20)
	var buf bytes.Buffer
	buf.WriteByte('Q')
	_ = binary.Write(&buf, binary.BigEndian, uint32((3<<20)+4))
	buf.Write(bytes.Repeat([]byte("x"), 8))

	f := newFramedReader(bufio.NewReader(&buf), nil, budget, nil)
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 13 || got[0] != 'Q' {
		t.Fatalf("read %d bytes starting %q; want the stream unchanged", len(got), got[:1])
	}
	if !budget.sem.TryAcquire(4 << 20) {
		t.Fatal("an unauthenticated session took budget")
	}
}

// A client may put its password and its first statement in one write, and
// then pgproto3's read-ahead has already pulled part of that statement off
// the socket while the session is still authenticating. A reader that
// started framing at that point would take whatever it found for a header
// -- so framing runs from the first byte, and only the charge waits for the
// client to prove who it is.
func TestFramingSurvivesAClientThatPipelinesPastAuthentication(t *testing.T) {
	ts := startServer(t, Config{
		MaxMessageBodyBudget: 4 << 20,
		MaxMessageBodyLen:    4 << 20,
		Authenticator:        CleartextAuthenticator{Lookup: lookup(map[string]string{"alice": "s3cret"})},
	})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &recordingExecutor{Executor: NewFakeExecutor(), seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.rawStartup(ProtocolVersion30, map[string]string{"user": "alice", "database": "db"})
	if _, ok := c.recv().(*pgproto3.AuthenticationCleartextPassword); !ok {
		t.Fatal("want a password request")
	}

	// Both messages in one write, so the second is on the socket before the
	// first has been answered.
	pw := &pgproto3.PasswordMessage{Password: "s3cret"}
	// Larger than pgproto3's read-ahead buffer, so it cannot arrive whole
	// inside the read that answers the password: part of it is on the
	// socket while the session is still authenticating, which is the case a
	// small pipelined statement would pass through by luck.
	sql := "select 1 -- " + strings.Repeat("x", 20<<10)
	q := &pgproto3.Query{String: sql}
	buf, err := pw.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	buf, err = q.Encode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.conn.Write(buf); err != nil {
		t.Fatal(err)
	}

	// The fake executor refuses the statement, which is fine: what matters
	// is that it was handed the statement, whole, rather than the session
	// dying on a header read out of the middle of it.
	// Two: the one that ends authentication, and the one that ends the
	// statement that was already on the socket when it did.
	for ready := 0; ready < 2; {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			ready++
		}
	}
	if len(seen) != 1 || seen[0] != sql {
		t.Fatalf("the executor saw %d statement(s) of length %d; want one of %d", len(seen), lengthOfFirst(seen), len(sql))
	}

	// Mis-framing does not corrupt the stream -- every byte still passes
	// through in order -- so the statement above arrives either way. What
	// it corrupts is the accounting: a reader that took body bytes for a
	// header is counting a message that does not exist, and the next real
	// body goes by uncharged inside it.
	c.sendQueryHeader(3 << 20)
	waitFor(t, func() bool { return !hasFree(ts, 2<<20) })
}

// A body larger than the budget could ever grant must be refused as soon as
// its header is read. Waiting for a grant that cannot come would hold the
// session, and its goroutine, for the whole stall timeout and then fail it
// with the wrong reason.
func TestABodyLargerThanTheWholeBudgetIsRefusedAtOnce(t *testing.T) {
	old := bodyStallTimeout
	bodyStallTimeout = 30 * time.Second
	defer func() { bodyStallTimeout = old }()

	ts := startServer(t, Config{MaxMessageBodyBudget: 2 << 20, MaxMessageBodyLen: 2 << 20})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	start := time.Now()
	c.sendQueryHeader(1 << 30)
	er, ok := c.recv().(*pgproto3.ErrorResponse)
	if !ok {
		t.Fatalf("got %T, want the oversize message refused", er)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the refusal took %v; it waited for a grant that could never come", took)
	}
	if strings.Contains(er.Message, "budget") {
		t.Fatalf("refused as %q; an oversize message is over the ceiling, not waiting for budget", er.Message)
	}
}

// What is bounded is a body that has stopped arriving. One that keeps
// coming, slowly, is a slow client rather than a stalled one, and closing
// it would make the budget a bandwidth requirement.
func TestABodyThatKeepsArrivingSlowlyIsNotClosed(t *testing.T) {
	old := bodyStallTimeout
	bodyStallTimeout = 300 * time.Millisecond
	defer func() { bodyStallTimeout = old }()

	ts := startServer(t, Config{MaxMessageBodyBudget: 4 << 20, MaxMessageBodyLen: 4 << 20})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	// A Query body is a NUL-terminated string; without the terminator the
	// session ends on a protocol error rather than on anything this test
	// is about.
	body := append([]byte("select 1 -- "+strings.Repeat("x", (2<<20)-12)), 0)
	c.sendQueryHeader(len(body))
	for off := 0; off < len(body); off += 64 << 10 {
		end := min(off+(64<<10), len(body))
		if _, err := c.conn.Write(body[off:end]); err != nil {
			t.Fatalf("the connection was closed while the body was still arriving: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
}

// stutterReader returns part of the header, then an error, then the rest:
// what a read deadline landing between two bytes of a header looks like.
type stutterReader struct {
	first []byte
	rest  []byte
	done  bool
}

func (s *stutterReader) Read(p []byte) (int, error) {
	if !s.done {
		s.done = true
		return copy(p, s.first), errStutter
	}
	if len(s.rest) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.rest)
	s.rest = s.rest[n:]
	return n, nil
}

var errStutter = &budgetError{}

// A read that fails part-way through a header has to leave those bytes
// where the next read will find them. Dropping them resumes the stream in
// the middle of a header, and every message after it is misread -- and the
// session does read on after some failures, a cancelled COPY among them.
func TestAHeaderInterruptedPartWayThroughIsNotLost(t *testing.T) {
	msg, err := (&pgproto3.Query{String: "select 1"}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	src := &stutterReader{first: msg[:2], rest: msg[2:]}
	f := newFramedReader(bufio.NewReader(src), nil, newBodyBudget(4<<20, 4<<20), nil)
	f.startCharging()

	if _, err := f.Read(make([]byte, 64)); err == nil {
		t.Fatal("want the interrupted read reported")
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("read %q after the interruption; want the whole message, header included", got)
	}
}
