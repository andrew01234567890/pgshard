package pgwire

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/sync/semaphore"
)

// bodyBudgetFloor is the declared body size at which a message starts
// costing budget. Below it the accounting would cost more than the memory
// it governs: a Query, a Bind and libpq's own COPY chunks are all far
// smaller, so the ordinary traffic of a session never touches the budget
// and never waits for it.
const bodyBudgetFloor = 1 << 20

// bodyStallTimeout is how long a body may go without progress while holding
// budget, and how long a session may wait for budget to be granted.
//
// A client that declares a large body and then stops sending is the case
// the budget exists to survive, and it is also the case that would defeat
// it: the memory is committed on its say-so, and while it holds it nobody
// else can. So the hold is bounded, and a session that overruns it loses
// its connection rather than the rest of the router losing the budget. It
// is idleness that is bounded, not the transfer: a large body arriving
// steadily over a slow link keeps its budget for as long as it keeps
// coming.
var bodyStallTimeout = 30 * time.Second

// bodyBudget is the memory every session together may have committed to
// message bodies that have been declared but not yet arrived.
type bodyBudget struct {
	sem   *semaphore.Weighted
	limit int64
}

// newBodyBudget returns the shared budget, or nil when the server does not
// bound one. The limit is never below one message's ceiling: a budget that
// could not admit a single legal message would refuse it for ever rather
// than make anything wait.
func newBodyBudget(limit int64, ceiling int) *bodyBudget {
	if limit <= 0 {
		return nil
	}
	if limit < int64(ceiling) {
		limit = int64(ceiling)
	}
	return &bodyBudget{sem: semaphore.NewWeighted(limit), limit: limit}
}

// framedReader is the frontend byte stream, read one protocol message at a
// time so the router knows how large a body is before pgproto3 allocates it.
//
// pgproto3 reads the five-byte header, checks the declared length against
// the session's ceiling, and then asks for a buffer of exactly that length
// before a single body byte has arrived. Nothing outside pgproto3 sees the
// declaration, so the only lever was the per-session ceiling and the bound
// was that ceiling times the session count: idle sessions parked on a header
// they never finished sending each cost the maximum.
//
// Reading the header here moves the decision to where the size is known.
// The body is charged to a budget the whole server shares, and the header is
// not handed on -- so pgproto3 does not allocate -- until the budget admits
// it. An idle session costs nothing, because its header has not arrived.
type framedReader struct {
	src    *bufio.Reader
	conn   net.Conn
	budget *bodyBudget
	// done is closed when the server is shutting down, so a session waiting
	// for budget it is never going to get does not hold the shutdown for
	// the length of the stall timeout.
	done <-chan struct{}

	// charging is false until the session has authenticated, so nothing an
	// unauthenticated client sends can take the server's budget. Framing
	// itself is not conditional: it starts with the first byte pgproto3
	// reads and never changes, because a reader that started framing
	// part-way through would take whatever it found for a header. The
	// authentication messages are ordinary frames and pass through it
	// uncharged.
	//
	// The startup packet is not involved either way: it is read straight
	// off the buffered reader below this one and never passes through here.
	charging bool

	hdr [5]byte
	// hdrN is how much of the header has arrived. A read that fails
	// part-way through one has to leave those bytes where the next read
	// will find them, or the stream resumes in the middle of a header.
	hdrN   int
	pend   []byte // header bytes not yet handed to pgproto3
	remain int    // body bytes not yet handed to pgproto3
	charge int64  // budget held for the body in flight; 0 when none
	stall  *time.Timer
}

func newFramedReader(src *bufio.Reader, conn net.Conn, budget *bodyBudget, done <-chan struct{}) *framedReader {
	return &framedReader{src: src, conn: conn, budget: budget, done: done}
}

// charge starts charging bodies. It is called where the session finishes
// authenticating.
func (f *framedReader) startCharging() { f.charging = true }

func (f *framedReader) Read(p []byte) (int, error) {
	if f.budget == nil {
		return f.src.Read(p)
	}
	if len(f.pend) > 0 {
		n := copy(p, f.pend)
		f.pend = f.pend[n:]
		return n, nil
	}
	if f.remain == 0 {
		if err := f.begin(); err != nil {
			return 0, err
		}
		n := copy(p, f.pend)
		f.pend = f.pend[n:]
		return n, nil
	}
	if len(p) > f.remain {
		p = p[:f.remain]
	}
	n, err := f.src.Read(p)
	f.remain -= n
	switch {
	case f.remain == 0:
		f.end()
	case n > 0 && f.stall != nil:
		// Still coming, so the clock starts again: what is bounded is a
		// body that has stopped arriving, not one that is slow.
		f.stall.Reset(bodyStallTimeout)
	}
	return n, err
}

// begin reads the next message's header and charges its body.
func (f *framedReader) begin() error {
	n, err := io.ReadFull(f.src, f.hdr[f.hdrN:])
	f.hdrN += n
	if err != nil {
		return err
	}
	f.hdrN = 0
	// The length covers itself, so a body of zero is a four-byte length and
	// nothing else. A length below four is a protocol error, and pgproto3
	// is the one that reports it -- the header goes on either way.
	body := int(binary.BigEndian.Uint32(f.hdr[1:])) - 4
	if body < 0 {
		body = 0
	}
	f.pend, f.remain = f.hdr[:], body
	if !f.charging || body < bodyBudgetFloor {
		return nil
	}
	if int64(body) > f.budget.limit {
		// Larger than the budget could ever grant, so waiting for it would
		// be waiting for nothing. pgproto3's own ceiling refuses it as soon
		// as it sees the header, which is the answer the client is owed.
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), bodyStallTimeout)
	defer cancel()
	if f.done != nil {
		go func() {
			select {
			case <-f.done:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	if err := f.budget.sem.Acquire(ctx, int64(body)); err != nil {
		f.pend, f.remain = nil, 0
		return fmt.Errorf("%w: %d-byte message body waited for the server's message budget", errBodyBudget, body)
	}
	f.charge = int64(body)
	// Held on this session's say-so, so this session is what gives way if
	// the body stops arriving. Closing the connection is what releases the
	// budget; a read deadline would be answered by whatever else set one.
	f.stall = time.AfterFunc(bodyStallTimeout, func() { _ = f.conn.Close() })
	return nil
}

func (f *framedReader) end() {
	if f.charge == 0 {
		return
	}
	if f.stall != nil {
		f.stall.Stop()
		f.stall = nil
	}
	f.budget.sem.Release(f.charge)
	f.charge = 0
}

// close releases whatever the session was holding when it ended.
func (f *framedReader) close() {
	if f != nil {
		f.end()
	}
}

var errBodyBudget = &budgetError{}

type budgetError struct{}

func (*budgetError) Error() string { return "message body budget exhausted" }
