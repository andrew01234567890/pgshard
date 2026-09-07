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

// bodyStallTimeout is how long a session may hold budget for a body that is
// not arriving, and how long it may wait for budget to be granted.
//
// A client that declares a large body and then stops sending is the case
// the budget exists to survive, and it is also the case that would defeat
// it: the memory is committed on its say-so, and while it holds it nobody
// else can. So the hold is bounded, and a session that overruns it loses
// its connection rather than the rest of the router losing the budget.
var bodyStallTimeout = 30 * time.Second

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
	budget *semaphore.Weighted

	// armed is false until the session has authenticated, so nothing an
	// unauthenticated client sends can take the server's budget.
	//
	// The startup packet is not the reason: it is read straight off the
	// buffered reader below this one and never passes through here at all.
	// The authentication messages do pass through, and they are ordinary
	// frames -- framing them would work. What it would mean is that a
	// client which has proved nothing could hold budget the rest of the
	// server needs. Today the pre-auth ceiling is well under the floor, so
	// it could not reach it anyway; that is two constants agreeing rather
	// than a rule, and this is the rule.
	armed bool

	hdr    [5]byte
	pend   []byte // header bytes not yet handed to pgproto3
	remain int    // body bytes not yet handed to pgproto3
	charge int64  // budget held for the body in flight; 0 when none
	stall  *time.Timer
}

func newFramedReader(src *bufio.Reader, conn net.Conn, budget *semaphore.Weighted) *framedReader {
	return &framedReader{src: src, conn: conn, budget: budget}
}

// arm starts framing. It is called where the session finishes
// authenticating, which is a message boundary: everything the client sends
// from here is an ordinary frontend message.
func (f *framedReader) arm() {
	if f.budget != nil {
		f.armed = true
	}
}

func (f *framedReader) Read(p []byte) (int, error) {
	if !f.armed {
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
	if f.remain == 0 {
		f.end()
	}
	return n, err
}

// begin reads the next message's header and charges its body.
func (f *framedReader) begin() error {
	if _, err := io.ReadFull(f.src, f.hdr[:]); err != nil {
		return err
	}
	// The length covers itself, so a body of zero is a four-byte length and
	// nothing else. A length below four is a protocol error, and pgproto3
	// is the one that reports it -- the header goes on either way.
	body := int(binary.BigEndian.Uint32(f.hdr[1:])) - 4
	if body < 0 {
		body = 0
	}
	f.pend, f.remain = f.hdr[:], body
	if body < bodyBudgetFloor {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), bodyStallTimeout)
	defer cancel()
	if err := f.budget.Acquire(ctx, int64(body)); err != nil {
		f.pend, f.remain = nil, 0
		return fmt.Errorf("%w: %d-byte message body waited for the server's message budget", errBodyBudget, body)
	}
	f.charge = int64(body)
	// Held on this session's say-so, so this session is what gives way if
	// the body does not arrive. Closing the connection is what releases the
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
	f.budget.Release(f.charge)
	f.charge = 0
}

// close releases whatever the session was holding when it ended.
func (f *framedReader) close() {
	f.end()
}

var errBodyBudget = &budgetError{}

type budgetError struct{}

func (*budgetError) Error() string { return "message body budget exhausted" }

// newBodyBudget returns the shared budget, or nil when the server does not
// bound one. The limit is never below one message's ceiling: a budget that
// could not admit a single legal message would refuse it for ever rather
// than make anything wait.
func newBodyBudget(limit int64, ceiling int) *semaphore.Weighted {
	if limit <= 0 {
		return nil
	}
	if limit < int64(ceiling) {
		limit = int64(ceiling)
	}
	return semaphore.NewWeighted(limit)
}
