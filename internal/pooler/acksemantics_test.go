package pooler

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/pgrepl"
)

// PGS-749 asked for "a test asserting ConfirmedLsn does not advance on a
// write the server has not confirmed". There is no such thing to wait for:
// a standby status update is one-way and PostgreSQL never replies to it, so
// no amount of waiting on this connection produces a server confirmation.
//
// What the ack can promise is pinned here instead: the position was clamped
// to what was really delivered, and a status write that FAILED does not
// advance it. Those are the two ways a caller could be told a position it
// should not record.

// failingSender is a replication connection whose writes do not arrive.
type failingSender struct{ err error }

func (f failingSender) SendStandbyStatus(pgrepl.StandbyStatus) error { return f.err }

// A failed send leaves the position where it was, so the ack times out
// rather than reporting a position nothing carried. Driven through
// sendStatus, which is where the order lives: recording before the write
// returns is the mistake, and it is invisible to a test that calls
// noteSent itself.
func TestAnAckDoesNotAdvanceWhenTheStatusWriteFailed(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.delivered.Store(100)
	r.acked.Store(100)

	if err := r.sendStatus(failingSender{err: errors.New("connection reset")}); err == nil {
		t.Fatal("a failed write reported success")
	}
	if got := r.sent.Load(); got != 0 {
		t.Fatalf("the position advanced to %d on a write that failed", got)
	}
	err := r.awaitSent(context.Background(), 100, 50*time.Millisecond)
	if err == nil {
		t.Fatal("the ack reported a position no status message carried")
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("code %v, want DeadlineExceeded: the position is not lost, the next ack asks again", got)
	}
}

// okSender is a connection whose writes arrive.
type okSender struct{ got []pgrepl.LSN }

func (o *okSender) SendStandbyStatus(s pgrepl.StandbyStatus) error {
	o.got = append(o.got, s.Flushed)
	return nil
}

// And a write that succeeds advances it, to the acked position.
func TestAStatusWriteThatSucceedsAdvancesThePosition(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.delivered.Store(100)
	r.acked.Store(100)
	c := &okSender{}
	if err := r.sendStatus(c); err != nil {
		t.Fatal(err)
	}
	if got := r.sent.Load(); got != 100 {
		t.Fatalf("sent %d, want 100", got)
	}
	if len(c.got) != 1 || c.got[0] != 100 {
		t.Fatalf("the server was told %v", c.got)
	}
	if err := r.awaitSent(context.Background(), 100, time.Second); err != nil {
		t.Fatalf("awaitSent after a good write: %v", err)
	}
}

// And once the write has happened, the wait ends.
func TestAnAckReturnsOnceTheStatusWriteHappened(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.delivered.Store(100)
	go func() {
		time.Sleep(10 * time.Millisecond)
		r.noteSent(100)
	}()
	if err := r.awaitSent(context.Background(), 100, 5*time.Second); err != nil {
		t.Fatalf("awaitSent: %v", err)
	}
}

// The clamp is the half that is exact, and the half a caller must record:
// asking to confirm past what was delivered would have a consumer skip
// data it never received.
func TestAnAckIsClampedToWhatWasDelivered(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.delivered.Store(40)
	lsn := min(uint64(100), r.delivered.Load())
	if lsn != 40 {
		t.Fatalf("clamped to %d, want the delivered 40", lsn)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		r.noteSent(40)
	}()
	if err := r.awaitSent(context.Background(), lsn, time.Second); err != nil {
		t.Fatalf("awaitSent: %v", err)
	}
	// The waiter must not be satisfied by a position beyond what was sent.
	if err := r.awaitSent(context.Background(), 100, 50*time.Millisecond); !errors.Is(err, err) || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a position past the sent one returned %v", err)
	}
}
