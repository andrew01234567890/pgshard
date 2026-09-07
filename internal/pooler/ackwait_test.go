package pooler

import (
	"testing"
	"time"
)

// An ack waits for the server to confirm the position it advanced to. It
// used to ask again every ten milliseconds, so the caller heard between
// zero and a full interval after the fact, on a path whose whole purpose is
// to tell a consumer its position is durable.
func TestAnAckHearsTheFlushAsItHappens(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.acked.Store(100)

	// Registered before the flush, which is the case that matters: a
	// waiter that reads the value and then starts waiting must not miss an
	// advance in between.
	flushed, advanced := r.flushedAt()
	if flushed != 0 {
		t.Fatalf("flushed = %d, want 0", flushed)
	}

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		<-advanced
		done <- time.Since(start)
	}()

	time.Sleep(20 * time.Millisecond)
	r.noteFlushed(100)

	select {
	case took := <-done:
		// Generous: the point is that it is woken rather than polled, and
		// a poll would have cost up to its own interval on top.
		if took > 2*time.Second {
			t.Fatalf("the wait took %v; it was not woken by the flush", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the flush did not wake the waiter")
	}
	if got := r.flushed.Load(); got != 100 {
		t.Fatalf("flushed = %d, want 100", got)
	}
}

// A flush that has already happened must not leave a waiter waiting for the
// next one: the value and the channel come from the same lock, so the
// caller sees the advance it already missed.
func TestAnAckThatIsAlreadySatisfiedDoesNotWait(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.noteFlushed(50)
	if flushed, _ := r.flushedAt(); flushed != 50 {
		t.Fatalf("flushed = %d, want the advance that already happened", flushed)
	}
}

// Every waiter is woken, not one of them.
func TestEveryWaiterHearsTheFlush(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	var chans []<-chan struct{}
	for range 4 {
		_, c := r.flushedAt()
		chans = append(chans, c)
	}
	r.noteFlushed(7)
	for i, c := range chans {
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d was not woken", i)
		}
	}
}

// The reader parks in a receive for a quarter of a second at a time. An ack
// that lands while it is parked waits out the rest of that before its
// status goes out, which is most of the latency an ack pays -- so while one
// is outstanding the wait is short.
func TestTheReaderWaitsLessWhileAnAckIsOutstanding(t *testing.T) {
	const normal = 250 * time.Millisecond
	r := &streamReader{wake: make(chan struct{}, 1)}

	if got := receiveWait(normal, r); got != normal {
		t.Fatalf("idle wait = %v, want the full %v", got, normal)
	}
	r.acked.Store(100)
	if got := receiveWait(normal, r); got >= normal {
		t.Fatalf("wait with an ack outstanding = %v, want less than %v", got, normal)
	}
	r.noteFlushed(100)
	if got := receiveWait(normal, r); got != normal {
		t.Fatalf("wait after the flush = %v, want the full %v again", got, normal)
	}
	// A deployment that configured a wait shorter than the ack-pending one
	// keeps its own.
	if got := receiveWait(time.Millisecond, r); got != time.Millisecond {
		t.Fatalf("configured wait = %v, want it kept", got)
	}
}
