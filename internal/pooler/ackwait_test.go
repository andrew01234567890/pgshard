package pooler

import (
	"context"
	"sync"
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

// The wait is woken by the flush, not by a clock. A poll would return up to
// its own interval late, so what this measures is the delay between the
// flush happening and the wait returning -- which a ten-millisecond poll
// cannot keep under a millisecond, and a signal does not notice.
//
// Worst of several attempts, because a poll that happens to tick just after
// the flush looks like a signal once.
func TestTheWaitIsWokenByTheFlushRatherThanAClock(t *testing.T) {
	var worst time.Duration
	for i := range 9 {
		r := &streamReader{wake: make(chan struct{}, 1)}
		// Spread across a ten-millisecond cycle, starting just past its
		// first tick. A poll that has just looked has to wait most of an
		// interval before it looks again; sampling only inside the first
		// interval would let one look like a signal.
		delay := 11*time.Millisecond + time.Duration(i)*time.Millisecond
		flushed := make(chan time.Time, 1)
		go func() {
			time.Sleep(delay)
			flushed <- time.Now()
			r.noteFlushed(100)
		}()
		if err := r.awaitFlush(context.Background(), 100, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		if late := time.Since(<-flushed); late > worst {
			worst = late
		}
	}
	if worst > 5*time.Millisecond {
		t.Fatalf("the wait returned %v after the flush at worst; a signal returns at once and a 10ms poll does not", worst)
	}
}

// A wait that is already satisfied returns without waiting at all.
func TestAWaitThatIsAlreadySatisfiedReturnsAtOnce(t *testing.T) {
	r := &streamReader{wake: make(chan struct{}, 1)}
	r.noteFlushed(100)
	start := time.Now()
	if err := r.awaitFlush(context.Background(), 100, time.Millisecond); err != nil {
		t.Fatalf("awaitFlush: %v", err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("took %v for a position already flushed", took)
	}
}

// The value and the channel have to come from one lock. Reading the value
// first and then registering leaves a window: a flush that lands in it
// closes a channel nobody holds yet, and the waiter then waits for the next
// one -- for a position that is already durable.
//
// Contention rather than a single attempt, because the window is only a few
// instructions wide: many waiters register while flushes land underneath
// them, and every waiter must either see a value that covers it or be woken.
func TestRegisteringForTheNextFlushCannotMissThisOne(t *testing.T) {
	const rounds, waiters = 300, 8
	for round := range rounds {
		r := &streamReader{wake: make(chan struct{}, 1)}
		target := uint64(round + 1)
		var wg sync.WaitGroup
		stranded := make(chan uint64, waiters)
		for range waiters {
			wg.Add(1)
			go func() {
				defer wg.Done()
				flushed, advanced := r.flushedAt()
				if flushed >= target {
					return
				}
				select {
				case <-advanced:
				case <-time.After(2 * time.Second):
					stranded <- flushed
				}
			}()
		}
		go r.noteFlushed(target)
		wg.Wait()
		close(stranded)
		if saw, ok := <-stranded; ok {
			t.Fatalf("round %d: a waiter saw flushed=%d and then waited for an advance that had already happened", round, saw)
		}
	}
}
