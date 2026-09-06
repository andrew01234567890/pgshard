package router

import (
	"context"
	"testing"
	"time"
)

// TestACancelThatIsNeverAnsweredGivesUp: the cancel is best-effort. It can
// fail, and a backend can be wedged somewhere PostgreSQL will not
// interrupt it, and the router used to wait for the drain regardless --
// for ever. That held the session's goroutine, the pooler session behind
// it and the router's own drain open with nothing able to end them.
//
// The observable is that the pooler sees the session released. Without the
// grace nothing ever releases it, because the goroutine that would is
// still waiting on a batch that will not finish.
func TestACancelThatIsNeverAnsweredGivesUp(t *testing.T) {
	defer func(d time.Duration) { cancelGrace = d }(cancelGrace)
	cancelGrace = 250 * time.Millisecond

	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = conn.Exec(ctx, "select wedged()")
	}()
	// Let the statement reach the pooler, then cancel it.
	time.Sleep(150 * time.Millisecond)
	cancel()

	deadline := time.After(5 * time.Second)
	for {
		h.fp.mu.Lock()
		released := len(h.fp.releases) > 0
		cancels := len(h.fp.cancels)
		h.fp.mu.Unlock()
		if released {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("the session was never released after a cancel nothing answered (%d cancel(s) sent): the router is still waiting on a batch that will not finish", cancels)
		case <-time.After(20 * time.Millisecond):
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the statement never returned to the client")
	}
}

// TestAScatterCancelThatIsNeverAnsweredGivesUp: the scatter pump read with
// context.Background() and the drain after cancelAll waited on <-p.done
// unconditionally, so the grace the single-shard path applies -- and that
// docs/router.md promises -- never applied to a scatter at all. A shard
// whose pooler accepts the Cancel and answers nothing held the session
// goroutine, every participant's stream and reserved backend, and its
// scatter slots, until the client disconnected. Repeat it and the slot
// budget is gone and later scatters are refused with 53300.
func TestAScatterCancelThatIsNeverAnsweredGivesUp(t *testing.T) {
	defer func(d time.Duration) { cancelGrace = d }(cancelGrace)
	cancelGrace = 250 * time.Millisecond

	h := newShardedHarness(t)
	freeSlots := func() int {
		h.r.scatter.mu.Lock()
		defer h.r.scatter.mu.Unlock()
		return h.r.scatter.free
	}
	before := freeSlots()
	conn := h.connect(t, h.dsn())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = conn.Exec(ctx, "select wedged_scatter from orders")
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		var cancels int
		for _, fp := range h.poolers {
			fp.mu.Lock()
			cancels += len(fp.cancels)
			fp.mu.Unlock()
		}
		t.Fatalf("the scatter never returned after a cancel nothing answered (%d cancel(s) sent): the merge is still waiting on batches that will not finish", cancels)
	}
	// And the slots it held are back, so later scatters can run: holding
	// them is how repeated occurrences exhaust the budget.
	deadline := time.After(5 * time.Second)
	for freeSlots() != before {
		select {
		case <-deadline:
			t.Fatalf("scatter slots still held after the statement returned: %d free, want %d", freeSlots(), before)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
