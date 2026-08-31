package pooler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// woke reports whether a wake-up is waiting for w without taking it: the
// wake-up is the state under test, so reading it must not spend it.
func woke(w *waiter) bool { return len(w.ch) > 0 }

// TestAReleaseWakesTheRoleAndTheBudgetNotEveryone: returning one backend
// closed a channel every parked acquirer watched, so one reusable backend
// woke all of them and all but one went straight back to sleep. It wakes
// the two that can use it: one waiting for that role, which can reuse the
// backend, and one waiting for the budget, which can evict it.
func TestAReleaseWakesTheRoleAndTheBudgetNotEveryone(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 1, MaxPerRole: 1}, pg.dial)
	b, err := p.Acquire(context.Background(), "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	p.mu.Lock()
	rp := p.role("db", "alice")
	role := []*waiter{newWaiter(), newWaiter(), newWaiter()}
	budget := []*waiter{newWaiter(), newWaiter(), newWaiter()}
	for i := range role {
		rp.waiters.park(role[i])
		p.budget.park(budget[i])
	}
	p.mu.Unlock()

	p.Release(b)

	for i := range role {
		want := i == 0
		if got := woke(role[i]); got != want {
			t.Fatalf("role waiter %d woke = %v, want %v", i, got, want)
		}
		if got := woke(budget[i]); got != want {
			t.Fatalf("budget waiter %d woke = %v, want %v", i, got, want)
		}
	}
	p.mu.Lock()
	queued := len(rp.waiters) + len(p.budget)
	p.mu.Unlock()
	if queued != 4 {
		t.Fatalf("%d waiters left queued, want the 4 that were not woken", queued)
	}
}

// TestAWakeUpItCannotUseIsHandedOn: a waiter that is woken and then leaves
// without the backend behind that wake-up takes the wake-up with it. The
// backend would then sit idle while acquirers that could take it stay
// parked until their timeout, so the wake-up passes to the next in line.
//
// A waiter cannot report this itself: its select has both its own resource
// and the wake-up ready, and picks either, so one that wins a freed slot
// leaves with a wake-up still in its channel. The channel is what decides.
func TestAWakeUpItCannotUseIsHandedOn(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 1, MaxPerRole: 1}, pg.dial)

	p.mu.Lock()
	first, second := newWaiter(), newWaiter()
	p.budget.park(first)
	p.budget.park(second)
	p.mu.Unlock()

	p.mu.Lock()
	p.budget.wakeOne()
	p.mu.Unlock()
	if !woke(first) || woke(second) {
		t.Fatal("the wake-up did not go to the waiter at the front of the queue")
	}

	p.leave(&p.budget, first)
	if !woke(second) {
		t.Fatal("a waiter that left without using its wake-up did not hand it on")
	}
}

// TestAWaiterThatWonAnotherResourceHandsItsWakeUpOn: a woken waiter whose
// select had its own resource ready too may take that instead. It reports
// success, but the wake-up is still in its channel, and a waiter that
// claimed to have used it would strand the backend it pointed at.
func TestAWaiterThatWonAnotherResourceHandsItsWakeUpOn(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 2, MaxPerRole: 2}, pg.dial)

	p.mu.Lock()
	rp := p.role("db", "alice")
	first, second := newWaiter(), newWaiter()
	rp.waiters.park(first)
	rp.waiters.park(second)
	rp.waiters.wakeOne()
	p.mu.Unlock()
	if !woke(first) {
		t.Fatal("the wake-up did not go to the waiter at the front of the queue")
	}

	// first took the role semaphore rather than the backend behind its
	// wake-up: it leaves having acquired something, wake-up unspent.
	rp.sem <- struct{}{}
	p.leave(&rp.waiters, first)

	if !woke(second) {
		t.Fatal("a waiter that won a different resource stranded its wake-up")
	}
}

// TestEveryAcquirerIsServedUnderATightBudget: with the wake-ups targeted,
// a lost or misdirected one strands an acquirer until its timeout instead
// of merely costing a scheduler wake. Every acquirer must still be served.
func TestEveryAcquirerIsServedUnderATightBudget(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 2, MaxPerRole: 2, AcquireTimeout: 10 * time.Second}, pg.dial)
	ctx := context.Background()

	var served atomic.Int64
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			role := []string{"alice", "bob"}[i%2]
			for range 20 {
				b, err := p.Acquire(ctx, "db", role, nil, nil)
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				served.Add(1)
				p.Release(b)
			}
		}()
	}
	wg.Wait()
	if n := served.Load(); n != 32*20 {
		t.Fatalf("served %d acquires, want %d", n, 32*20)
	}
}
