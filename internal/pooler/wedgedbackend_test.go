package pooler

import (
	"context"
	"testing"
	"time"
)

// The Execute handler relays through pgproto3, which reads and writes the
// socket with no context anywhere. A backend PostgreSQL will not interrupt
// therefore held this handler, the backend and the session entry for as
// long as it took -- whatever the router had already given up on. The
// router's own waits are bounded, so the router walked away and the pooler
// stayed, and the reservation sweep skips an attached session, so nothing
// on this side ended it either.
func TestARouterThatGivesUpDoesNotLeaveTheSessionAttached(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The fake backend blocks on this one until the harness tears down.
	if err := stream.Send(queryReq("wedged", "select pg_sleep(30)", gen(7, 3), identity("alice"))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.pg.queries.Load() >= 1 })
	waitFor(t, func() bool { return attachedSession(h) })

	// What the router does when its own grace runs out.
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for attachedSession(h) {
		if time.Now().After(deadline) {
			t.Fatal("the session is still attached; the handler is still inside a backend read nothing ends")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// And the backend it was using is not handed to the next session: a read
// cut short mid-statement leaves the protocol state unknown.
func TestABackendCutShortIsDiscardedNotPooled(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(queryReq("wedged", "select pg_sleep(30)", gen(7, 3), identity("alice"))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return attachedSession(h) })
	cancel()
	waitFor(t, func() bool { return !attachedSession(h) })

	// A pooled backend would be reused; a discarded one makes the next
	// session dial again.
	before := h.pg.dials.Load()
	fresh, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, fresh, queryReq("after", "select 1", gen(7, 3), identity("alice")))
	if got := h.pg.dials.Load(); got != before+1 {
		t.Fatalf("dials went %d -> %d; a backend cut short mid-statement must not be pooled", before, got)
	}
}

func attachedSession(h *harness) bool {
	h.srv.mu.Lock()
	defer h.srv.mu.Unlock()
	se := h.srv.sessions["wedged"]
	return se != nil && se.attached
}

// The deadline watcher belongs to the message being relayed, not to the
// backend. A statement that ends hands its backend back to the pool from
// inside the same call the watcher was armed for, so a router giving up a
// moment later would otherwise reach a connection this session no longer
// owns: cutting short the reset in flight on it, and clearing a deadline
// its next owner had just set.
func TestGivingUpDoesNotReachABackendAlreadyBackInThePool(t *testing.T) {
	h := startHarness(t, PoolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reached := make(chan struct{})
	h.pg.holdDiscard.Store(&reached)

	stream, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, stream, queryReq("done", "select 1", gen(7, 3), identity("alice")))

	// The statement is answered and the backend is on its way back to the
	// pool, still resetting.
	<-reached
	cancel()
	// Long enough for a watcher still armed to have acted on it. Without
	// this the reset finishes first and the assertion below holds whether
	// the watcher was scoped correctly or not.
	time.Sleep(100 * time.Millisecond)
	before := h.pg.dials.Load()
	close(h.pg.releaseDiscard)

	// The reset runs on the pooler's own goroutine and the backend is
	// pooled when it returns, so the next session has to be opened after
	// that has happened -- not merely after the fake stopped holding it.
	// Opening it first makes the pool dial a second backend for want of an
	// idle one, which is the very thing this asserts does not happen.
	waitFor(t, func() bool {
		_, idle := h.srv.cfg.Pool.Stats()
		return idle == 1
	})

	// The backend finished its reset and was pooled, so the next session
	// gets it rather than dialling a new one.
	fresh, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, fresh, queryReq("next", "select 1", gen(7, 3), identity("alice")))
	if got := h.pg.dials.Load(); got != before {
		t.Fatalf("dials went %d -> %d; giving up cut short a reset on a backend the session had already released", before, got)
	}
}
