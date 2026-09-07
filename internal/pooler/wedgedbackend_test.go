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
