package pooler

import (
	"context"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestTheFenceIsRecheckedAfterWaitingForABackend.
//
// The fence was evaluated on a view read BEFORE the acquire, and the acquire
// waits -- up to AcquireTimeout. The epoch can be bumped, the generation can
// move and the member can stop serving in that window, and the statement was
// then written to the socket regardless.
//
// The correlation runs the wrong way, which is what makes it worth
// re-checking rather than accepting: backends are scarcest, so the wait is
// longest, exactly during a failover or a cutover flip -- the moments the
// fence exists for. "The pooler is the fence that makes a stateless router
// safe" was only as tight as that wait.
func TestTheFenceIsRecheckedAfterWaitingForABackend(t *testing.T) {
	ctx := context.Background()
	h := startHarness(t, PoolConfig{MaxBackends: 1, MaxPerRole: 1, AcquireTimeout: 3 * time.Second})

	// Pin the only backend, so the next request must wait for it.
	if res, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: "pinned", Generation: gen(7, 3)}); err != nil || res.Error != nil {
		t.Fatalf("reserve: %v %v", err, res.GetError())
	}
	pinned, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinned.CloseSend() })
	if e := firstError(roundTrip(t, pinned, queryReq("pinned", "select 1", gen(7, 3), identity("alice")))); e != nil {
		t.Fatalf("pinning statement: %v", e)
	}

	// A second session asks for a backend it cannot have yet, and the epoch
	// is bumped while it waits -- exactly the window the re-check covers.
	go func() {
		time.Sleep(150 * time.Millisecond)
		h.src.Set(View{Generation: 7, Epoch: 9, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true})
		_ = pinned.CloseSend()
		_, _ = h.client.Release(ctx, &pgshardv1.ReleaseRequest{SessionId: "pinned"})
	}()

	waiter, err := h.client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waiter.CloseSend() })
	e := firstError(roundTrip(t, waiter, queryReq("waiter", "select 1", gen(7, 3), identity("alice"))))
	if e == nil {
		t.Fatal("a statement gated on an epoch that moved while it waited for a backend was executed anyway")
	}
	if e.GetSqlstate() != "55000" {
		t.Fatalf("refused, but not by the fence: %s %s", e.GetSqlstate(), e.GetMessage())
	}
}
