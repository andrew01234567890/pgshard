package pooler

import (
	"context"
	"fmt"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestPreparedSessionsCeilingAtTheBackendBudget measures what pinning
// costs. A reserved session -- which is what a client using named
// statements, session GUCs or a SQL-level PREPARE becomes -- holds its
// backend between statements, so a pool of N backends serves N such
// clients and no more, however many connect. A session that is never
// reserved holds a backend only while its statement runs, so the same pool
// serves any number of them.
//
// This is the number the prepared-statement work is measured against: it
// is the ceiling that keeps a prepared workload short of transaction
// pooling.
func TestPreparedSessionsCeilingAtTheBackendBudget(t *testing.T) {
	const budget, clients = 4, 12
	h := startHarness(t, PoolConfig{MaxBackends: budget, MaxPerRole: budget, AcquireTimeout: 200 * time.Millisecond})
	ctx := context.Background()

	pinned := 0
	for i := 0; i < clients; i++ {
		sid := fmt.Sprintf("pinned-%d", i)
		res, err := h.client.Reserve(ctx, &pgshardv1.ReserveRequest{SessionId: sid, Generation: gen(7, 3)})
		if err != nil || res.Error != nil {
			break
		}
		stream, err := h.client.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stream.CloseSend() })
		if e := firstError(roundTrip(t, stream, queryReq(sid, "select 1", gen(7, 3), identity("alice")))); e != nil {
			break
		}
		pinned++
	}
	t.Logf("backend budget %d: %d of %d pinned clients served, %d backends held", budget, pinned, clients, h.srv.held())
	if pinned != budget {
		t.Errorf("pinned clients served = %d, want the whole budget and no more (%d)", pinned, budget)
	}

	// The same budget and the same number of clients, none of them pinned.
	// A fresh pool: the pinned clients above are still holding every
	// backend they took, which is the starvation this measures against.
	h = startHarness(t, PoolConfig{MaxBackends: budget, MaxPerRole: budget, AcquireTimeout: 200 * time.Millisecond})
	unpinned := 0
	for i := 0; i < clients; i++ {
		// A stream carries one session, so an unpinned client is a stream
		// of its own that ends when its statement does.
		stream, err := h.client.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		e := firstError(roundTrip(t, stream, queryReq(fmt.Sprintf("free-%d", i), "select 1", gen(7, 3), identity("alice"))))
		_ = stream.CloseSend()
		if e != nil {
			break
		}
		unpinned++
	}
	t.Logf("backend budget %d: %d of %d unpinned clients served", budget, unpinned, clients)
	if unpinned != clients {
		t.Errorf("unpinned clients served = %d, want all %d: a backend is only held while a statement runs", unpinned, clients)
	}
}
