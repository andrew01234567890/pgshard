package pooler

import (
	"context"
	"testing"
	"time"
)

// TestARoleIsNotStarvedByItsOwnIdleBackendInAnotherDatabase.
//
// The per-role semaphore is shared by every database a role connects to,
// but idle lists and waiters are per (database, role). So a role that went
// idle in one database held a slot its own waiter in another database could
// neither see nor reclaim.
//
// With MaxPerRole=1: query database A, release, then query database B --
// every acquire timed out while A's connection sat idle and the shard-wide
// budget had room. The pool-wide eviction path was never reached, because
// that budget was never the thing exhausted.
//
// Reproduced by gpt-6-astra with one live idle backend and a global limit
// of four.
func TestARoleIsNotStarvedByItsOwnIdleBackendInAnotherDatabase(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 4, MaxPerRole: 1, AcquireTimeout: 500 * time.Millisecond}, pg.dial)
	ctx := context.Background()

	a, err := p.Acquire(ctx, "app", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Release(a) // idle in "app", still holding alice's only slot

	b, err := p.Acquire(ctx, "reports", "alice", nil, nil)
	if err != nil {
		t.Fatalf("alice cannot reach a second database while her own idle backend holds the only slot: %v", err)
	}
	if b.database != "reports" {
		t.Fatalf("acquired a backend for %q, want reports", b.database)
	}
	p.Release(b)

	// The cap still holds: two LIVE backends for one role must not coexist.
	c, err := p.Acquire(ctx, "app", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, "reports", "alice", nil, nil); err == nil {
		t.Fatal("MaxPerRole stopped being enforced: a second live backend for the same role was handed out")
	}
	p.Release(c)
}
