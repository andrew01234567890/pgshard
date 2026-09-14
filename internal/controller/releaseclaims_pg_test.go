//go:build integration

package controller

import (
	"context"
	"testing"
	"time"
)

// TestReleaseClaimsHandsWorkflowsBack: a claim is otherwise only given up
// by expiry, so a controller that stopped leading left its workflows
// unownable for DefaultOwnerLease -- five minutes of a reshard standing
// still, with any write pause the pass had raised standing with it.
func TestReleaseClaimsHandsWorkflowsBack(t *testing.T) {
	parallelPG(t)
	f := newResolverFixture(t)
	ctx := context.Background()

	var id string
	if err := f.pool.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, '{}'::jsonb) RETURNING id::text`,
		KindReshard, StateRunning).Scan(&id); err != nil {
		t.Fatal(err)
	}
	const oldLeader, successor = "replica-a", "replica-b"

	if _, held, err := claimWorkflow(ctx, f.pool, oldLeader, id, time.Hour); err != nil || !held {
		t.Fatalf("first claim: held=%v err=%v", held, err)
	}
	// A fresh claim is refused while the lease stands: that is the stall.
	if _, held, err := claimWorkflow(ctx, f.pool, successor, id, time.Hour); err != nil || held {
		t.Fatalf("successor took a live claim: held=%v err=%v", held, err)
	}

	n, err := ReleaseClaims(ctx, f.pool, oldLeader)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("released %d claims, want 1", n)
	}
	if _, held, err := claimWorkflow(ctx, f.pool, successor, id, time.Hour); err != nil || !held {
		t.Errorf("the successor still could not claim a released workflow: held=%v err=%v; it waits out the lease", held, err)
	}

	// Scoped: releasing as one replica must not hand away another's.
	if n, err := ReleaseClaims(ctx, f.pool, oldLeader); err != nil || n != 0 {
		t.Errorf("releasing again took %d claim(s) that belong to %s now: %v", n, successor, err)
	}
	var owner *string
	if err := f.pool.QueryRow(ctx, `SELECT owner FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner == nil || *owner != successor {
		t.Errorf("owner is %v, want %q", owner, successor)
	}
}
