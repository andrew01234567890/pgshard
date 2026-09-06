package catalog

import (
	"context"
	"errors"
	"os/exec"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// requireDocker skips (or, where docker is required, fails) rather than
// letting `docker run` fail as a test failure: every other PostgreSQL-backed
// test in this package skips without a daemon, and these two turned a
// machine with no Docker into a red suite.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker daemon unavailable")
	}
}

// TestOnlyTheOwnerMayChangeALiveFence: an unowned writer -- the agent RPC
// reaching any catalog -- must not disturb a fence a barrier is holding.
// Clearing one opens writes in the middle of the barrier that raised it,
// and raising over one takes the owner's stamp away, so the owner's own
// release matches nothing and the cluster stays fenced.
func TestOnlyTheOwnerMayChangeALiveFence(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := SetWriteFence(ctx, conn, true, "an unowned pause"); err != nil {
		t.Fatalf("an unowned fence is this path's own business: %v", err)
	}
	if err := RaiseWriteFence(ctx, conn, "barrier b1", "the-barrier-holding-it"); err != nil {
		t.Fatal(err)
	}
	if err := SetWriteFence(ctx, conn, false, ""); !errors.Is(err, ErrFenceOwned) {
		t.Fatalf("an owned fence was released by someone who does not hold it: %v", err)
	}
	if _, err := ReleaseWriteFence(ctx, conn, "the-barrier-holding-it"); err != nil {
		t.Fatalf("the owner must still be able to release: %v", err)
	}
}

// TestARestoredCatalogCanBeUnfenced: the certified restore point is taken
// while the barrier holds the fence -- Barrier.run raises it, then creates
// the point on every group -- so a catalog restored to that point comes
// back stamped with an owner that finished before the backup was even
// restored and can never return to release it. Left to the owner rule, the
// restored cluster would stay fenced for good; the restore path is the one
// caller entitled to clear a fence it does not own.
func TestARestoredCatalogCanBeUnfenced(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// What the backup captured.
	if err := RaiseWriteFence(ctx, conn, "barrier b1", "an-owner-from-before-the-restore"); err != nil {
		t.Fatal(err)
	}
	// What the agent RPC would do, and why it is not enough here.
	if err := SetWriteFence(ctx, conn, false, ""); !errors.Is(err, ErrFenceOwned) {
		t.Fatalf("expected the owner rule to refuse the ordinary path: %v", err)
	}
	if err := ClearWriteFenceAfterRestore(ctx, conn); err != nil {
		t.Fatal(err)
	}
	var fenced bool
	var owner string
	if err := conn.QueryRow(ctx, `SELECT write_fence, write_fence_owner FROM pgshard.shard_map_generation`).Scan(&fenced, &owner); err != nil {
		t.Fatal(err)
	}
	if fenced || owner != "" {
		t.Fatalf("fence=%v owner=%q; a restored catalog must come back unfenced and unowned", fenced, owner)
	}
}

// TestOnlyAnInterruptedRunsFenceIsClearedByRecovery: a barrier whose
// controller died leaves a fence nothing would ever clear, and recovery has
// to lift it. A cluster restored to a barrier comes back holding that
// barrier's fence -- owner, reason and all -- and that one must stay up until
// two-phase reconciliation finishes. What separates them is the restore
// point: a run reserves its row uncertified before raising the fence and
// certifies it just before releasing.
func TestOnlyAnInterruptedRunsFenceIsClearedByRecovery(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	read := func() (bool, string) {
		t.Helper()
		var fenced bool
		var owner string
		if err := conn.QueryRow(ctx, `SELECT write_fence, write_fence_owner FROM pgshard.shard_map_generation`).Scan(&fenced, &owner); err != nil {
			t.Fatal(err)
		}
		return fenced, owner
	}
	reserve := func(name string, certified bool) {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO pgshard.restore_points (id, name, shard_map_generation, certified)
			VALUES (gen_random_uuid(), $1, 1, $2)`, name, certified); err != nil {
			t.Fatal(err)
		}
	}

	// Nothing raised: nothing to clear.
	if cleared, err := ClearStaleBarrierFence(ctx, conn); err != nil || cleared {
		t.Fatalf("cleared=%v err=%v with no fence raised", cleared, err)
	}

	// An unowned fence was raised by something that is not a barrier.
	if err := SetWriteFence(ctx, conn, true, "restoring"); err != nil {
		t.Fatal(err)
	}
	if cleared, err := ClearStaleBarrierFence(ctx, conn); err != nil || cleared {
		t.Fatalf("an unowned fence was cleared: cleared=%v err=%v", cleared, err)
	}
	if err := SetWriteFence(ctx, conn, false, ""); err != nil {
		t.Fatal(err)
	}

	// What a restored cluster comes back holding: the fence of a barrier
	// that completed, so its row is certified.
	reserve("nightly", true)
	if err := RaiseWriteFence(ctx, conn, "barrier nightly", "the-barrier-that-completed"); err != nil {
		t.Fatal(err)
	}
	if cleared, err := ClearStaleBarrierFence(ctx, conn); err != nil || cleared {
		t.Fatalf("a restored cluster was unfenced mid-reconciliation: cleared=%v err=%v", cleared, err)
	}
	if fenced, _ := read(); !fenced {
		t.Fatal("the restored cluster's fence was lifted under it")
	}

	// What an interrupted run leaves: the row was reserved and never
	// certified.
	reserve("interrupted", false)
	if err := RaiseWriteFence(ctx, conn, "barrier interrupted", "a-controller-that-died"); err != nil {
		t.Fatal(err)
	}
	cleared, err := ClearStaleBarrierFence(ctx, conn)
	if err != nil || !cleared {
		t.Fatalf("an interrupted run's fence was left up: cleared=%v err=%v", cleared, err)
	}
	if fenced, owner := read(); fenced || owner != "" {
		t.Fatalf("fence=%v owner=%q after recovery", fenced, owner)
	}
}
