package catalog

import (
	"context"
	"errors"
	"testing"
)

// TestOnlyTheOwnerMayChangeALiveFence: an unowned writer -- the agent RPC
// reaching any catalog -- must not disturb a fence a barrier is holding.
// Clearing one opens writes in the middle of the barrier that raised it,
// and raising over one takes the owner's stamp away, so the owner's own
// release matches nothing and the cluster stays fenced.
func TestOnlyTheOwnerMayChangeALiveFence(t *testing.T) {
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
