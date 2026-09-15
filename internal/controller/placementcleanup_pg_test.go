package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// claimLostOnDial hands the workflow to another replica when a pass makes
// its at'th dial, and then lets that dial through: the pass carries on with
// its claim gone, as one stalled past the lease does.
type claimLostOnDial struct {
	ShardDBDialer
	pool  *pgxpool.Pool
	id    string
	at    int
	mu    sync.Mutex
	dials int
}

func (d *claimLostOnDial) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	d.mu.Lock()
	d.dials++
	if d.dials == d.at {
		_, _ = d.pool.Exec(ctx, `UPDATE pgshard.workflows SET owner = 'replica-b', owned_at = now() WHERE id = $1::uuid`, d.id)
	}
	d.mu.Unlock()
	return d.ShardDBDialer.DialDatabase(ctx, set, id, db)
}

// TestAFailedPlacementStopsCleaningUpWhenItLosesItsClaim (PGS-843): fail
// checked the claim once and then dialled every source and every shard,
// dropping replication and shadows, and finally the table lock -- all of
// which the replica that had claimed the workflow meanwhile was using. The
// claim is checked before each shard and before the lock.
func TestAFailedPlacementStopsCleaningUpWhenItLosesItsClaim(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	startPlacement(t, f)
	id, _ := f.driveUntil("orders", time.Minute, StagePlacementCatchUp)
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'replica-a', owned_at = now() WHERE id = $1::uuid`, id)
	wf := f.load(id)
	wf.owner, wf.fence = "replica-a", wf.state
	sources, holders := wf.from.Sources(), wf.rt.Holders()
	if len(sources) < 2 || len(holders) < 2 {
		t.Fatalf("sources %v, holders %v: this needs two of each", sources, holders)
	}
	count := func(shards []int32, sql func(int32) string, arg func(int32) string) int {
		t.Helper()
		n := 0
		for _, s := range shards {
			n += int(queryOne[int64](t, f.app(s), sql(s), arg(s)))
		}
		return n
	}
	slots := func() int {
		return count(sources, func(int32) string { return `SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1` }, wf.slotName)
	}
	shadows := func() int {
		return count(holders, func(int32) string { return `SELECT count(*) FROM pg_class WHERE relname = $1` }, func(int32) string { return wf.shadow() })
	}
	if slots() != len(sources) || shadows() != len(holders) {
		t.Fatalf("slots %d of %d, shadows %d of %d: the workflow has not built what this test needs", slots(), len(sources), shadows(), len(holders))
	}
	locked := func() bool {
		return queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, id) == 1
	}

	// Lost while dropping the first source's replication: that goes, and
	// nothing after it. The fence is released on every fenced shard first.
	fenced, err := f.placer.fencedShards(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	f.placer.Shards = &claimLostOnDial{ShardDBDialer: f.placer.Shards, pool: f.pool, id: id, at: len(fenced) + 1}
	if err := f.placer.fail(ctx, wf, fatal("a failure found by a pass about to lose its claim")); !isNotOwner(err) {
		t.Fatalf("a pass that lost its claim mid-cleanup: %v, want errNotOwner", err)
	}
	if n := slots(); n != len(sources)-1 {
		t.Errorf("%d of %d replication slots left; a pass that lost its claim at the first source must stop there", n, len(sources))
	}
	if n := shadows(); n != len(holders) {
		t.Errorf("a pass that lost its claim dropped %d of %d shadows the new owner is copying into", len(holders)-n, len(holders))
	}
	if !locked() {
		t.Error("a pass that lost its claim dropped the table lock")
	}
	if state, _, owner, _ := ownerRow(t, f, id); state == StateFailed || owner != "replica-b" {
		t.Errorf("the workflow is %s owned by %q, want it left to replica-b", state, owner)
	}
}

// TestAFailedPlacementKeepsTheShadowsOfAClaimLostDuringItsReplicationDrops:
// a claim lost on the last source is seen before the first shadow.
func TestAFailedPlacementKeepsTheShadowsOfAClaimLostDuringItsReplicationDrops(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'replica-a', owned_at = now() WHERE id = $1::uuid`, id)
	wf := f.load(id)
	wf.owner, wf.fence = "replica-a", wf.state
	fenced, err := f.placer.fencedShards(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	holders := wf.rt.Holders()
	shadows := func() int64 {
		t.Helper()
		var n int64
		for _, h := range holders {
			n += queryOne[int64](t, f.app(h), `SELECT count(*) FROM pg_class WHERE relname = $1`, wf.shadow())
		}
		return n
	}
	if shadows() != int64(len(holders)) {
		t.Fatal("the workflow has not built its shadows, so this proves nothing")
	}
	f.placer.Shards = &claimLostOnDial{ShardDBDialer: f.placer.Shards, pool: f.pool, id: id, at: len(fenced) + len(wf.from.Sources())}
	if err := f.placer.fail(ctx, wf, fatal("a failure found by a pass about to lose its claim")); !isNotOwner(err) {
		t.Fatalf("a pass that lost its claim on its last source: %v, want errNotOwner", err)
	}
	if n := shadows(); n != int64(len(holders)) {
		t.Errorf("a pass that lost its claim dropped %d of %d shadows", int64(len(holders))-n, len(holders))
	}
}

// TestAFailedPlacementKeepsTheLockItLostDuringItsLastDrop: a claim lost on
// the last shard reached is seen only after every drop; the table lock is
// the new owner's by then.
func TestAFailedPlacementKeepsTheLockItLostDuringItsLastDrop(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'replica-a', owned_at = now() WHERE id = $1::uuid`, id)
	wf := f.load(id)
	wf.owner, wf.fence = "replica-a", wf.state
	fenced, err := f.placer.fencedShards(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	last := len(fenced) + len(wf.from.Sources()) + len(wf.rt.Holders())
	f.placer.Shards = &claimLostOnDial{ShardDBDialer: f.placer.Shards, pool: f.pool, id: id, at: last}
	if err := f.placer.fail(ctx, wf, fatal("a failure found by a pass about to lose its claim")); !isNotOwner(err) {
		t.Fatalf("a pass that lost its claim on its last drop: %v, want errNotOwner", err)
	}
	if n := queryOne[int64](t, f.catalog, `SELECT count(*) FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, id); n != 1 {
		t.Error("a pass that lost its claim on its last drop dropped the table lock")
	}
}

// TestAFailedPlacementDropsShadowsOnlyWhereItCouldHaveBuiltThem: shadows
// are built on the new placement's holders. A shard outside them that
// cannot be reached is not a shadow left behind.
func TestAFailedPlacementDropsShadowsOnlyWhereItCouldHaveBuiltThem(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)
	wf := f.load(id)
	wf.rt.placement.Placement = "unsharded"
	broken := &brokenShard{ShardDBDialer: f.placer.Shards}
	for _, s := range wf.rt.ids {
		if s != wf.rt.home {
			broken.broken.Store(s + 1)
		}
	}
	f.placer.Shards = broken
	if err := f.placer.dropShadows(ctx, wf); err != nil {
		t.Fatalf("a shard that holds nothing under the new placement was dialled for its shadow: %v", err)
	}
}

// TestAFailedPlacementWithoutRoutingSaysItsShadowsMayRemain: a resumed pass
// whose routing could not be loaded fails without knowing where shadows
// were built, and records that instead of nothing.
func TestAFailedPlacementWithoutRoutingSaysItsShadowsMayRemain(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)
	wf := f.load(id)
	wf.rt = nil
	if err := f.placer.fail(ctx, wf, fatal("serving shard set changed")); err != nil {
		t.Fatal(err)
	}
	leaked := queryOne[string](t, f.catalog, `SELECT coalesce(status->>'leaked', '') FROM pgshard.workflows WHERE id = $1::uuid`, id)
	if !strings.Contains(leaked, "shadow tables may remain") {
		t.Fatalf("leaked = %q, want it to say the shadows may remain", leaked)
	}
}
