package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// shadowTables lists the placement artifacts a pass has created on a shard.
func shadowTables(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func ownerRow(t *testing.T, f *placementFixture, id string) (state, stage, owner string, updated time.Time) {
	t.Helper()
	if err := f.catalog.QueryRow(context.Background(),
		`SELECT state, coalesce(status->>'stage', ''), coalesce(owner, ''), updated_at FROM pgshard.workflows WHERE id = $1::uuid`,
		id).Scan(&state, &stage, &owner, &updated); err != nil {
		t.Fatal(err)
	}
	return
}

// startPlacement creates a sharded table, re-keys it, and drives the
// workflow to the copying stage, where a pass still has shadow tables to
// fill and renames ahead of it.
func startPlacement(t *testing.T, f *placementFixture) string {
	t.Helper()
	for id := range int32(2) {
		c := f.app(id)
		mustExec(t, c, `CREATE TABLE orders (id bigint NOT NULL, tenant_id bigint NOT NULL, region_id bigint NOT NULL, note text, PRIMARY KEY (id, tenant_id, region_id))`)
	}
	conns := []*pgx.Conn{f.app(0), f.app(1)}
	for i := range int64(200) {
		tenant, region := i*7919+13, i%97
		mustExec(t, conns[f.shardOf(tenant)], `INSERT INTO orders VALUES ($1, $2, $3, $4)`, i, tenant, region, "n")
	}
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'orders', 'sharded', 'tenant_id')`)
	if res := f.reconcile(); res.TablesMadeEffective != 1 {
		t.Fatalf("%+v", res)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET shard_key = 'region_id' WHERE table_name = 'orders'`)
	if res := f.reconcile(); res.WorkflowsCreated != 1 {
		t.Fatalf("%+v", res)
	}
	id, _ := f.driveUntil("orders", 60*time.Second, StagePlacementCopying, StagePlacementCatchUp)
	return id
}

// TestPlacementPassStopsWhenTheWorkflowIsTakenOver: leadership was checked
// between passes only, so a replica that lost it mid-pass carried on
// creating shadow tables, renaming and publishing against shards a new
// leader was already driving. A pass now holds a claim on the workflow and
// stops at its next step when the claim is gone.
func TestPlacementPassStopsWhenTheWorkflowIsTakenOver(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	_, stageBefore, _, updatedBefore := ownerRow(t, f, id)
	before := [][]string{shadowTables(t, f.app(0)), shadowTables(t, f.app(1))}

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'another-replica', owned_at = now() WHERE id = $1::uuid`, id)

	for range 3 {
		out, err := f.placer.Pass(ctx)
		if err != nil {
			t.Fatalf("a taken-over workflow must not fail the pass: %v", err)
		}
		if out.Driven != 0 || out.Advanced != 0 || out.Failed != 0 {
			t.Fatalf("a pass drove a workflow it does not own: %+v", out)
		}
	}

	state, stage, owner, updated := ownerRow(t, f, id)
	if owner != "another-replica" {
		t.Fatalf("the claim was stolen back from the live owner: %q", owner)
	}
	if stage != stageBefore || !updated.Equal(updatedBefore) || state == StateFailed {
		t.Fatalf("a taken-over workflow was written: state %s stage %s (was %s), updated %s (was %s)",
			state, stage, stageBefore, updated, updatedBefore)
	}
	for shard := range int32(2) {
		if got := shadowTables(t, f.app(shard)); strings.Join(got, ",") != strings.Join(before[shard], ",") {
			t.Fatalf("shard %d was mutated after the takeover: %v, was %v", shard, got, before[shard])
		}
	}
}

// TestPlacementPassStopsWhenTheClaimIsRevokedMidPass revokes the claim
// after the pass has taken it, which is what a replica that lost leadership
// to a peer sees. The step in flight is the last thing it does.
func TestPlacementPassStopsWhenTheClaimIsRevokedMidPass(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	wf := f.load(id)
	wf.owner = "a-claim-that-was-revoked"
	_, stageBefore, _, updatedBefore := ownerRow(t, f, id)
	before := [][]string{shadowTables(t, f.app(0)), shadowTables(t, f.app(1))}

	advanced, err := f.placer.drive(ctx, wf)
	if !isNotOwner(err) {
		t.Fatalf("a revoked pass kept driving: advanced=%v err=%v", advanced, err)
	}
	if err := f.placer.save(ctx, wf, "status from a pass that lost its claim"); !isNotOwner(err) {
		t.Fatalf("a revoked pass wrote status: %v", err)
	}

	_, stage, _, updated := ownerRow(t, f, id)
	if stage != stageBefore || !updated.Equal(updatedBefore) {
		t.Fatalf("a revoked pass wrote the workflow: stage %s (was %s), updated %s (was %s)", stage, stageBefore, updated, updatedBefore)
	}
	for shard := range int32(2) {
		if got := shadowTables(t, f.app(shard)); strings.Join(got, ",") != strings.Join(before[shard], ",") {
			t.Fatalf("shard %d was mutated after the revocation: %v, was %v", shard, got, before[shard])
		}
	}
}

// TestTwoPassesOnOneWorkflowOnlyOneAdvancesIt: two replicas that both
// believe they lead drive the same workflow. Exactly one advances it; the
// other stops without failing anything.
func TestTwoPassesOnOneWorkflowOnlyOneAdvancesIt(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	other := *f.placer
	other.Replica = "the-other-replica"
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = NULL, owned_at = NULL WHERE id = $1::uuid`, id)

	first, err := f.placer.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _, owner, _ := ownerRow(t, f, id)
	if owner == "" {
		t.Fatal("a pass drove a workflow without claiming it")
	}
	if first.Driven != 1 {
		t.Fatalf("the claiming replica did not drive the workflow: %+v", first)
	}

	second, err := other.Pass(ctx)
	if err != nil {
		t.Fatalf("the replica without the claim failed its pass: %v", err)
	}
	if second.Driven != 0 || second.Advanced != 0 {
		t.Fatalf("both replicas drove the same workflow: %+v", second)
	}
}

func isNotOwner(err error) bool { return err != nil && errors.Is(err, errNotOwner) }

// TestPassStopsWhenTheWorkflowStateMovedUnderIt: a claim alone is not
// enough. A workflow cancelled or failed by another writer while a pass was
// mid-stage would have taken that pass's next status write on top of the new
// state; the write carries the state the pass started from as well.
func TestPassStopsWhenTheWorkflowStateMovedUnderIt(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	wf := f.load(id)
	var owner string
	if err := f.catalog.QueryRow(ctx, `SELECT coalesce(owner, '') FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	wf.owner, wf.fence = owner, wf.state
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET state = $2 WHERE id = $1::uuid`, id, StateCancelled)

	if err := f.placer.save(ctx, wf, "status from a pass whose state moved"); !isNotOwner(err) {
		t.Fatalf("a pass wrote over a state it did not start from: %v", err)
	}
	var state string
	if err := f.catalog.QueryRow(ctx, `SELECT state FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != StateCancelled {
		t.Fatalf("the stale pass overwrote the state: %s", state)
	}
}
