package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// The fence is the state the pass started from, so only the owner half
	// of the guard can refuse this write.
	wf.owner, wf.fence = "a-claim-that-was-revoked", wf.state
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

// TestAnExpiredClaimIsTakenOver: a replica that dies mid-pass leaves its
// claim behind. Nothing else would ever take the workflow if the claim
// outlived its owner, so it is only held while it is refreshed.
func TestAnExpiredClaimIsTakenOver(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'a-replica-that-died', owned_at = now() - interval '1 hour' WHERE id = $1::uuid`, id)
	out, err := f.placer.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Driven != 1 {
		t.Fatalf("an expired claim stranded the workflow: %+v", out)
	}
	_, _, owner, _ := ownerRow(t, f, id)
	if owner == "a-replica-that-died" || owner == "" {
		t.Fatalf("the workflow was driven without taking the claim over: %q", owner)
	}

	// A live claim is not stealable, so the two cannot be confused.
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owner = 'a-live-replica', owned_at = now() WHERE id = $1::uuid`, id)
	if out, err = f.placer.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if out.Driven != 0 {
		t.Fatalf("a live claim was stolen: %+v", out)
	}
}

// TestALongStepKeepsItsClaimAlive: copying one source shard is a single
// step over a whole table and can outrun the lease. A pass that is still
// making progress keeps its claim; one that is not loses it.
func TestALongStepKeepsItsClaimAlive(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	if _, err := f.placer.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	var owner string
	var before time.Time
	if err := f.catalog.QueryRow(ctx, `SELECT coalesce(owner, ''), owned_at FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&owner, &before); err != nil {
		t.Fatal(err)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET owned_at = now() - interval '1 hour' WHERE id = $1::uuid`, id)

	if err := holdClaim(ctx, f.pool, id, owner); err != nil {
		t.Fatalf("a step of the owning pass lost the claim: %v", err)
	}
	var after time.Time
	if err := f.catalog.QueryRow(ctx, `SELECT owned_at FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before.Add(-time.Second)) {
		t.Fatalf("the step did not refresh the lease: %s", after)
	}
	if err := holdClaim(ctx, f.pool, id, "some-other-replica"); !isNotOwner(err) {
		t.Fatalf("a step of a pass that does not own the workflow held a claim: %v", err)
	}
}

// takenOverDialer takes the workflow over the first time a pass reaches a
// shard, which is how a pass loses its claim: part-way through, with work
// already done and a status write still to come.
type takenOverDialer struct {
	ShardDBDialer
	pool *pgxpool.Pool
	id   string
	once sync.Once
}

func (d *takenOverDialer) DialDatabase(ctx context.Context, set string, id int32, db string) (ShardConn, error) {
	var moved bool
	d.once.Do(func() {
		_, _ = d.pool.Exec(ctx, `UPDATE pgshard.workflows SET state = $2 WHERE id = $1::uuid`, d.id, StatePaused)
		moved = true
	})
	if moved {
		// The step fails for its own reason, as a shard that has just gone
		// away would; the pass then has a status write left to make.
		return nil, errors.New("shard is unreachable")
	}
	return d.ShardDBDialer.DialDatabase(ctx, set, id, db)
}

// TestCopierPassSurvivesAWorkflowTakenOverMidStep: a copy pass whose
// workflow moves under it fails its step and then tries to record why. That
// write is refused, and returning the refusal from Pass would log a
// spurious failure and skip every workflow after it in the list.
func TestCopierPassSurvivesAWorkflowTakenOverMidStep(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()

	var id string
	if err := f.catalog.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status)
		VALUES (gen_random_uuid(), $1, $2, '{"database": "app"}'::jsonb, jsonb_build_object('stage', $3::text))
		RETURNING id::text`, KindReshard, StateRunning, StageCopying).Scan(&id); err != nil {
		t.Fatal(err)
	}
	c := &Copier{
		Pool:   f.pool,
		Shards: &takenOverDialer{ShardDBDialer: f.placer.Shards, pool: f.pool, id: id},
		Logger: f.placer.Logger,
	}

	out, err := c.Pass(ctx)
	if err != nil {
		t.Fatalf("a workflow that moved under the pass failed the whole pass: %v", err)
	}
	if out.Failed != 0 {
		t.Fatalf("a workflow that moved under the pass was failed: %+v", out)
	}
	var state string
	if err := f.catalog.QueryRow(ctx, `SELECT state FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != StatePaused {
		t.Fatalf("the pass wrote over the state that moved under it: %s", state)
	}
}

// TestARevokedPassDoesNotTearDownTheWorkflowItLost: fail lifts the write
// fence and drops the table lock before it records the failure. Run by a
// pass whose claim is gone, that hands the table back while the replica
// that owns the workflow now is still driving it -- unfenced, and with the
// lock free for a second workflow on the same table.
func TestARevokedPassDoesNotTearDownTheWorkflowItLost(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := startPlacement(t, f)

	locks := func() int {
		t.Helper()
		var n int
		if err := f.catalog.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if locks() != 1 {
		t.Fatal("the workflow does not hold its table lock")
	}

	wf := f.load(id)
	wf.owner, wf.fence = "a-claim-that-was-revoked", wf.state
	if err := f.placer.fail(ctx, wf, fatal("a failure raised by a pass that lost its claim")); !isNotOwner(err) {
		t.Fatalf("a revoked pass failed the workflow: %v", err)
	}
	if locks() != 1 {
		t.Fatal("a revoked pass dropped the table lock of the workflow it lost")
	}
	state, _, _, _ := ownerRow(t, f, id)
	if state == StateFailed {
		t.Fatal("a revoked pass failed a workflow another replica owns")
	}
}

// cutoverParked inserts a running reshard workflow parked mid-cutover at
// step, claimed by owner as of now (or never claimed when owner is empty).
func cutoverParked(t *testing.T, f *placementFixture, step, owner string) string {
	t.Helper()
	var id string
	var ownerArg any
	if owner != "" {
		ownerArg = owner
	}
	if err := f.catalog.QueryRow(context.Background(),
		`INSERT INTO pgshard.workflows (id, kind, state, spec, status, owner, owned_at)
		 VALUES (gen_random_uuid(), $1, $2, '{"database": "app", "shard_set": "g2"}'::jsonb,
		         jsonb_build_object('stage', $3::text, 'cutover', jsonb_build_object('step', $4::text)),
		         $5, CASE WHEN $5::text IS NULL THEN NULL ELSE now() END)
		 RETURNING id::text`, KindReshard, StateRunning, StageSwitching, step, ownerArg).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestNoCutoverStepIsDrivenByTwoControllers is parameterised over every
// step of the switch, so a step added later is covered by construction
// rather than by remembering to add a case.
//
// A controller killed between any two steps leaves the workflow claimed
// until its lease runs out. A second controller that believes it leads must
// not touch it meanwhile: the steps before the journal hold a write fence
// and table locks, and a pass that acted on a workflow it does not own
// would lift a fence the owner is still relying on.
func TestNoCutoverStepIsDrivenByTwoControllers(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	for _, step := range switchSteps {
		t.Run(step, func(t *testing.T) {
			id := cutoverParked(t, f, step, "replica-a")
			stageBefore, updatedBefore, ownerBefore := cutoverRow(t, f, id)

			other := &Copier{Pool: f.pool, Shards: f.placer.Shards, Logger: f.placer.Logger, Replica: "replica-b"}
			out, err := other.Pass(ctx)
			if err != nil {
				t.Fatalf("a pass that owns nothing must not fail: %v", err)
			}
			if out.Driven != 0 {
				t.Fatalf("replica-b drove a workflow claimed by replica-a at step %s: %+v", step, out)
			}
			stage, updated, owner := cutoverRow(t, f, id)
			if stage != stageBefore || !updated.Equal(updatedBefore) || owner != ownerBefore {
				t.Fatalf("replica-b mutated a workflow it does not own at step %s: stage %s(was %s) owner %s(was %s) updated %s(was %s)",
					step, stage, stageBefore, owner, ownerBefore, updated, updatedBefore)
			}
			mustExec(t, f.catalog, `DELETE FROM pgshard.workflows WHERE id = $1::uuid`, id)
		})
	}
}

// TestAnUnclaimedCutoverStepIsPickedUp is the control for the test above.
// Two processes that never actually contend pass whether or not the claim
// works: if Pass simply did not select a workflow parked mid-cutover, every
// case above would pass for the wrong reason. Here the same workflow, left
// unclaimed, IS claimed -- so the refusals above are the claim doing its job.
func TestAnUnclaimedCutoverStepIsPickedUp(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	id := cutoverParked(t, f, StepFence, "")

	other := &Copier{Pool: f.pool, Shards: f.placer.Shards, Logger: f.placer.Logger, Replica: "replica-b"}
	if _, err := other.Pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if _, _, owner := cutoverRow(t, f, id); owner != "replica-b" {
		t.Fatalf("an unclaimed workflow parked mid-cutover was not claimed: owner %q", owner)
	}
}

func cutoverRow(t *testing.T, f *placementFixture, id string) (stage string, updated time.Time, owner string) {
	t.Helper()
	var o *string
	if err := f.catalog.QueryRow(context.Background(),
		`SELECT status->>'stage', updated_at, owner FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&stage, &updated, &o); err != nil {
		t.Fatal(err)
	}
	if o != nil {
		owner = *o
	}
	return stage, updated, owner
}
