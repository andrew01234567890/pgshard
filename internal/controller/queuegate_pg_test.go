package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAMigrationWaitsUntilTheReshardCompletes (PGS-899): DDL issued during a
// reshard waits for the whole run, not only its copy. After the switch the
// retired set still applies the targets' writes in reverse, and a rollback
// returns serving to it; a column added on the serving set breaks that apply
// and leaves the rollback refused.
func TestAMigrationWaitsUntilTheReshardCompletes(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, NULL))`)
	reconcile(t, connect(t, pool.Config().ConnString()))
	const reshard = "00000000-0000-0000-0000-0000000008b1"
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'reshard', 'running', '{"shard_set": "g2", "generation": 2, "source_set": "default"}', '{"stage": "copying"}')`, reshard)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	store := &PGMigrationStore{Pool: pool}
	a := &Applier{Store: store, Shards: noDial{t}, RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != catalog.MigrationQueued {
		t.Fatalf("a migration during a reshard copy is %s, want queued", m.State)
	}
	m.State, m.Meta.ShardSet = catalog.MigrationRunning, "default"
	for _, stage := range []string{StageCatchUpDone, StageAwaitingSwitch, StageSwitching, StageSwitched, StageCompleting, StageRollingBack} {
		exec(`UPDATE pgshard.workflows SET status = jsonb_build_object('stage', $2::text) WHERE id = $1`, reshard, stage)
		if err := store.Save(ctx, m, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
			t.Fatalf("starting a migration while the reshard is at %s: %v, want it held", stage, err)
		}
	}
	exec(`UPDATE pgshard.workflows SET state = 'completed', status = '{"stage": "completed"}' WHERE id = $1`, reshard)
	if err := store.Save(ctx, m, 0); err != nil {
		t.Fatalf("starting a migration once the reshard completed: %v", err)
	}
	if !queryOne[bool](t, connect(t, pool.Config().ConnString()), `SELECT state = 'running' AND started_at IS NOT NULL FROM pgshard.migrations WHERE id = $1`, id) {
		t.Fatal("the started migration is not running with its start stamped")
	}
}

// TestAPlacementHoldsDDLOnItsDatabaseOnly (PGS-899): DDL on a table a
// placement moves failed the move or was lost at its swap. A placement now
// holds DDL on its database until it has swapped, and nothing elsewhere.
func TestAPlacementHoldsDDLOnItsDatabaseOnly(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.databases (name) VALUES ('other')`)
	const move = "00000000-0000-0000-0000-0000000008b2"
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'table_placement', 'running', '{"database": "app", "schema_name": "public", "table_name": "orders"}', '{"stage": "copying"}')`, move)
	store := &PGMigrationStore{Pool: pool}
	start := func(database string) error {
		t.Helper()
		id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: database, Statement: "CREATE INDEX i ON t (c)", Kind: "CREATE INDEX", Strategy: "direct", Scope: "all"})
		if err != nil {
			t.Fatal(err)
		}
		m, err := catalog.LoadMigration(ctx, pool, id)
		if err != nil {
			t.Fatal(err)
		}
		m.State, m.Meta.ShardSet = catalog.MigrationRunning, "default"
		return store.Save(ctx, m, 0)
	}
	if err := start("app"); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("DDL on the moving table's database: %v, want it held", err)
	}
	if err := start("other"); err != nil {
		t.Fatalf("DDL on another database waited for the placement: %v", err)
	}
	exec(`UPDATE pgshard.workflows SET status = '{"stage": "retiring"}' WHERE id = $1`, move)
	exec(`DELETE FROM pgshard.migrations`)
	if err := start("app"); err != nil {
		t.Fatalf("DDL after the placement swapped: %v", err)
	}
}

// TestAHomeMigrationHeldAcrossACutoverRunsOnTheNewHomeShard (PGS-899): the
// router records the home shard when it queues, and a cutover gives the
// database a home shard in the new set. A migration held through the
// reshard started on the old id -- another database in the new set.
func TestAHomeMigrationHeldAcrossACutoverRunsOnTheNewHomeShard(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, 0)), ('default', 1, int8range(0, NULL))`)
	cat := connect(t, pool.Config().ConnString())
	reconcile(t, cat)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "GRANT SELECT ON items TO app",
		Kind: "GRANT", Strategy: "direct", Scope: "home", HomeShard: 0})
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE pgshard.databases SET home_shard = 1 WHERE name = 'app'`)
	store := &PGMigrationStore{Pool: pool}

	stale, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	stale.State, stale.Meta.ShardSet, stale.PerShard = catalog.MigrationRunning, "default", map[string]catalog.ShardMigration{"0": {State: catalog.ShardPending}}
	if err := store.Save(ctx, stale, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("a start on the home shard the database no longer has: %v, want it refused", err)
	}

	elsewhere := stale
	elsewhere.Meta.ShardSet, elsewhere.HomeShard, elsewhere.PerShard = "g2", 1, map[string]catalog.ShardMigration{"1": {State: catalog.ShardPending}}
	if err := store.Save(ctx, elsewhere, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("a start planned against g2 while default serves: %v, want it refused", err)
	}

	shards := newFakeShards()
	a := &Applier{Store: store, Shards: shards, RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != catalog.MigrationComplete || m.HomeShard != 1 || len(m.PerShard) != 1 || m.PerShard["1"].State != catalog.ShardApplied {
		t.Fatalf("migration %s on home shard %d with %v, want it applied on shard 1 only", m.State, m.HomeShard, m.PerShard)
	}
	if len(shards.statements(0)) != 0 {
		t.Fatalf("shard 0 ran %v", shards.statements(0))
	}
}

// TestASchemaChangeFinishingDuringDescribeRefusesThePlacementStart
// (PGS-899): a placement captures the table's owner, grants, comment and
// columns before it starts, and restores them at the swap. A migration on
// the table's database finishing between that capture and the start would be
// undone by the restore, so the start waits and the next pass describes
// again.
func TestASchemaChangeFinishingDuringDescribeRefusesThePlacementStart(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	const move = "00000000-0000-0000-0000-0000000008b3"
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'table_placement', 'pending', '{"database": "app", "schema_name": "public", "table_name": "orders"}', '{"stage": "preparing"}')`, move)
	var describedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&describedAt); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, finished_at) VALUES (gen_random_uuid(), 'app', 'REVOKE ...', 'REVOKE', 'direct', 'all', 'complete', clock_timestamp())`)
	p := &Placer{Pool: pool}
	wf := &placementWorkflow{id: move, state: StatePending, stage: StagePlacementPreparing,
		spec: placementSpec{Database: "app", SchemaName: "public", TableName: "orders"}}
	if err := p.start(ctx, wf, true, describedAt); !isWaitingInQueue(err) {
		t.Fatalf("a start after a REVOKE finished during describe: %v, want it to wait", err)
	}
	if state := queryOne[string](t, connect(t, pool.Config().ConnString()), `SELECT state FROM pgshard.workflows WHERE id = $1`, move); state != StatePending {
		t.Fatalf("the placement is %s", state)
	}
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&describedAt); err != nil {
		t.Fatal(err)
	}
	if err := p.start(ctx, wf, true, describedAt); err != nil {
		t.Fatalf("a start described after the change: %v", err)
	}
}

// TestTheApplierRecordsItIsAlive (PGS-899): a router waiting on a queued
// migration tells a long queue from no controller by the heartbeat.
func TestTheApplierRecordsItIsAlive(t *testing.T) {
	ctx, pool, _ := holdCatalog(t)
	a := &Applier{Store: &PGMigrationStore{Pool: pool}, Shards: noDial{t}, RewriteSettle: -1, Term: func() int64 { return 0 }}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.beat(runCtx, a.Store.(heartbeater), func() bool { return true })
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, found, err := catalog.ControllerHeartbeatAge(ctx, pool, catalog.HeartbeatApplier); err != nil {
			t.Fatal(err)
		} else if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the applier recorded no heartbeat")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	<-done
}

// TestACopyWaitsForAMigrationQueuedBeforeIt (PGS-899): a reshard began its
// copy whenever no migration was running, so DDL queued before it waited for
// the whole reshard. The copy now waits its turn.
func TestACopyWaitsForAMigrationQueuedBeforeIt(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	ctx := context.Background()
	wfID := f.startWorkflow()
	id, err := catalog.EnqueueMigration(ctx, f.pool, catalog.DDLMigration{Database: "app", Statement: "CREATE INDEX orders_extra ON orders (note)",
		Kind: "CREATE INDEX", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.migrations SET arrival = 0 WHERE id = $1`, id)
	f.pass()
	if _, stage, msg := f.workflow(wfID); stage != StageReadyForCopy || !strings.Contains(msg, "migration "+id+", queued before it") {
		t.Fatalf("the copy is at %s (%q), want it waiting for the migration queued before it", stage, msg)
	}
	mustExec(t, f.catalog, `UPDATE pgshard.migrations SET state = 'complete', finished_at = now() WHERE id = $1`, id)
	f.pass()
	if _, stage, msg := f.workflow(wfID); stage == StageReadyForCopy {
		t.Fatalf("the copy still waits (%q) after the migration completed", msg)
	}
}

// TestAPlacementThatCannotStartGivesUpItsPlace (PGS-899): a placement whose
// own start keeps failing -- here, its table locked by another workflow --
// records why, so the queue does not hold everything behind it; once it
// starts the record is cleared.
func TestAPlacementThatCannotStartGivesUpItsPlace(t *testing.T) {
	parallelPG(t)
	g := newGateFixture(t)
	ctx := context.Background()
	mustExec(t, g.catalog, `INSERT INTO pgshard.workflow_locks (kind, key, workflow_id) VALUES ($1, 'app.public.items', gen_random_uuid())`, LockKindTable)
	_, _ = g.placer.Pass(ctx)
	startError := func() string {
		return queryOne[string](t, g.catalog, `SELECT coalesce(status->>'start_error', '') FROM pgshard.workflows WHERE id = $1::uuid`, g.placement)
	}
	if got := startError(); !strings.Contains(got, "is locked by workflow") {
		t.Fatalf("start_error = %q, want the lock conflict", got)
	}
	mustExec(t, g.catalog, `DELETE FROM pgshard.workflow_locks WHERE kind = $1`, LockKindTable)
	_, _ = g.placer.Pass(ctx)
	if got := startError(); got != "" {
		t.Fatalf("start_error = %q after the placement started", got)
	}
	if state, _, msg := g.state(g.placement); state != StateRunning {
		t.Fatalf("the placement is %s (%q), want it started", state, msg)
	}
}

// TestAHomeMigrationForADroppedDatabaseFailsWithoutStoppingTheApplier
// (PGS-899): a home-scope migration queued in a database that is then
// dropped has no home shard to start on. It fails, rather than erroring the
// pass and leaving every later migration undriven.
func TestAHomeMigrationForADroppedDatabaseFailsWithoutStoppingTheApplier(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, NULL))`)
	reconcile(t, connect(t, pool.Config().ConnString()))
	exec(`INSERT INTO pgshard.databases (name) VALUES ('gone')`)
	orphan, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "gone", Statement: "GRANT SELECT ON t TO app", Kind: "GRANT", Strategy: "direct", Scope: "home"})
	if err != nil {
		t.Fatal(err)
	}
	exec(`DELETE FROM pgshard.databases WHERE name = 'gone'`)
	next, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "CREATE TABLE notes (id int)", Kind: "CREATE TABLE", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	a := &Applier{Store: &PGMigrationStore{Pool: pool}, Shards: newFakeShards(), RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatalf("the pass stopped at the orphaned migration: %v", err)
	}
	for id, want := range map[string]string{orphan: catalog.MigrationFailed, next: catalog.MigrationComplete} {
		m, err := catalog.LoadMigration(ctx, pool, id)
		if err != nil || m.State != want {
			t.Fatalf("migration %s is %s (%v), want %s", m.Statement, m.State, err, want)
		}
	}
}
