package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// noDial fails a test the moment anything dials a shard.
type noDial struct{ t *testing.T }

func (d noDial) DialDatabase(context.Context, string, int32, string) (ShardConn, error) {
	d.t.Error("a shard was dialled for a migration that must be held")
	return nil, errors.New("no dial")
}

func (d noDial) DialDatabaseAs(context.Context, string, int32, string, string, string) (ShardConn, error) {
	d.t.Error("a shard was dialled for a migration that must be held")
	return nil, errors.New("no dial")
}

// TestAMigrationDoesNotStartWhileAReshardCopies (PGS-872): a reshard copies
// by logical replication, which carries no DDL, and the applier held a
// migration only from the cutover's fence, so a column added during the copy
// broke the targets' apply. A migration now stays queued while a reshard or
// upgrade copies -- in the applier's pass, and in the statement that would
// start it, for a pass that read no hold -- and starts once the copy is past
// its switch.
func TestAMigrationDoesNotStartWhileAReshardCopies(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	mustExec(t, cat, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, int8range(NULL, NULL))`)
	reconcile(t, cat)
	const reshard = "00000000-0000-0000-0000-0000000008a1"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'reshard', 'running', '{"shard_set": "g2", "generation": 2}', '{"stage": "copying"}')`, reshard)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all", HomeShard: 0})
	if err != nil {
		t.Fatal(err)
	}
	state := func() string {
		return queryOne[string](t, cat, `SELECT state FROM pgshard.migrations WHERE id = $1`, id)
	}

	a := &Applier{Store: &PGMigrationStore{Pool: pool}, Shards: noDial{t}, RewriteSettle: -1}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := state(); got != catalog.MigrationQueued {
		t.Fatalf("a migration during a reshard copy is %s, want it held queued", got)
	}

	// A pass that read no hold -- it looked before the copy was recorded --
	// still cannot start it, and leaves it queued rather than failing.
	blind := &Applier{Store: lockBlindStore{&PGMigrationStore{Pool: pool}}, Shards: noDial{t}, RewriteSettle: -1}
	if _, err := blind.RunOnce(ctx); err != nil {
		t.Fatalf("a pass that read no hold: %v, want the migration left queued", err)
	}
	if got := state(); got != catalog.MigrationQueued {
		t.Fatalf("a pass that read no hold left the migration %s", got)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	m.State = catalog.MigrationRunning
	if err := catalog.SaveMigrationProgress(ctx, pool, m, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("starting a migration while a reshard copies: %v, want it held", err)
	}
	if got := state(); got != catalog.MigrationQueued {
		t.Fatalf("the refused start left the migration %s", got)
	}

	for _, stage := range []string{StageCatchUpDone, StageAwaitingSwitch, StageSwitching} {
		mustExec(t, cat, `UPDATE pgshard.workflows SET status = jsonb_build_object('stage', $2::text) WHERE id = $1`, reshard, stage)
		if err := catalog.SaveMigrationProgress(ctx, pool, m, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
			t.Fatalf("starting a migration while the reshard is at %s: %v, want it held", stage, err)
		}
	}
	mustExec(t, cat, `UPDATE pgshard.workflows SET state = 'paused', status = '{"stage": "copying", "paused_from": "running"}' WHERE id = $1`, reshard)
	if err := catalog.SaveMigrationProgress(ctx, pool, m, 0); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("starting a migration while the reshard is paused mid-copy: %v, want it held (its subscriptions still apply)", err)
	}

	mustExec(t, cat, `UPDATE pgshard.workflows SET state = 'running', status = '{"stage": "switched"}' WHERE id = $1`, reshard)
	if err := catalog.SaveMigrationProgress(ctx, pool, m, 0); err != nil {
		t.Fatalf("starting a migration once the reshard has switched: %v", err)
	}
	if got := state(); got != catalog.MigrationRunning {
		t.Fatalf("the migration is %s after the reshard switched", got)
	}
}

// TestACopyWaitsForAMigrationThatWasStartingAsItBegan (PGS-872): the
// applier's start and the copy's start are separate transactions. A start
// that had not committed when the copy looked must still be seen, or the
// copy materializes a schema the migration then changes.
func TestACopyWaitsForAMigrationThatWasStartingAsItBegan(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all", HomeShard: 0})
	if err != nil {
		t.Fatal(err)
	}
	starting := connect(t, dsn)
	tx, err := starting.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE pgshard.migrations SET state = 'running' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	type result struct {
		dbs []string
		err error
	}
	done := make(chan result, 1)
	go func() {
		dbs, err := migrationsApplying(ctx, pool)
		done <- result{dbs, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the copy's look did not wait for a start in flight: %v %v", r.dbs, r.err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || !slices.Equal(r.dbs, []string{"app"}) {
		t.Fatalf("after the start committed the copy saw %v %v, want the migration applying on app", r.dbs, r.err)
	}
}

// TestACopyDoesNotMaterializeUnderAnApplyingMigration (PGS-872): a
// migration still applying when a reshard begins its copy would change the
// schema the copy materializes on the targets. The copy records itself and
// waits for it before creating anything.
func TestACopyDoesNotMaterializeUnderAnApplyingMigration(t *testing.T) {
	parallelPG(t)
	f := newCopyFixture(t)
	ctx := context.Background()
	wfID := f.startWorkflow()
	id, err := catalog.EnqueueMigration(ctx, f.pool, catalog.DDLMigration{Database: "app", Statement: "CREATE INDEX orders_extra ON orders (note)",
		Kind: "CREATE INDEX", Strategy: "direct", Scope: "all", HomeShard: 0})
	if err != nil {
		t.Fatal(err)
	}
	mustExecPool(t, f.pool, `UPDATE pgshard.migrations SET state = 'running' WHERE id = '`+id+`'`)

	f.pass()
	_, stage, msg := f.workflow(wfID)
	if stage != StageCopying || !strings.Contains(msg, "a migration is still applying on app") {
		t.Fatalf("with a migration applying the copy is at %s (%q), want it recorded and waiting", stage, msg)
	}
	for id := range 2 {
		src := connect(t, f.appDSN("default", int32(id)))
		if n := queryOne[int64](t, src, `SELECT count(*) FROM pg_publication`); n != 0 {
			t.Fatalf("source %d has %d publications while a migration applies", id, n)
		}
	}

	mustExecPool(t, f.pool, `UPDATE pgshard.migrations SET state = 'complete' WHERE id = '`+id+`'`)
	f.pass()
	if _, _, msg := f.workflow(wfID); strings.Contains(msg, "a migration is still applying") {
		t.Fatalf("the copy still waits after the migration completed: %q", msg)
	}
	src0 := connect(t, f.appDSN("default", 0))
	if n := queryOne[int64](t, src0, `SELECT count(*) FROM pg_publication`); n == 0 {
		t.Fatal("the copy created no publications once the migration completed")
	}
}

func holdCatalog(t *testing.T) (context.Context, *pgxpool.Pool, func(sql string, args ...any)) {
	t.Helper()
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	cat := connect(t, dsn)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	return ctx, pool, func(sql string, args ...any) { mustExec(t, cat, sql, args...) }
}

// TestAHeldRoleMigrationDoesNotStopTheRoleVerifier (PGS-872): a reshard's
// new shards get the managed roles from the role verifier, before their
// schema copy restores objects owned by those roles. The verifier waits
// while a role or grant migration is pending, and the copy holds queued
// migrations, so a role migration queued as a reshard began held the roles
// the copy needed, and the copy held the migration. A held migration has
// touched no group and no longer counts; one that is running, or queued and
// free to start, still does.
func TestAHeldRoleMigrationDoesNotStopTheRoleVerifier(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	const reshard = "00000000-0000-0000-0000-0000000008a4"
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ($1, 'reshard', 'running', '{"shard_set": "g2", "generation": 2}', '{"stage": "copying"}')`, reshard)
	store := &PGRoleStore{Pool: pool}
	pending := func() bool {
		t.Helper()
		got, err := store.RoleMigrationsPending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, kind := range []string{"CREATE ROLE", "GRANT"} {
		id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: kind + " ...", Kind: kind, Strategy: "direct", Scope: "all"})
		if err != nil {
			t.Fatal(err)
		}
		if pending() {
			t.Fatalf("a queued %s held by a reshard copy stops the role verifier, which the copy needs to restore its schema", kind)
		}
		exec(`UPDATE pgshard.migrations SET state = 'running' WHERE id = $1`, id)
		if !pending() {
			t.Fatalf("a running %s does not stop the role verifier", kind)
		}
		exec(`UPDATE pgshard.migrations SET state = 'complete' WHERE id = $1`, id)
	}
	if _, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "DROP ROLE r", Kind: "DROP ROLE", Strategy: "direct", Scope: "all"}); err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE pgshard.workflows SET status = '{"stage": "switched"}' WHERE id = $1`, reshard)
	if !pending() {
		t.Fatal("a queued role migration free to start does not stop the role verifier")
	}
}

// TestAMigrationStartWaitsForACopyThatIsStarting (PGS-872): the start's
// hold check and the copy's start are separate transactions. A start that
// read its snapshot before the copy committed its stage would begin unseen,
// so the start takes the move gate the copy holds while it begins.
func TestAMigrationStartWaitsForACopyThatIsStarting(t *testing.T) {
	ctx, pool, _ := holdCatalog(t)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	m.State = catalog.MigrationRunning

	copier, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = copier.Rollback(ctx) }()
	if err := lockMoveGate(ctx, copier); err != nil {
		t.Fatal(err)
	}
	if _, err := copier.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ('00000000-0000-0000-0000-0000000008a5', 'reshard', 'running', '{"shard_set": "g2", "generation": 2}', '{"stage": "copying"}')`); err != nil {
		t.Fatal(err)
	}

	saved := make(chan error, 1)
	go func() { saved <- (&PGMigrationStore{Pool: pool}).Save(ctx, m, 0) }()
	waitFor(t, time.Minute, func() bool {
		select {
		case err := <-saved:
			t.Fatalf("the start returned (%v) while a copy was beginning under the move gate", err)
		default:
		}
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting
	}, "the start never waited on the move gate")
	if err := copier.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-saved; !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("a start that waited for a copy to begin: %v, want it held", err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM pgshard.migrations WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != catalog.MigrationQueued {
		t.Fatalf("the migration is %s, want it left queued", state)
	}
}

// TestAHeldStartFromAStaleLeaderSaysLeadershipPassed: a start refused both
// for a hold and for a term that has passed reports the term, so the pass
// stops rather than carrying on as the leader.
func TestAHeldStartFromAStaleLeaderSaysLeadershipPassed(t *testing.T) {
	ctx, pool, exec := holdCatalog(t)
	exec(`INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES ('00000000-0000-0000-0000-0000000008a6', 'reshard', 'running', '{"shard_set": "g2", "generation": 2}', '{"stage": "copying"}')`)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "ALTER TABLE orders ADD COLUMN extra int",
		Kind: "ALTER TABLE", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := catalog.LoadMigration(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	m.State = catalog.MigrationRunning
	stale, err := catalog.TakeLeaderTerm(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.SaveMigrationProgress(ctx, pool, m, stale); !errors.Is(err, catalog.ErrMigrationHeld) {
		t.Fatalf("a held start from the current leader: %v, want it held", err)
	}
	if _, err := catalog.TakeLeaderTerm(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SaveMigrationProgress(ctx, pool, m, stale); !errors.Is(err, catalog.ErrNotLeaderTerm) {
		t.Fatalf("a held start from a leader whose term has passed: %v, want ErrNotLeaderTerm", err)
	}
}

// lockBlindStore reads no DDL locks, as a pass that looked just before a
// copy was recorded.
type lockBlindStore struct{ *PGMigrationStore }

func (lockBlindStore) LockedDatabases(context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}

// TestTheCatalogHoldNamesTheStagesTheApplierHolds: the start predicate in
// the catalog package spells the stages out; a stage renamed or added here
// and not there would let a migration start mid-copy.
func TestTheCatalogHoldNamesTheStagesTheApplierHolds(t *testing.T) {
	predicate := catalog.MigrationHeldPredicate
	named := func(word string) bool { return strings.Contains(predicate, "'"+word+"'") }
	for _, stage := range ddlHoldStages {
		if !named(stage) {
			t.Errorf("catalog.MigrationHeldPredicate does not hold stage %q", stage)
		}
	}
	for _, kind := range copyKinds {
		if !named(kind) {
			t.Errorf("catalog.MigrationHeldPredicate does not hold workflow kind %q", kind)
		}
	}
	for _, state := range []string{StateRunning, StatePaused} {
		if !named(state) {
			t.Errorf("catalog.MigrationHeldPredicate does not hold workflow state %q", state)
		}
	}
}
