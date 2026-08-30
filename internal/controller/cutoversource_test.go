package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestFlipRefusesAnObsoleteSourceOnPostgres: a reshard freezes the set it
// copies from when it is created, but the serving set is rediscovered every
// pass. If another workflow flips first, this one is still aiming at a
// source that has been retired: cutting over would publish a second serving
// set and drop whatever was committed to the one that flipped.
func TestFlipRefusesAnObsoleteSourceOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		name  string
		gen   int64
		state string
	}{{"default", 1, catalog.ShardSetServing}, {"g2", 2, catalog.ShardSetProvisioning}} {
		if err := catalog.MaterializeShardSet(ctx, tx, s.name, s.gen, s.state, rs, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Another workflow flipped underneath: g3 now serves and default is
	// retired, so this cutover's frozen source is stale.
	mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('g3', 3, 'serving')`)
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = 'default'`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	o := &pgCutover{
		c:      &Copier{Pool: pool},
		wf:     &copyWorkflow{set: "g2", ids: []int32{0, 1}, ranges: rs},
		srcSet: "default",
	}

	// Driving Flip itself, not the check: a regression that dropped the
	// call site would pass a test that only exercised the helper.
	err = o.Flip(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "no longer the only serving") {
		t.Fatalf("a cutover from a retired source must be refused: %v", err)
	}
	if st := queryOne[string](t, conn, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'g2'`); st != catalog.ShardSetProvisioning {
		t.Fatalf("a refused flip must publish nothing: g2 is %s", st)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_sets WHERE state = 'serving'`); n != 1 {
		t.Fatalf("%d serving sets after a refused flip", n)
	}

	// Once the frozen source is the only serving set again, it proceeds.
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = 'g3'`)
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'serving' WHERE shard_set = 'default'`)
	if err := o.Flip(ctx, ""); err != nil {
		t.Fatalf("the sole serving source must be accepted: %v", err)
	}
	if st := queryOne[string](t, conn, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'g2'`); st != catalog.ShardSetServing {
		t.Fatalf("g2 must be serving after the flip, is %s", st)
	}
}

// TestCutoverFenceIsNotAnotherWorkflowsToLiftOnPostgres: the write fence a
// cutover raises on its source was a shared boolean, so a second cutover of
// the same source joined a fence it had not raised, and either one aborting
// opened writes the other still believed were held. A write could then
// commit after the remaining workflow sampled its final LSN but before it
// flipped: neither copied nor rerouted.
func TestCutoverFenceIsNotAnotherWorkflowsToLiftOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, rs, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'shard0', 'serving', 1), ('default', 1, 'shard1', 'serving', 1)`)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	first := &pgCutover{c: &Copier{Pool: pool}, wf: &copyWorkflow{id: newWorkflowID(t, conn), set: "g2"}, srcSet: "default"}
	second := &pgCutover{c: &Copier{Pool: pool}, wf: &copyWorkflow{id: newWorkflowID(t, conn), set: "g3"}, srcSet: "default"}

	if err := first.Fence(ctx); err != nil {
		t.Fatalf("first fence: %v", err)
	}
	if err := second.Fence(ctx); err == nil || !strings.Contains(err.Error(), "holds the write fence") {
		t.Fatalf("a second cutover must not join a fence it did not raise: %v", err)
	}
	// The second aborting must not open writes the first still holds.
	if err := second.Release(ctx); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'default' AND migrating`); n != 2 {
		t.Fatalf("the first cutover's fence was lifted by the second: %d shards still fenced", n)
	}
	// Its owner may lift it, and re-fencing is idempotent for the owner.
	if err := first.Fence(ctx); err != nil {
		t.Fatalf("re-fencing by the owner: %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'default' AND migrating`); n != 0 {
		t.Fatalf("the owner could not lift its own fence: %d shards still fenced", n)
	}
}

func newWorkflowID(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	return queryOne[string](t, conn, `SELECT gen_random_uuid()::text`)
}

// TestUnwindLiftsTheFenceWithoutTheTargets guards the undo a cancelled cutover
// needs. Complete requires the targets, and a cancelled reshard is usually one
// whose targets have been deleted, so cancelling past the fence used to drop
// the forward replication and leave the source write-fenced with no workflow
// left to lift it.
func TestUnwindLiftsTheFenceWithoutTheTargets(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, rs, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'shard0', 'serving', 1), ('default', 1, 'shard1', 'serving', 1)`)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	id := newWorkflowID(t, conn)
	// No dbs and no reachable sources: Unwind must still do the catalog half,
	// which is the half that strands a cluster.
	o := &pgCutover{c: &Copier{Pool: pool}, wf: &copyWorkflow{id: id, set: "g2"}, srcSet: "default"}

	if err := o.Fence(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.workflow_locks (kind, key, workflow_id) VALUES ('table', 'app.public.ledger', $1::uuid)`, id)
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 2 {
		t.Fatalf("the fence must be raised before the undo means anything: %d", n)
	}

	if err := o.Unwind(ctx); err != nil {
		t.Fatalf("unwind: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 0 {
		t.Errorf("%d shard(s) left write-fenced after the cutover was undone", n)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.workflow_locks WHERE workflow_id = $1::uuid`, id); n != 0 {
		t.Errorf("%d lock row(s) left after the cutover was undone", n)
	}
}

// TestCutoverFenceWaitsForARunningMigrationOnPostgres: the DDL lock a
// cutover takes only holds back migrations that have not started, and one
// already running is driven to completion rather than abandoned. Fencing
// past it split that migration across both sets -- its recorded per-shard
// progress described the set it was planned against, while its remaining
// shards would be applied against the other.
func TestCutoverFenceWaitsForARunningMigrationOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, rs, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'shard0', 'serving', 1), ('default', 1, 'shard1', 'serving', 1)`)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state)
		VALUES (gen_random_uuid(), 'app', 'create table t (id int)', 'CREATE TABLE', 'direct', 'all', 'running')`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	o := &pgCutover{c: &Copier{Pool: pool}, wf: &copyWorkflow{id: newWorkflowID(t, conn), set: "g2"}, srcSet: "default"}

	err = o.Fence(ctx)
	if err == nil || !errors.Is(err, errRetry) {
		t.Fatalf("fence proceeded past a running migration: %v", err)
	}
	if !strings.Contains(err.Error(), "app") {
		t.Fatalf("the wait does not name the database: %v", err)
	}
	// Writes must not be fenced while it waits: the pause clients see should
	// not include the migration.
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 0 {
		t.Fatalf("%d shards fenced while waiting for a migration", n)
	}
	// The DDL lock must be held through the wait, or a new migration starts
	// between attempts and the cutover never gets in.
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.workflow_locks WHERE kind = 'ddl'`); n != 1 {
		t.Fatalf("ddl locks held while waiting: %d, want 1", n)
	}

	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'complete'`)
	if err := o.Fence(ctx); err != nil {
		t.Fatalf("fence after the migration finished: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); n != 2 {
		t.Fatalf("%d shards fenced after the migration finished, want 2", n)
	}
}

// TestOnlyOneWorkflowMayMoveOutOfAServingSetOnPostgres: two workflows
// moving data out of the same serving set can retire each other's source.
// The flip refuses that when it reaches it, which is late: by then both
// have provisioned groups, copied data and fenced writes. The catalog now
// refuses the second at creation, and an in-place reshard of the serving
// set -- which has no other set to retire -- is deliberately still allowed,
// because a cluster-wide rule would queue every newly declared set behind
// work the reconciler recreates on every pass.
func TestOnlyOneWorkflowMayMoveOutOfAServingSetOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	dsn := startPostgres(t)
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		name  string
		gen   int64
		state string
	}{{"default", 1, catalog.ShardSetServing}, {"g2", 2, catalog.ShardSetDesired}, {"g3", 3, catalog.ShardSetDesired}} {
		if err := catalog.MaterializeShardSet(ctx, tx, s.name, s.gen, s.state, rs, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.serving (shard_set, generation) VALUES ('default', 1)`)

	res := reconcile(t, conn)
	if res.WorkflowsCreated != 1 {
		t.Fatalf("two sets declared at once must produce one workflow, not %d: %+v", res.WorkflowsCreated, res)
	}
	if len(res.Invalid) != 1 || !strings.Contains(res.Invalid[0], "already moving data out of default") {
		t.Fatalf("the pass must say why the second set waits: %+v", res.Invalid)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.workflows WHERE kind = ANY($1)`, copyKinds); n != 1 {
		t.Fatalf("%d workflows after the pass", n)
	}

	// The catalog refuses it by any path, not only through the reconciler.
	_, err = conn.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec)
		VALUES (gen_random_uuid(), 'reshard', 'pending', '{"shard_set": "g3", "source_set": "default"}'::jsonb)`)
	if err == nil || !strings.Contains(err.Error(), "workflows_one_moving_per_source") {
		t.Fatalf("a second moving workflow inserted directly: %v", err)
	}

	// A workflow out of a different source is not in the hazard: two
	// workflows can only retire each other's source when it is the same
	// source, and the rule is that narrow on purpose.
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec)
		VALUES (gen_random_uuid(), 'reshard', 'pending', '{"shard_set": "g9", "source_set": "g8"}'::jsonb)`); err != nil {
		t.Fatalf("a move out of another source must not be blocked: %v", err)
	}

	// An in-place reshard of the serving set is not a move and is allowed
	// beside it: its source and target are the same set.
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec)
		VALUES (gen_random_uuid(), 'reshard', 'pending', '{"shard_set": "default"}'::jsonb)`); err != nil {
		t.Fatalf("an in-place reshard must not be blocked: %v", err)
	}

	// Once the first ends, the next declared set may move.
	mustExec(t, conn, `UPDATE pgshard.workflows SET state = 'completed' WHERE spec->>'source_set' IS NOT NULL`)
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec)
		VALUES (gen_random_uuid(), 'reshard', 'pending', '{"shard_set": "g3", "source_set": "default"}'::jsonb)`); err != nil {
		t.Fatalf("a moving workflow after the first ended: %v", err)
	}
}
