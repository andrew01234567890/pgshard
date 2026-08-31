package controller

import (
	"cmp"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
	if err == nil || !isSourceRetired(err) {
		t.Fatalf("a source another workflow retired can never come back, so the switch is over, not waiting: %v", err)
	}
	if st := queryOne[string](t, conn, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'g2'`); st != catalog.ShardSetProvisioning {
		t.Fatalf("a refused flip must publish nothing: g2 is %s", st)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_sets WHERE state = 'serving'`); n != 1 {
		t.Fatalf("%d serving sets after a refused flip", n)
	}

	// Two serving sets with the frozen source among them is a state this
	// switch did not cause and may still resolve, so it stays a plain
	// error the pass retries rather than one that ends the workflow.
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'serving' WHERE shard_set = 'default'`)
	err = o.Flip(ctx, "")
	if err == nil || isSourceRetired(err) || !strings.Contains(err.Error(), "no longer the only serving") {
		t.Fatalf("a source still serving beside another set is not retired: %v", err)
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

// racingCutovers builds two cutovers of the same source, on their own
// target sets, over one catalog. Their fixture is the state the flip acts
// on: a serving source of two shards, two provisioned targets and a
// database whose home shard the flip moves.
func racingCutovers(t *testing.T) (*pgxpool.Pool, *pgx.Conn, *pgCutover, *pgCutover) {
	t.Helper()
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
	}{{"default", 1, catalog.ShardSetServing}, {"g2", 2, catalog.ShardSetProvisioning}, {"g3", 3, catalog.ShardSetProvisioning}} {
		if err := catalog.MaterializeShardSet(ctx, tx, s.name, s.gen, s.state, rs, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mk := func(set string, gen int64) *pgCutover {
		return &pgCutover{
			c:         &Copier{Pool: pool},
			wf:        &copyWorkflow{id: newWorkflowID(t, conn), set: set, gen: gen, ids: []int32{0, 1}, ranges: rs},
			srcSet:    "default",
			srcIDs:    []int32{0, 1},
			srcRanges: rs,
		}
	}
	return pool, conn, mk("g2", 2), mk("g3", 3)
}

// serving names the shard sets the catalog is serving.
func serving(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT shard_set FROM pgshard.shard_sets WHERE state = $1 ORDER BY shard_set`, catalog.ShardSetServing)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// both runs two catalog operations so that they really overlap, and
// returns their errors in order.
//
// Starting two goroutines proves nothing on its own: the second may not
// begin until the first has committed, and then no two transactions ever
// held a view of the catalog at once. So the test holds the row both of
// them must write -- the source shard set -- until both have started,
// passed their own checks and blocked on it. Releasing it then decides
// the race inside PostgreSQL, which is where it happens in production.
func both(t *testing.T, conn *pgx.Conn, first, second func() error) (error, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The map generation is the last row both a flip and a rollback write,
	// so a lock on it holds each of them after it has read the catalog and
	// decided to publish -- which is the interleaving that matters.
	if _, err := tx.Exec(ctx, `SELECT generation FROM pgshard.shard_map_generation FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	gate := make(chan struct{})
	errs := make([]error, 2)
	for i, fn := range []func() error{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			errs[i] = fn()
		}()
	}
	close(gate)

	// Both are in flight once both are waiting on the held row.
	deadline := time.Now().Add(30 * time.Second)
	for {
		n := queryOne[int64](t, conn, `SELECT count(*) FROM pg_locks WHERE NOT granted`)
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 2 operations reached the contended row; they did not overlap", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	return errs[0], errs[1]
}

// TestTwoFlipsOfOneSourceCannotBothPublishOnPostgres: each flip checks that
// its source is still the only serving set, but the check and the publish
// are two statements. Two cutovers of the same source that overlap could
// each see a sole serving source and each publish on top of it, which is
// the state the check exists to prevent: two serving shard sets, and every
// write committed to one of them invisible through the other.
func TestTwoFlipsOfOneSourceCannotBothPublishOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	_, conn, first, second := racingCutovers(t)

	e1, e2 := both(t, conn, func() error { return first.Flip(ctx, "") }, func() error { return second.Flip(ctx, "") })
	if (e1 == nil) == (e2 == nil) {
		t.Fatalf("exactly one flip must publish; got %v and %v", e1, e2)
	}
	// The loser is stopped by the isolation level, not by the sole-serving
	// check: both read the catalog before either wrote, so both saw one
	// serving source. Repeatable read is what turns the second write into a
	// failure, and this asserts that rather than any error at all.
	if lost := cmp.Or(e1, e2); !strings.Contains(lost.Error(), "40001") {
		t.Fatalf("the losing flip failed with %v, want a serialization failure", lost)
	}
	if got := serving(t, conn); len(got) != 1 {
		t.Fatalf("serving sets after two racing flips: %v", got)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.serving`); n != 1 {
		t.Fatalf("%d rows in pgshard.serving after two racing flips", n)
	}
}

// TestAFlipRacingARollbackLeavesOneServingSetOnPostgres: a rollback puts
// its source back and retires its target, so it publishes a serving set
// exactly as a flip does. The two race only where both are still possible:
// once the first workflow has switched to g2, a second reshard out of g2 is
// a live flip, and the first workflow rolling back to default is a live
// rollback -- of the same serving set.
func TestAFlipRacingARollbackLeavesOneServingSetOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	pool, conn, first, _ := racingCutovers(t)

	if err := first.Flip(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got := serving(t, conn); len(got) != 1 || got[0] != "g2" {
		t.Fatalf("after the first flip: %v", got)
	}

	// A reshard out of the set that now serves.
	rs, _ := placement.Split(2)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g4", 4, catalog.ShardSetProvisioning, rs, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	next := &pgCutover{
		c:         &Copier{Pool: pool},
		wf:        &copyWorkflow{id: newWorkflowID(t, conn), set: "g4", gen: 4, ids: []int32{0, 1}, ranges: rs},
		srcSet:    "g2",
		srcIDs:    []int32{0, 1},
		srcRanges: rs,
	}

	e1, e2 := both(t, conn, func() error { return first.flipBack(ctx) }, func() error { return next.Flip(ctx, "") })
	if e1 != nil && e2 != nil {
		t.Fatalf("one of the two must succeed; got %v and %v", e1, e2)
	}
	got := serving(t, conn)
	if len(got) != 1 {
		t.Fatalf("serving sets after a rollback raced a flip: %v (rollback %v, flip %v)", got, e1, e2)
	}
	// Whichever won published its own serving row. A row for the set it
	// replaced is expected to remain: it is deleted when the workflow
	// completes, because until then the old set is still reachable and a
	// rollback may need it back.
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.serving WHERE shard_set = $1`, got[0]); n != 1 {
		t.Fatalf("the serving set %s has no published generation", got[0])
	}
}

// atOnce starts two operations together. Unlike the flips, a fence is
// decided by one conditional UPDATE, so whichever order PostgreSQL runs
// them in is a real order -- there is no window between a check and a
// write to force them into.
func atOnce(first, second func() error) (error, error) {
	var wg sync.WaitGroup
	gate := make(chan struct{})
	errs := make([]error, 2)
	for i, fn := range []func() error{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			errs[i] = fn()
		}()
	}
	close(gate)
	wg.Wait()
	return errs[0], errs[1]
}

// TestTwoCutoversFencingOneSourceAtOnceOnPostgres: the fence is owned, and
// the owner is decided by a write. Two cutovers fencing the same source at
// the same moment must not both come away believing they hold it.
func TestTwoCutoversFencingOneSourceAtOnceOnPostgres(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	_, conn, first, second := racingCutovers(t)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
		VALUES ('default', 0, 'shard0', 'serving', 1), ('default', 1, 'shard1', 'serving', 1)`)

	e1, e2 := atOnce(func() error { return first.Fence(ctx) }, func() error { return second.Fence(ctx) })
	if (e1 == nil) == (e2 == nil) {
		t.Fatalf("exactly one cutover may hold the fence; got %v and %v", e1, e2)
	}
	winner, loser := first, second
	if e1 != nil {
		winner, loser = second, first
	}
	// The loser giving up must not open writes the winner is holding.
	if err := loser.Release(ctx); err != nil {
		t.Fatalf("the loser's release: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'default' AND migrating`); n != 2 {
		t.Fatalf("the winner's fence was lifted by the cutover that lost the race: %d shards still fenced", n)
	}
	if err := winner.Release(ctx); err != nil {
		t.Fatalf("the winner's release: %v", err)
	}
	if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'default' AND migrating`); n != 0 {
		t.Fatalf("%d shards still fenced after the owner released", n)
	}
}
