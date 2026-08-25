package controller

import (
	"context"
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
		if err := catalog.MaterializeShardSet(ctx, tx, s.name, s.gen, s.state, rs); err != nil {
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
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, rs); err != nil {
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
