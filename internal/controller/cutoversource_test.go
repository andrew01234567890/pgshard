package controller

import (
	"context"
	"strings"
	"testing"

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
	o := &pgCutover{c: &Copier{Pool: pool}, srcSet: "default"}
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	err = o.sourceStillSoleServing(ctx, tx2)
	if err == nil || !strings.Contains(err.Error(), "no longer the only serving") {
		t.Fatalf("a cutover from a retired source must be refused: %v", err)
	}

	// Once it is the only serving set again, the cutover may proceed.
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = 'g3'`)
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'serving' WHERE shard_set = 'default'`)
	tx3, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx3.Rollback(ctx) }()
	if err := o.sourceStillSoleServing(ctx, tx3); err != nil {
		t.Fatalf("the sole serving source must be accepted: %v", err)
	}
}
