package controller

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestLiveShardSetsIncludesReshardTargets guards the ordering that made a
// reshard impossible for any cluster whose sharded tables carry a GRANT. Roles
// were materialized onto the serving shard set only, so a target never had
// them; the copy then materialized the source's schema onto that target, the
// schema named a role the target had never heard of, and the copy pass failed
// on every tick for the life of the reshard.
func TestLiveShardSetsIncludesReshardTargets(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET generation = 1, state = 'serving' WHERE shard_set = 'default'`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES
		('g2', 2, 'provisioning'), ('g3', 3, 'retired')`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	got, err := (&PGRoleStore{Pool: pool}).LiveShardSets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got, "g2") {
		t.Errorf("a reshard target must receive roles before its schema is materialized; got %v", got)
	}
	if !slices.Contains(got, "default") {
		t.Errorf("the serving set must still receive roles; got %v", got)
	}
	if slices.Contains(got, "g3") {
		t.Errorf("a retired set has no groups left to materialize onto; got %v", got)
	}
}
