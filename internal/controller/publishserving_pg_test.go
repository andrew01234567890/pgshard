package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestPublishesServingWhenStatusRowsExistAlready guards the case where the
// operator records a group's primary before the controller's first pass. The
// map used to be published only by the pass that created the shard_status
// rows, so losing that race left pgshard.serving empty for good: every key
// failed to route, and the reconciler kept proposing a reshard for a set whose
// map had simply never been published.
func TestPublishesServingWhenStatusRowsExistAlready(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET generation = 1, state = 'serving' WHERE shard_set = 'default'`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
		('default', 0, '[,0)'), ('default', 1, '[0,)')`)
	// The operator got there first.
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint) VALUES
		('default', 0, 'shard0', 'serving', 1, 'shard0-0:5432'),
		('default', 1, 'shard1', 'serving', 1, 'shard1-0:5432')`)

	res := reconcile(t, conn)
	if len(res.Invalid) != 0 {
		t.Fatalf("reconcile reported %v", res.Invalid)
	}

	var generation int64
	if err := conn.QueryRow(ctx, `SELECT generation FROM pgshard.serving WHERE shard_set = 'default'`).Scan(&generation); err != nil {
		t.Fatalf("the shard map was never published, so nothing can route: %v", err)
	}
	if generation == 0 {
		t.Fatalf("published generation %d, want the desired generation", generation)
	}

	var workflows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pgshard.workflows`).Scan(&workflows); err != nil {
		t.Fatal(err)
	}
	if workflows != 0 {
		t.Fatalf("publishing a set's first map must not look like a reshard; created %d workflow(s)", workflows)
	}
}
