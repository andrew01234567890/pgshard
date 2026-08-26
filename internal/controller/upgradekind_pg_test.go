package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestUpgradeSetIsNeverTakenForAReshard guards the classification a major
// upgrade depends on. The controller decides whether a pending set is an
// upgrade or a plain reshard from its recorded major, the first time it sees
// the set, and never revisits it. A set that appeared without its major was
// taken for a reshard, and an upgrade that lost that race ran with none of its
// preconditions.
func TestUpgradeSetIsNeverTakenForAReshard(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	rs, err := placement.Split(1)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, rs, 18); err != nil {
		t.Fatal(err)
	}
	// The upgrade target, written the way the operator writes it: the major is
	// part of the same statement, not a follow-up call the reconciler can run
	// in between.
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetDesired, rs, 19); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
		VALUES ('default', 0, 'shard0', 'serving', 1, 'shard0-0:5432')`)

	res := reconcile(t, conn)
	if len(res.Invalid) != 0 {
		t.Fatalf("reconcile reported %v", res.Invalid)
	}

	var kind, major string
	if err := conn.QueryRow(ctx, `SELECT kind, coalesce(spec->>'pg_major', '') FROM pgshard.workflows WHERE spec->>'shard_set' = 'g2'`).
		Scan(&kind, &major); err != nil {
		t.Fatalf("no workflow was created for the upgrade target: %v", err)
	}
	if kind != KindUpgrade {
		t.Errorf("workflow kind = %q, want %q: a set whose major differs from the serving set is an upgrade", kind, KindUpgrade)
	}
	if major != "19" {
		t.Errorf("workflow spec pg_major = %q, want 19", major)
	}
}

// TestMaterializeShardSetRecordsTheMajor is the narrower half: the major has to
// land in the same statement as the set, or the window this closes reopens.
func TestMaterializeShardSetRecordsTheMajor(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	rs, err := placement.Split(1)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g5", 5, catalog.ShardSetDesired, rs, 19); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := queryOne[int64](t, conn, `SELECT coalesce(pg_major, 0) FROM pgshard.shard_sets WHERE shard_set = 'g5'`); got != 19 {
		t.Fatalf("pg_major = %d, want 19 recorded with the set itself", got)
	}
}
