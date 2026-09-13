package controller

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// PGS-789, end to end against a real catalog and a real shard: an operator
// deleting the workflow row of a paused cutover leaves the sources refusing
// every writing transaction with 25006, and nothing is left to give it back.
//
// The shard runs in its own PostgreSQL, not the catalog's: the pause is
// ALTER SYSTEM SET default_transaction_read_only = on, and raising it on the
// catalog would stop the test writing to the catalog.
func TestDeletingAPausedWorkflowLeavesTheSourcesWritable(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	catalogDSN := startPostgres(t)
	shardDSN := startPostgres(t)

	cat := connect(t, catalogDSN)
	if err := catalog.Migrate(ctx, cat); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, cat, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
		VALUES ('default', 0, 'shard0', 'serving', 1, 'shard0:5432')`)

	const wfID = "11111111-1111-1111-1111-111111111111"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state) VALUES ($1::uuid, 'reshard', 'running')`, wfID)
	mustExec(t, cat, `UPDATE pgshard.shard_status SET write_paused_by = $1::uuid WHERE shard_set = 'default' AND shard_id = 0`, wfID)

	shard := connect(t, shardDSN)
	mustExec(t, shard, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, shard, `SELECT pg_reload_conf()`)
	waitReadOnly(t, shardDSN, true)

	sweep := &WritePauseSweep{Pool: pool, Shards: realShards{shardDSN}}

	// While the workflow exists the pause is someone's, and the sweep must
	// not touch it: a cutover in its swap step is relying on it.
	freed, err := sweep.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if freed != 0 {
		t.Fatal("the sweep lifted a pause a live workflow is relying on")
	}
	waitReadOnly(t, shardDSN, true)

	// Nor while it is merely paused: pauseBefore parks a workflow before
	// the swap step, so a paused workflow holds no write pause and a
	// workflow that holds one is not paused.
	mustExec(t, cat, `UPDATE pgshard.workflows SET state = 'paused' WHERE id = $1::uuid`, wfID)
	if freed, err := sweep.Pass(ctx); err != nil || freed != 0 {
		t.Fatalf("a paused workflow is not a finished one: freed %d, %v", freed, err)
	}

	// A workflow that ENDED without releasing is the same fact as one that
	// was deleted: the pause lives inside a single swap attempt, so a
	// terminal workflow cannot still be relying on it.
	mustExec(t, cat, `UPDATE pgshard.workflows SET state = 'failed' WHERE id = $1::uuid`, wfID)
	if freed, err := sweep.Pass(ctx); err != nil || freed != 1 {
		t.Fatalf("a failed workflow still holding a pause: freed %d, %v", freed, err)
	}
	waitReadOnly(t, shardDSN, false)

	// Put it back and delete the row outright, which is the case the
	// ticket was filed for: nothing is left to run the release at all.
	mustExec(t, shard, `ALTER SYSTEM SET default_transaction_read_only = on`)
	mustExec(t, shard, `SELECT pg_reload_conf()`)
	waitReadOnly(t, shardDSN, true)
	mustExec(t, cat, `UPDATE pgshard.shard_status SET write_paused_by = $1::uuid WHERE shard_set = 'default' AND shard_id = 0`, wfID)
	mustExec(t, cat, `DELETE FROM pgshard.workflows WHERE id = $1::uuid`, wfID)
	freed, err = sweep.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if freed != 1 {
		t.Fatalf("freed %d shards, want 1", freed)
	}
	waitReadOnly(t, shardDSN, false)

	// A writing transaction on a fresh backend is the whole point.
	writer := connect(t, shardDSN)
	if _, err := writer.Exec(ctx, `CREATE TABLE after_the_sweep (id int)`); err != nil {
		t.Fatalf("the source still refuses writes: %v", err)
	}

	var claimed *string
	if err := cat.QueryRow(ctx, `SELECT write_paused_by::text FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("the claim survived the sweep: %s", *claimed)
	}

	// And a second pass over a cluster with nothing to sweep does nothing,
	// which is what makes it safe on every tick.
	if freed, err := sweep.Pass(ctx); err != nil || freed != 0 {
		t.Fatalf("second pass freed %d, %v", freed, err)
	}
}

// waitReadOnly waits for the reload to reach a fresh backend: pg_reload_conf
// returns before the setting is in force, so reading it on the connection
// that asked for it answers the old value.
func waitReadOnly(t *testing.T, dsn string, want bool) {
	t.Helper()
	ctx := context.Background()
	var last bool
	for range 100 {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		err = conn.QueryRow(ctx, `SELECT current_setting('default_transaction_read_only') = 'on'`).Scan(&last)
		_ = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if last == want {
			return
		}
	}
	t.Fatalf("default_transaction_read_only is %v, want %v", last, want)
}
