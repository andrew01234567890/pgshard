package controller

import (
	"context"
	"testing"
	"time"

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
	mustExec(t, cat, `UPDATE pgshard.shard_status SET write_paused_by = $1::uuid, write_paused_at = now() WHERE shard_set = 'default' AND shard_id = 0`, wfID)
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
	var instant *time.Time
	if err := cat.QueryRow(ctx, `SELECT write_paused_at FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&instant); err != nil || instant != nil {
		t.Fatalf("the pause instant survived the claim: %v %v", instant, err)
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
	for i := range 100 {
		if i > 0 {
			// pg_reload_conf returns before the setting reaches a new
			// backend, so a tight loop of connects can finish before the
			// reload lands and report the old value as final.
			time.Sleep(50 * time.Millisecond)
		}
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

// The retirement pause is not a claimed pause, and the sweep must never lift
// it.
//
// Complete makes a RETIRED set read-only for good: its pods stay up for the
// retirement window and its -rw Service still answers, so a client connected
// straight to it would have writes acknowledged by a primary nothing reads
// from again and lose them at deletion, with no error anywhere. Complete is
// also the last thing every terminal path does, so a claim on that pause
// would be an orphan by the time the next sweep ran -- the sweep would hand
// the retired set back within seconds of retirement, reversing the guarantee
// Complete exists to make.
func TestOnlyAPauseSomebodyMeansToLiftIsClaimed(t *testing.T) {
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

	const wfID = "22222222-2222-2222-2222-222222222222"
	mustExec(t, cat, `INSERT INTO pgshard.workflows (id, kind, state) VALUES ($1::uuid, 'reshard', 'running')`, wfID)
	o := &pgCutover{c: &Copier{Pool: pool, Shards: realShards{shardDSN}},
		wf: &copyWorkflow{id: wfID}, srcSet: "default", srcIDs: []int32{0}}

	// The swap's pause: claimed, so the sweep can finish it.
	if err := o.pauseSetClaimed(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	if claim := pauseClaim(t, cat); claim != wfID {
		t.Fatalf("a pause that will be lifted is claimed, got %q", claim)
	}
	if err := o.pauseSetClaimed(ctx, "default", []int32{0}, false); err != nil {
		t.Fatal(err)
	}
	if claim := pauseClaim(t, cat); claim != "" {
		t.Fatalf("the claim outlived the pause: %q", claim)
	}

	// The retirement pause: unclaimed, and invisible to the sweep even with
	// no workflow row left at all.
	if err := o.pauseSet(ctx, "default", []int32{0}, true); err != nil {
		t.Fatal(err)
	}
	if claim := pauseClaim(t, cat); claim != "" {
		t.Fatalf("the retirement pause was claimed as %q; the sweep would lift it the moment the workflow finished", claim)
	}
	waitReadOnly(t, shardDSN, true)
	mustExec(t, cat, `DELETE FROM pgshard.workflows WHERE id = $1::uuid`, wfID)

	sweep := &WritePauseSweep{Pool: pool, Shards: realShards{shardDSN}}
	if freed, err := sweep.Pass(ctx); err != nil || freed != 0 {
		t.Fatalf("the sweep lifted the retirement pause: freed %d, %v", freed, err)
	}
	waitReadOnly(t, shardDSN, true)
}

func pauseClaim(t *testing.T, cat *pgx.Conn) string {
	t.Helper()
	var claim *string
	if err := cat.QueryRow(context.Background(),
		`SELECT write_paused_by::text FROM pgshard.shard_status WHERE shard_set = 'default' AND shard_id = 0`).Scan(&claim); err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		return ""
	}
	return *claim
}
