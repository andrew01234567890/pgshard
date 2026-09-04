package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestCancelsWorkflowWhoseSourceVanished guards against a reshard whose source
// set was dropped: every cutover pass rebuilds the source's shards from its
// ranges, so it failed identically forever with no supported way to stop it.
func TestCancelsWorkflowWhoseSourceVanished(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET generation = 1, state = 'serving' WHERE shard_set = 'default'`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint) VALUES
		('default', 0, 'shard0', 'serving', 1, 'shard0-0:5432')`)
	// The target still exists; the source it copies from was dropped.
	mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('g2', 2, 'provisioning')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('g2', 0, '[,0)'), ('g2', 1, '[0,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('15151515-1515-1515-1515-151515151515', 'reshard', 'running',
		 '{"shard_set": "g2", "source_set": "gone"}'::jsonb, '{"stage": "copying"}'::jsonb)`)

	res := reconcile(t, conn)
	if res.ReshardsCancelled != 1 {
		t.Fatalf("a workflow whose source is gone must be cancelled, got %+v", res)
	}

	var state, stage, reason string
	if err := conn.QueryRow(ctx, `SELECT state, status->>'stage', status->>'reason'
		FROM pgshard.workflows WHERE id = '15151515-1515-1515-1515-151515151515'`).Scan(&state, &stage, &reason); err != nil {
		t.Fatal(err)
	}
	if state != StateCancelled {
		t.Fatalf("state = %s, want %s", state, StateCancelled)
	}
	if stage != StageCancelled {
		t.Fatalf("stage = %s, want %s: there is no source left to clean up on", stage, StageCancelled)
	}
	if reason != "source shard set removed" {
		t.Fatalf("reason = %q, want it to name the source", reason)
	}

	// The target set is still registered, so its status rows must survive.
	var rows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'g2'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Logf("target status rows retained: %d", rows)
	}
}

// TestCancelsPendingWorkflowWhoseSetVanished: an in-place range edit creates
// a reshard workflow in 'pending', and nothing drives it yet. Dropping the
// set it was created for used to leave that row active for ever -- the
// cancel pass listed it, then skipped it through the default arm. It
// outlived the set, and because the catalog counts an active workflow as
// moving data out of its source, it also held off the next reshard that
// wanted to.
func TestCancelsPendingWorkflowWhoseSetVanished(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET generation = 1, state = 'serving' WHERE shard_set = 'default'`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint) VALUES
		('default', 0, 'shard0', 'serving', 1, 'shard0-0:5432')`)
	// The workflow's own set never existed as far as this pass can see.
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('16161616-1616-1616-1616-161616161616', 'reshard', 'pending',
		 '{"shard_set": "gone", "source_set": "default"}'::jsonb, '{}'::jsonb)`)

	res := reconcile(t, conn)
	if res.ReshardsCancelled != 1 {
		t.Fatalf("a pending workflow whose set is gone must be cancelled, got %+v", res)
	}

	var state, stage string
	if err := conn.QueryRow(ctx, `SELECT state, status->>'stage'
		FROM pgshard.workflows WHERE id = '16161616-1616-1616-1616-161616161616'`).Scan(&state, &stage); err != nil {
		t.Fatal(err)
	}
	if state != StateCancelled {
		t.Fatalf("state = %s, want %s", state, StateCancelled)
	}
	if stage != StageCancelled {
		t.Fatalf("stage = %s, want %s: a pending workflow created nothing to clean up", stage, StageCancelled)
	}
}
