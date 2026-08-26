package controller

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestPinSourceIsSetOnce guards the rule that a workflow copies from the set it
// recorded, not from whichever set is serving when a later pass happens to ask.
// Copier passes are not leader-gated, so two of them can resolve different sets
// around a flip.
func TestPinSourceIsSetOnce(t *testing.T) {
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
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state) VALUES
		('default', 0, 'shard0', 'serving'), ('default', 1, 'shard1', 'serving')`)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('12121212-1212-1212-1212-121212121212', 'reshard', 'running', '{"shard_set": "g2"}'::jsonb, '{}'::jsonb)`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	c := &Copier{Pool: pool}

	load := func() *copyWorkflow {
		wf := &copyWorkflow{id: "12121212-1212-1212-1212-121212121212"}
		if err := conn.QueryRow(ctx, `SELECT coalesce(status->'cutover'->>'source_set', '') FROM pgshard.workflows WHERE id = $1::uuid`, wf.id).
			Scan(&wf.cutover.SourceSet); err != nil {
			t.Fatal(err)
		}
		return wf
	}

	set, ids, err := c.pinSource(ctx, load())
	if err != nil {
		t.Fatal(err)
	}
	if set != "default" {
		t.Fatalf("first pin resolved %q, want default", set)
	}
	if len(ids) != 2 {
		t.Fatalf("shard ids = %v, want both shards", ids)
	}

	// A newer generation takes over while the workflow is still copying, and
	// a second pass is working from a view of the workflow it loaded before
	// the first pass recorded anything -- which is what a second, non-leader
	// controller process has.
	mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('g3', 3, 'serving')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('g3', 0, '[,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state) VALUES ('g3', 0, 'g3-shard0', 'serving')`)
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET state = 'retired' WHERE shard_set = 'default'`)

	stale := &copyWorkflow{id: "12121212-1212-1212-1212-121212121212"}
	set, ids, err = c.pinSource(ctx, stale)
	if err != nil {
		t.Fatal(err)
	}
	if set != "default" {
		t.Fatalf("a concurrent pass claimed %q; the source recorded first must win", set)
	}
	if len(ids) != 2 {
		t.Fatalf("shard ids = %v, want the recorded source's shards", ids)
	}

	var stored string
	if err := conn.QueryRow(ctx, `SELECT status->'cutover'->>'source_set' FROM pgshard.workflows WHERE id = '12121212-1212-1212-1212-121212121212'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "default" {
		t.Fatalf("the recorded source was overwritten with %q", stored)
	}
}

// TestPinSourceLoserAdoptsTheWinner covers the overlapping case the sequential
// test cannot: the loser's claim statement starts while the winner still holds
// the row, so it blocks, then finds nothing to update. It has to adopt the
// winner's source rather than report the workflow gone.
func TestPinSourceLoserAdoptsTheWinner(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.shard_sets SET generation = 1, state = 'serving' WHERE shard_set = 'default'`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state) VALUES ('default', 0, 'shard0', 'serving')`)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('14141414-1414-1414-1414-141414141414', 'reshard', 'running', '{"shard_set": "g2"}'::jsonb, '{}'::jsonb)`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	c := &Copier{Pool: pool}

	// The winner records 'default' but holds the row until we say so.
	winner, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := winner.Exec(ctx, `UPDATE pgshard.workflows
		SET status = status || jsonb_build_object('cutover', jsonb_build_object('source_set', 'default'))
		WHERE id = '14141414-1414-1414-1414-141414141414'`); err != nil {
		t.Fatal(err)
	}

	type result struct {
		set string
		err error
	}
	done := make(chan result, 1)
	go func() {
		set, _, err := c.pinSource(ctx, &copyWorkflow{id: "14141414-1414-1414-1414-141414141414"})
		done <- result{set, err}
	}()

	// Wait until the loser is genuinely blocked on the row rather than merely
	// slow to start; a sleep here would let a delayed worker pass even the
	// broken form.
	observer := connect(t, dsn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting bool
		if err := observer.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case r := <-done:
			t.Fatalf("the loser finished before the winner committed: %q %v", r.set, r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the loser never blocked on the workflow row")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := winner.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("the loser failed instead of adopting the winner: %v", r.err)
		}
		if r.set != "default" {
			t.Fatalf("the loser used %q, want the winner's default", r.set)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the loser never returned")
	}
}

// TestPinSourceRefusesPartialStatus guards against building replication for
// only some of a source's shards: the ranges are what the workflow owns, and a
// status row can be missing for a shard that still exists.
func TestPinSourceRefusesPartialStatus(t *testing.T) {
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
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state) VALUES
		('default', 0, 'shard0', 'serving')`)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('13131313-1313-1313-1313-131313131313', 'reshard', 'running',
		 '{"shard_set": "g2", "source_set": "default"}'::jsonb, '{}'::jsonb)`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	c := &Copier{Pool: pool}

	wf := &copyWorkflow{id: "13131313-1313-1313-1313-131313131313"}
	wf.spec.SourceSet = "default"
	if _, _, err := c.pinSource(ctx, wf); err == nil {
		t.Fatal("a source missing a status row was accepted; replication would cover only some of its shards")
	}
}
