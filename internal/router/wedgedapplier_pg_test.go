package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestASynchronousDDLWaitEndsWhenTheApplierIsAliveButStuck (PGS-972).
//
// A shard that stops answering mid-DDL without closing the connection wedges
// the applier's pass, while its liveness goroutine keeps beating on its own
// timer. A client waiting synchronously for the migration must not read that
// beat as progress: it waits with no end, and so does every session queued
// behind it. PGS-907 made the wait read the pass stamp instead; the catalog
// and controller halves of that are tested, but nothing pinned that the
// router's DEFAULT probe is the pass one -- it could go back to liveness and
// every test stayed green. This drives the real queue against a real catalog.
func TestASynchronousDDLWaitEndsWhenTheApplierIsAliveButStuck(t *testing.T) {
	dsn := startCatalogContainer(t, "ghcr.io/andrew01234567890/pgshard-postgres:18")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO pgshard.databases (name) VALUES ('app')`)
	exec(`UPDATE pgshard.leader_term SET term = 1`)
	id, err := catalog.EnqueueMigration(ctx, pool, catalog.DDLMigration{Database: "app", Statement: "CREATE INDEX i ON t (c)",
		Kind: "CREATE INDEX", Strategy: "direct", Scope: "all"})
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE pgshard.migrations SET state = 'running', per_shard = '{"0": {"state": "running"}}' WHERE id = $1`, id)

	// A pass finished once, long ago, and has not finished since.
	if err := catalog.BeatController(ctx, pool, catalog.HeartbeatApplierPass, 1); err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE pgshard.controller_heartbeat SET beat_at = now() - interval '1 hour' WHERE component = $1`, catalog.HeartbeatApplierPass)

	// The liveness goroutine, beating throughout; and, for the contrast, a
	// pass that keeps finishing.
	beating, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	passes := make(chan bool, 1)
	passes <- false
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-beating.Done():
				return
			case <-tick.C:
			}
			_ = catalog.BeatController(beating, pool, catalog.HeartbeatApplier, 1)
			pass := <-passes
			passes <- pass
			if pass {
				_ = catalog.BeatController(beating, pool, catalog.HeartbeatApplierPass, 1)
			}
		}
	}()

	q := &PGMigrationQueue{Pool: pool, Poll: 20 * time.Millisecond, MaxWait: 2 * time.Second}

	// Passes finishing: the controller is getting somewhere, so the wait
	// lasts until the caller gives up.
	<-passes
	passes <- true
	bounded, cancel := context.WithTimeout(ctx, 4*time.Second)
	_, err = q.Wait(bounded, id, nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with passes finishing the wait ended with %v, want it to last until the caller gave up", err)
	}

	// The pass wedges; liveness keeps beating. The wait must end on its own
	// bound, naming the reason, well before the caller's.
	<-passes
	passes <- false
	exec(`UPDATE pgshard.controller_heartbeat SET beat_at = now() - interval '1 hour' WHERE component = $1`, catalog.HeartbeatApplierPass)
	bounded, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	start := time.Now()
	_, err = q.Wait(bounded, id, nil)
	var quiet errNoApplier
	if !errors.As(err, &quiet) {
		t.Fatalf("with the pass wedged and liveness beating the wait ended with %v after %s, want errNoApplier", err, time.Since(start).Round(time.Millisecond))
	}
}
