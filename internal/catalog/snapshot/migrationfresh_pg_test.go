package snapshot

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestRefreshHoldsWhatAMigrationRecordedWhenItsWaitReturns (PGS-871) plays
// a migration the way the applier writes it -- per-shard progress, then the
// view row and the final state in one transaction -- and looks at the
// snapshot at the moment a router polling the migration row, as Wait does,
// first sees it done. Each progress update notifies, so a DDL burst drains
// the notification budget and the reload that would carry the view waits
// for a refill: without Refresh the view was missing on every run, and a
// view missing from the snapshot is read from one shard with no error.
func TestRefreshHoldsWhatAMigrationRecordedWhenItsWaitReturns(t *testing.T) {
	dsn := startPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name, default_placement) VALUES ('app', 'sharded')`)
	w := NewWatcher(dsn, Options{Logf: func(string, ...any) {}})
	go func() { _ = w.Run(ctx) }()
	waitUntil(t, 20*time.Second, "the first snapshot", func() bool { return w.Current() != nil })
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	poller, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = poller.Close(context.WithoutCancel(ctx)) }()

	stale := 0
	const runs = 5
	for i := range runs {
		id := [16]byte{0x87, 0x1, byte(i)}
		view := "v" + string(rune('a'+i))
		exec(`INSERT INTO pgshard.migrations (id, database, statement, strategy, state) VALUES ($1, 'app', 'CREATE VIEW', 'direct', 'running')`, id)
		for shard := range 6 {
			exec(`UPDATE pgshard.migrations SET per_shard = per_shard || jsonb_build_object($2::text, 'applied') WHERE id = $1`, id, fmt.Sprint(shard))
			time.Sleep(120 * time.Millisecond)
		}
		exec(`BEGIN`)
		exec(`INSERT INTO pgshard.views (database, schema_name, view_name, base_schema, base_name, shape) VALUES ('app', 'public', $1, 'public', 't', 'opaque')`, view)
		exec(`UPDATE pgshard.migrations SET state = 'complete' WHERE id = $1`, id)
		exec(`COMMIT`)

		tick := time.NewTicker(200 * time.Millisecond)
		for range tick.C {
			var state string
			if err := poller.QueryRow(ctx, `SELECT state FROM pgshard.migrations WHERE id = $1`, id).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state == "complete" {
				break
			}
		}
		tick.Stop()
		key := TableKey{Database: "app", SchemaName: "public", TableName: view}
		if _, ok := w.Current().Views[key]; !ok {
			stale++
		}
		start := time.Now()
		if err := w.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		if _, ok := w.Current().Views[key]; !ok {
			t.Fatalf("run %d: after Refresh the snapshot still lacks the view the migration recorded", i)
		}
		if took := time.Since(start); took > notifyRefill/2 {
			t.Errorf("run %d: Refresh took %s; it waited for the notification budget", i, took)
		}
	}
	t.Logf("%d of %d migrations were seen done before the notification-driven reload held their view", stale, runs)
}
