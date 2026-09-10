package catalog

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestALocalDatabaseAndADistributedTableCannotBothBeDeclared.
//
// local_only says every object of a database lives on its home shard, which
// is what lets the router run its DDL on the client's own connection instead
// of fanning it out. A sharded or reference table in such a database would
// make that false, and the DDL that had already run without being recorded
// could not be reconstructed for the shards it never reached.
//
// The two declarations therefore have to refuse each other -- including
// when they are made at the same time, from different sessions, which is the
// case a plain read cannot see.
func TestALocalDatabaseAndADistributedTableCannotBothBeDeclared(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	dsn := startPostgres(t, candidateImages[0])
	conn := connect(t, dsn)
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) error {
		_, err := conn.Exec(ctx, sql, args...)
		return err
	}
	if err := exec(`INSERT INTO pgshard.databases (name, local_only) VALUES ('local', true)`); err != nil {
		t.Fatal(err)
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('local', 'public', 'items', 'unsharded')`); err != nil {
		t.Fatalf("an unsharded table is the only kind a local database holds: %v", err)
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('local', 'public', 'orders', 'sharded', 'tenant_id')`); err == nil {
		t.Fatal("a sharded table was declared in a local database")
	} else if !strings.Contains(err.Error(), "local_only") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// And the other direction.
	if err := exec(`INSERT INTO pgshard.databases (name) VALUES ('shared')`); err != nil {
		t.Fatal(err)
	}
	if err := exec(`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('shared', 'public', 'orders', 'sharded', 'tenant_id')`); err != nil {
		t.Fatal(err)
	}
	if err := exec(`UPDATE pgshard.databases SET local_only = true WHERE name = 'shared'`); err == nil {
		t.Fatal("a database holding a sharded table was declared local")
	} else if !strings.Contains(err.Error(), "sharded or reference") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// THE RACE. Neither check sees the other session's uncommitted row, so
	// without a lock both commit and the database ends up holding exactly
	// what it says it cannot.
	if err := exec(`INSERT INTO pgshard.databases (name) VALUES ('racy')`); err != nil {
		t.Fatal(err)
	}
	a, b := connect(t, dsn), connect(t, dsn)
	ta, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ta.Rollback(ctx) }()
	if _, err := ta.Exec(ctx, `UPDATE pgshard.databases SET local_only = true WHERE name = 'racy'`); err != nil {
		t.Fatal(err)
	}

	// b declares a sharded table in it while a is still open. It must not
	// be able to decide on its own; it has to wait for a.
	done := make(chan error, 1)
	go func() {
		tb, err := b.Begin(ctx)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = tb.Rollback(ctx) }()
		if _, err := tb.Exec(ctx, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('racy', 'public', 'orders', 'sharded', 'tenant_id')`); err != nil {
			done <- err
			return
		}
		done <- tb.Commit(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("the sharded table was decided while the declaration was still open: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := ta.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("both declarations committed: the local database holds a sharded table")
		}
		if !strings.Contains(err.Error(), "local_only") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting declaration never answered")
	}
}
