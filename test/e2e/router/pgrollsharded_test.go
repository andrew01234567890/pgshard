//go:build integration

package router

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPgrollAgainstAShardedDatabase runs the real pgroll binary against a
// database that is not local: a table sharded over two shards, alongside a
// reference table.
//
// This is the acceptance gate for stage 1 of PGS-765. The six pgshard
// changes it asked for are all merged -- functions, triggers and comments
// fan out; a pgroll migration of a shard key or a reference table is
// refused at start; a database can run a transaction's DDL statement by
// statement; a schema can be pinned to the home shard; a migration is
// answered once the router's snapshot has it; and DDL during a reshard is
// queued rather than refused. Whether pgroll actually works over them is a
// different question, and only the real binary answers it.
//
//	go install github.com/xataio/pgroll@v0.16.3
func TestPgrollAgainstAShardedDatabase(t *testing.T) {
	bin := os.Getenv("PGSHARD_TEST_PGROLL")
	if bin == "" {
		bin = os.Getenv("HOME") + "/go/bin/pgroll"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skip("no pgroll binary: go install github.com/xataio/pgroll@v0.16.3")
	}
	s := startShardedStackWith(t, nil, nil)
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	s.awaitReference(t, conn)

	// The database stays sharded. What it gains is what stage 1 gives
	// pgroll: its state schema pinned to the home shard, and DDL inside a
	// transaction run statement by statement.
	pool, err := pgxpool.New(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `UPDATE pgshard.databases SET local_schemas = '{pgroll}', ddl_transactions = 'sequential' WHERE name = $1`, appDatabase); err != nil {
		t.Fatal(err)
	}
	// pgroll installs event triggers, which PostgreSQL allows only to a
	// superuser, on any PostgreSQL and not only through pgshard. pgshard
	// refuses a superuser's fanned-out DDL, because the applier runs a
	// client's statement by SET ROLE into that client and a superuser there
	// would be an escalation. The two are only compatible in sequence: the
	// role is a superuser while pgroll installs its state, and a plain role
	// for every migration afterwards.
	superuser := func(on bool) {
		t.Helper()
		verb := "NOSUPERUSER"
		if on {
			verb = "SUPERUSER"
		}
		for id, dsn := range s.shardDSNs {
			sh, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatalf("shard %d: %v", id, err)
			}
			if _, err := sh.Exec(ctx, `ALTER ROLE `+appRole+` `+verb); err != nil {
				sh.Close()
				t.Fatalf("shard %d: %v", id, err)
			}
			sh.Close()
		}
	}
	superuser(true)
	// pgroll alters the table, so the role it connects as has to own it.
	// The fixture creates it as the superuser and grants; ownership is what
	// ALTER TABLE asks for, on any PostgreSQL.
	for id, dsn := range s.shardDSNs {
		sh, err := pgxpool.New(ctx, strings.Replace(dsn, "/postgres?", "/"+appDatabase+"?", 1))
		if err != nil {
			t.Fatalf("shard %d: %v", id, err)
		}
		for _, table := range []string{"orders", "regions"} {
			if _, err := sh.Exec(ctx, `ALTER TABLE `+table+` OWNER TO `+appRole); err != nil {
				sh.Close()
				t.Fatalf("shard %d: %s: %v", id, table, err)
			}
		}
		sh.Close()
	}
	// Let the router pick the catalog change up.
	time.Sleep(3 * time.Second)

	url := s.dsn(appRole, appPassword, appDatabase)
	run := func(what string, args ...string) (string, error) {
		c, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()
		out, err := exec.CommandContext(c, bin, append(args, "--postgres-url", url)...).CombinedOutput()
		fmt.Printf("PGROLL %s %v -> err=%v\n%s\n", what, args, err, out)
		return string(out), err
	}

	if _, err := run("init", "init"); err != nil {
		t.Fatalf("pgroll init against a sharded database: %v", err)
	}
	// Idempotent: the tool is run again by anyone who is not sure.
	if _, err := run("init again", "init"); err != nil {
		t.Errorf("pgroll init is not idempotent: %v", err)
	}

	// Its state is installed; from here it is an ordinary client, and every
	// migration below is a fanned-out one.
	superuser(false)

	// Rows on both shards, so anything pgroll does has to reach both.
	t0, t1 := twoTenants(t)
	for _, v := range []struct {
		tenant int64
		id     int
	}{{t0, 1}, {t0, 2}, {t1, 3}} {
		if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", v.tenant, v.id); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	// The schema is not empty -- the sharded table and the reference table
	// are already there -- and pgroll refuses to start a migration over a
	// schema with no history of its own until it has a baseline. It says
	// so and exits 0, so the exit code is not the thing to read.
	if _, err := run("baseline", "baseline", "00_existing", dir, "--yes"); err != nil {
		t.Fatalf("pgroll baseline: %v", err)
	}

	addColumn := dir + "/01_note.json"
	if err := os.WriteFile(addColumn, []byte(`{
	  "operations": [
	    {"add_column": {"table": "orders", "column": {"name": "note", "type": "text", "nullable": true}}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// pgroll exits 0 on a refusal it prints, so the output is the verdict.
	out, err := run("start add_column", "start", addColumn)
	if err != nil || strings.Contains(out, "Run `pgroll baseline`") {
		t.Fatalf("pgroll start of an add_column on a sharded table: %v\n%s", err, out)
	}

	// Every shard has the column: a migration that reached one of them is
	// the failure this whole ticket is about.
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "orders", "_pgroll_new_note") && !s.columnOn(t, id, "orders", "note") {
			t.Errorf("shard %d has neither note nor _pgroll_new_note after start", id)
		}
	}

	// The version schema exists and routes: reading through it must reach
	// both shards, not the home shard alone.
	var version string
	if err := conn.QueryRow(ctx, `SELECT pgroll.latest_version('public')`).Scan(&version); err != nil {
		t.Fatalf("no version schema after start: %v", err)
	}
	vc := s.connect(t)
	if _, err := vc.Exec(ctx, `SET search_path TO "public_`+version+`"`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := vc.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatalf("reading through the version view: %v", err)
	}
	if n != 3 {
		t.Errorf("the version view answered %d rows, want 3 -- a view routed to one shard answers 2", n)
	}

	if _, err := run("complete", "complete"); err != nil {
		t.Fatalf("pgroll complete: %v", err)
	}
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "orders", "note") {
			t.Errorf("shard %d does not have the completed column", id)
		}
		if s.columnOn(t, id, "orders", "_pgroll_new_note") {
			t.Errorf("shard %d still has the temporary column after complete", id)
		}
	}

	// A migration pgshard cannot complete is refused at its start, and
	// pgroll is left able to start another rather than wedged.
	shardKey := dir + "/02_shardkey.json"
	if err := os.WriteFile(shardKey, []byte(`{
	  "operations": [
	    {"alter_column": {"table": "orders", "column": "tenant_id", "type": "numeric",
	      "up": "tenant_id", "down": "tenant_id"}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = run("start shard key", "start", shardKey)
	if err == nil {
		t.Error("a pgroll migration of the shard key was accepted; completing it drops and renames the shard key")
	} else if !strings.Contains(out, "shard key") {
		t.Errorf("the refusal does not say why: %s", out)
	}

	// A refused start still leaves pgroll holding a migration for the
	// schema, so the next one is refused with "already in progress" until
	// the failed one is rolled back. That is pgroll's own protocol, and the
	// point of refusing at start rather than at complete: rollback can undo
	// a start, and nothing can undo a half-completed rename.
	if _, err := run("rollback", "rollback"); err != nil {
		t.Fatalf("a refused start could not be rolled back, which leaves pgroll wedged: %v", err)
	}

	// And the tool still works afterwards, which is the part that matters:
	// a refusal that wedges pgroll is worse than one that does not happen.
	index := dir + "/03_index.json"
	if err := os.WriteFile(index, []byte(`{
	  "operations": [
	    {"create_index": {"table": "orders", "name": "orders_note_idx", "columns": [{"column": "note"}]}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("start create_index", "start", index); err != nil {
		t.Fatalf("pgroll was wedged by the refusal: %v", err)
	}
	if _, err := run("complete index", "complete"); err != nil {
		t.Fatalf("pgroll complete after the refusal: %v", err)
	}
	for id := range s.shardDSNs {
		if !s.indexOn(t, id, "orders_note_idx") {
			t.Errorf("shard %d does not have the index pgroll created", id)
		}
	}
}

// columnOn reports whether one shard's copy of a table has a column.
func (s *shardedStack) columnOn(tb testing.TB, shard int, table, column string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_name = $1 AND column_name = $2)`, table, column)
}

// indexOn reports whether one shard has an index by name.
func (s *shardedStack) indexOn(tb testing.TB, shard int, name string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, name)
}

func (s *shardedStack) shardBool(tb testing.TB, shard int, sql string, args ...any) bool {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, strings.Replace(s.shardDSNs[shard], "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		tb.Fatalf("shard %d: %v", shard, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var ok bool
	if err := conn.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
		tb.Fatalf("shard %d: %s: %v", shard, sql, err)
	}
	return ok
}
