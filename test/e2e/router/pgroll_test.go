//go:build integration

package router

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPgrollAgainstALocalDatabase runs the real pgroll binary against a
// real pgshard stack: init, start, a read and a write through the version
// schema it creates, and complete.
//
// pgroll is the tool pgshard's own online-DDL design borrows from, and
// every one of its steps is something pgshard used to refuse: its whole
// setup runs in one transaction, it creates functions, triggers, comments
// and event triggers, it backfills with a writable CTE, and it replaces its
// version views in a BEGIN/COMMIT batch. In a database declared local none
// of those are questions about distribution any more.
//
// The binary is not vendored, so this skips without it:
//
//	go install github.com/xataio/pgroll@v0.16.3
func TestPgrollAgainstALocalDatabase(t *testing.T) {
	bin := os.Getenv("PGSHARD_TEST_PGROLL")
	if bin == "" {
		bin = os.Getenv("HOME") + "/go/bin/pgroll"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skip("no pgroll binary: go install github.com/xataio/pgroll@v0.16.3")
	}
	s := startStack(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `UPDATE pgshard.databases SET local_only = true WHERE name = $1`, appDatabase); err != nil {
		t.Fatal(err)
	}
	// pgroll installs event triggers, which PostgreSQL allows only to a
	// superuser -- on any PostgreSQL, not only through pgshard.
	sh, err := pgxpool.New(ctx, s.shardDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close()
	if _, err := sh.Exec(ctx, `ALTER ROLE `+appRole+` SUPERUSER`); err != nil {
		t.Fatal(err)
	}
	conn := s.connect(t)
	if _, err := conn.Exec(ctx, `select 1`); err != nil {
		t.Fatal(err)
	}
	// Let the router pick up the catalog change.
	time.Sleep(3 * time.Second)

	url := s.dsn(appRole, appPassword, appDatabase)
	run := func(args ...string) {
		c, cancel := context.WithTimeout(ctx, 120*time.Second)
		defer cancel()
		out, err := exec.CommandContext(c, bin, append(args, "--postgres-url", url)...).CombinedOutput()
		fmt.Printf("PGROLL %v -> err=%v\n%s\n", args, err, out)
	}
	run("init")

	// A table for pgroll to migrate, and a row in it, so the backfill has
	// something to do.
	for _, sql := range []string{
		`CREATE TABLE people (id int PRIMARY KEY, name text NOT NULL)`,
		`INSERT INTO people (id, name) VALUES (1, 'ada'), (2, 'grace')`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	dir := t.TempDir()
	mig := dir + "/01_nickname.json"
	if err := os.WriteFile(mig, []byte(`{
	  "operations": [
	    {"alter_column": {"table": "people", "column": "name", "type": "varchar(255)",
	      "up": "UPPER(name)", "down": "name"}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	run("start", mig)

	// The migration is live: the new version's schema of views exists, and
	// reading through it gives the backfilled column.
	var version string
	if err := conn.QueryRow(ctx, `SELECT pgroll.latest_version('public')`).Scan(&version); err != nil {
		t.Fatalf("no version schema after start: %v", err)
	}
	fmt.Println("VERSION SCHEMA:", version)
	vc := s.connect(t)
	if _, err := vc.Exec(ctx, `SET search_path TO "public_`+version+`"`); err != nil {
		t.Fatal(err)
	}
	rows, err := vc.Query(ctx, `SELECT id, name FROM people ORDER BY id`)
	if err != nil {
		t.Fatalf("reading through the version view: %v", err)
	}
	got := map[int]string{}
	for rows.Next() {
		var id int
		var nick string
		if err := rows.Scan(&id, &nick); err != nil {
			t.Fatal(err)
		}
		got[id] = nick
	}
	rows.Close()
	fmt.Println("THROUGH THE VIEW:", got)
	if got[1] != "ADA" || got[2] != "GRACE" {
		t.Fatalf("backfill did not run through pgshard: %v", got)
	}

	// A write through the new version's view reaches the old column too,
	// which is what pgroll's dual-write trigger is for -- and that trigger
	// compares current_setting('search_path') to the version schema name
	// exactly, so it is also a check on how pgshard reports the GUC.
	if _, err := vc.Exec(ctx, `INSERT INTO people (id, name) VALUES (3, 'hopper')`); err != nil {
		t.Fatalf("insert through the version view: %v", err)
	}
	var old string
	if err := conn.QueryRow(ctx, `SELECT name FROM people WHERE id = 3`).Scan(&old); err != nil {
		t.Fatal(err)
	}
	fmt.Println("DUAL WRITE landed in the old column as:", old)
	if old != "hopper" {
		t.Fatalf("the dual-write trigger did not fill the old column: %q", old)
	}

	run("complete")
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM people WHERE name IS NOT NULL`).Scan(&n); err != nil {
		t.Fatalf("after complete: %v", err)
	}
	if n != 3 {
		t.Fatalf("after complete %d rows have the new column, want 3", n)
	}
}
