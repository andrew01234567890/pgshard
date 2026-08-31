//go:build integration

package router

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func sqlstate(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestRouterSingleShard(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)

	t.Run("ddl_dml_select", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "create table items (id int primary key, name text)"); err != nil {
			t.Fatal(err)
		}
		if tag, err := conn.Exec(ctx, "insert into items values ($1, $2), ($3, $4)", 1, "one", 2, "two"); err != nil || tag.RowsAffected() != 2 {
			t.Fatalf("insert: %v %v", tag, err)
		}
		var name string
		if err := conn.QueryRow(ctx, "select name from items where id = $1", 2).Scan(&name); err != nil || name != "two" {
			t.Fatalf("select: %q %v", name, err)
		}
		if err := conn.QueryRow(ctx, "select name from items where id = 1", pgx.QueryExecModeSimpleProtocol).Scan(&name); err != nil || name != "one" {
			t.Fatalf("simple select: %q %v", name, err)
		}
	})

	t.Run("prepared_statements", func(t *testing.T) {
		if _, err := conn.Prepare(ctx, "by_id", "select name from items where id = $1"); err != nil {
			t.Fatal(err)
		}
		var name string
		if err := conn.QueryRow(ctx, "by_id", 1).Scan(&name); err != nil || name != "one" {
			t.Fatalf("by_id: %q %v", name, err)
		}
		if err := conn.Deallocate(ctx, "by_id"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("transaction_rollback", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into items values (3, 'three')"); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := tx.QueryRow(ctx, "select count(*) from items").Scan(&n); err != nil || n != 3 {
			t.Fatalf("inside txn: %d %v", n, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, "select count(*) from items").Scan(&n); err != nil || n != 2 {
			t.Fatalf("after rollback: %d %v", n, err)
		}
	})

	t.Run("set_replayed_after_release", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "set application_name to 'router-e2e'"); err != nil {
			t.Fatal(err)
		}
		// This used to take a session advisory lock and assert it was gone
		// after a transaction, as proof the backend had been released.
		// That is the defect, not a probe: PostgreSQL keeps a session
		// advisory lock until it is unlocked or the session ends, so a
		// test asserting it vanishes was asserting that pgshard breaks
		// the guarantee. The router refuses the session-scoped forms now,
		// and the pooler's own tests cover backend release.
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var v string
		if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "router-e2e" {
			t.Fatalf("application_name after release: %q %v", v, err)
		}
	})

	t.Run("session_advisory_lock_refused", func(t *testing.T) {
		// A lock the router cannot keep is refused rather than granted: the
		// backend holding it does not stay with the session, so a second
		// client would be told it holds the same lock. The transaction
		// form is pinned to the transaction and means what it says.
		for _, sql := range []string{"select pg_advisory_lock(4242)", "select pg_try_advisory_lock(4242)", "select pg_advisory_unlock_all()"} {
			if _, err := conn.Exec(ctx, sql); sqlstate(err) != "0A000" {
				t.Fatalf("%s: %v, want a 0A000 refusal", sql, err)
			}
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var got bool
		if err := tx.QueryRow(ctx, "select pg_try_advisory_xact_lock(4242)").Scan(&got); err != nil || !got {
			t.Fatalf("a transaction advisory lock must still work: %v %v", got, err)
		}
	})

	t.Run("copy_from_stdin", func(t *testing.T) {
		tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("10\tten\n11\televen\n"), "copy items from stdin")
		if err != nil || tag.String() != "COPY 2" {
			t.Fatalf("copy: %v %v", tag, err)
		}
		var n int
		if err := conn.QueryRow(ctx, "select count(*) from items where id >= 10").Scan(&n); err != nil || n != 2 {
			t.Fatalf("after copy: %d %v", n, err)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		go func() {
			time.Sleep(500 * time.Millisecond)
			_ = conn.PgConn().CancelRequest(ctx)
		}()
		start := time.Now()
		_, err := conn.Exec(ctx, "select pg_sleep(20)")
		if sqlstate(err) != "57014" {
			t.Fatalf("cancel: %v", err)
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("cancel took %s", time.Since(start))
		}
		var n int
		if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
			t.Fatalf("after cancel: %v", err)
		}
	})

	t.Run("refusals_and_auth", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "listen ch"); sqlstate(err) != "0A000" {
			t.Fatalf("listen: %v", err)
		}
		if _, err := pgx.Connect(ctx, s.dsn(appRole, "wrong", appDatabase)); sqlstate(err) != "28P01" {
			t.Fatalf("wrong password: %v", err)
		}
		if _, err := pgx.Connect(ctx, s.dsn(appRole, appPassword, "nope")); sqlstate(err) != "3D000" {
			t.Fatalf("missing database: %v", err)
		}
	})

	t.Run("psql", func(t *testing.T) {
		if err := exec.Command("docker", "image", "inspect", pgImage()).Run(); err != nil {
			t.Skip("no psql image")
		}
		name := "pgshard-router-e2e-psql-" + s.routerPort
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
		var out []byte
		var err error
		for attempt := 0; attempt < 5; attempt++ {
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			out, err = exec.CommandContext(cctx, "docker", "run", "--rm", "--name", name, "--add-host", "host.docker.internal:host-gateway",
				"-e", "PGPASSWORD="+appPassword, "-e", "PGCONNECT_TIMEOUT=10", "--entrypoint", "psql", pgImage(),
				"-h", "host.docker.internal", "-p", s.routerPort, "-U", appRole, "-d", appDatabase, "-X", "-At",
				"-c", "select count(*) from items", "-c", "begin", "-c", "insert into items values (99, 'x')", "-c", "rollback",
				"-c", "select count(*) from items where id = 99").CombinedOutput()
			cancel()
			if err == nil || !strings.Contains(string(out), "Connection refused") {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			t.Fatalf("psql: %v: %s", err, out)
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		if len(lines) != 5 || lines[0] == "" || lines[1] != "BEGIN" || lines[2] != "INSERT 0 1" || lines[3] != "ROLLBACK" || lines[4] != "0" {
			t.Fatalf("psql output %q", out)
		}
	})
}

// BenchmarkRouterSelect1 compares `select 1` through the router with a direct
// pgx connection to the shard. Enable with PGSHARD_BENCH_ROUTER=1.
func BenchmarkRouterSelect1(b *testing.B) {
	if os.Getenv("PGSHARD_BENCH_ROUTER") == "" {
		b.Skip("set PGSHARD_BENCH_ROUTER=1 to run")
	}
	s := startStack(b)
	ctx := context.Background()
	viaRouter := s.connect(b)
	direct, err := pgx.Connect(ctx, s.shardDSN)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = direct.Close(ctx) }()
	for _, c := range []struct {
		name string
		conn *pgx.Conn
	}{{"router", viaRouter}, {"direct", direct}} {
		b.Run(c.name, func(b *testing.B) {
			var n int
			for i := 0; i < b.N; i++ {
				if err := c.conn.QueryRow(ctx, "select 1").Scan(&n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestParameterStatusReachesTheClient: PostgreSQL reports a GUC_REPORT
// setting whenever it changes, and drivers read timestamps, intervals and
// escaped text according to what they were last told. The router used to
// drop those messages, so a client that changed one went on parsing results
// by the values it was given at startup.
func TestParameterStatusReachesTheClient(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	defer func() { _ = conn.Close(ctx) }()

	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "UTC" {
		t.Fatalf("TimeZone at startup = %q, want UTC", got)
	}
	if _, err := conn.Exec(ctx, "set time zone 'America/New_York'"); err != nil {
		t.Fatal(err)
	}
	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "America/New_York" {
		t.Fatalf("TimeZone after SET = %q; the client is still parsing by the value it had at startup", got)
	}

	// A setting undone by ROLLBACK is reported back, so the client's view
	// follows the transaction rather than the statement.
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "set local time zone 'Asia/Tokyo'"); err != nil {
		t.Fatal(err)
	}
	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "Asia/Tokyo" {
		t.Fatalf("TimeZone after SET LOCAL = %q", got)
	}
	if _, err := conn.Exec(ctx, "rollback"); err != nil {
		t.Fatal(err)
	}
	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "America/New_York" {
		t.Fatalf("TimeZone after the rollback = %q; the client kept a value the backend no longer holds", got)
	}
}
