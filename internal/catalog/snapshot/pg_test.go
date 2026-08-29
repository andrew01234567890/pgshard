package snapshot

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

const pgImage = "ghcr.io/andrew01234567890/pgshard-postgres:18"

func startPostgres(t *testing.T) string {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable; skipping snapshot integration tests")
	}
	if exec.Command("docker", "image", "inspect", pgImage).Run() != nil {
		if out, err := exec.Command("docker", "pull", pgImage).CombinedOutput(); err != nil {
			dockertest.Unavailable(t, "image %s unavailable: %v: %s", pgImage, err, out)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--entrypoint", "sh", pgImage, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("postgres did not become ready")
	return ""
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func TestSnapshotWithPostgres(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name, default_placement) VALUES ('app', 'sharded')`)
	mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES
		('app', 'public', 'orders', 'sharded', 'customer_id'),
		('app', 'public', 'settings', 'unsharded', NULL),
		('app', 'public', 'pending', 'sharded', 'id')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
		('default', 0, '[,0)'), ('default', 1, '[0,)')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint) VALUES
		('default', 0, 'g0', 'serving', 3, 'g0-0:5432'), ('default', 1, 'g1', 'migrating', 1, NULL)`)
	mustExec(t, conn, `INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key, effective_generation) VALUES
		('app', 'public', 'orders', 'sharded', 'customer_id', 7)`)
	mustExec(t, conn, `INSERT INTO pgshard.roles (rolname, verifier) VALUES ('alice', 'SCRAM-SHA-256$4096:salt$a:b')`)
	mustExec(t, conn, `UPDATE pgshard.shard_map_generation SET generation = 5`)

	t.Run("load", func(t *testing.T) {
		s, err := Load(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if s.ShardMapGeneration != 5 || s.DesiredGeneration == 0 {
			t.Fatalf("generations: %s", s)
		}
		if id, err := s.Locate("default", -1); err != nil || id != 0 {
			t.Fatalf("locate -1: %d %v", id, err)
		}
		if id, err := s.Locate("default", 0); err != nil || id != 1 {
			t.Fatalf("locate 0: %d %v", id, err)
		}
		if sv := s.Serving[ShardKey{"default", 0}]; sv.PrimaryEndpoint != "g0-0:5432" || sv.Epoch != 3 || sv.State != "serving" {
			t.Fatalf("serving: %+v", sv)
		}
		if got := s.Tables[TableKey{"app", "public", "orders"}]; got.Placement != "sharded" || got.ShardKey != "customer_id" || got.Generation != 7 {
			t.Fatalf("effective placement from status: %+v", got)
		}
		if got := s.Tables[TableKey{"app", "public", "settings"}]; got.Placement != "unsharded" {
			t.Fatalf("unsharded fallback: %+v", got)
		}
		if _, ok := s.Tables[TableKey{"app", "public", "pending"}]; ok {
			t.Fatal("sharded table without status must not be effective")
		}
		if s.Databases["app"].DefaultPlacement != "sharded" {
			t.Fatalf("databases: %+v", s.Databases)
		}
		if strings.Contains(s.String(), "SCRAM") {
			t.Fatal("snapshot string leaks verifier")
		}
		roles, err := LoadRoles(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := roles.Verifier("alice"); !ok || !strings.HasPrefix(v, "SCRAM") {
			t.Fatalf("roles: %v", roles)
		}
		if out := fmt.Sprintf("%+v %#v", roles, roles); strings.Contains(out, "SCRAM") {
			t.Fatalf("roles leaked: %s", out)
		}
	})

	t.Run("pending_rewrite_hides_columns", func(t *testing.T) {
		id, err := catalog.EnqueueMigration(ctx, conn, catalog.DDLMigration{Database: "app",
			Statement: "alter table orders alter column amount type bigint", Kind: "ALTER TABLE",
			Strategy: catalog.StrategyRewrite, Scope: "all",
			Meta: catalog.MigrationMeta{Rewrite: &catalog.RewriteChange{Schema: "public", Table: "orders",
				Column: "amount", NewType: "bigint", Using: "amount::bigint", Columns: []string{"customer_id", "id", "amount"}}}})
		if err != nil {
			t.Fatal(err)
		}
		s, err := Load(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Tables[TableKey{"app", "public", "orders"}]
		if len(got.HiddenColumns) != 1 || !strings.HasPrefix(got.HiddenColumns[0], "_pgshard_amount_") {
			t.Fatalf("hidden columns: %+v", got)
		}
		if len(got.VisibleColumns) != 3 || got.VisibleColumns[2] != "amount" {
			t.Fatalf("visible columns: %+v", got)
		}
		m, err := catalog.LoadMigration(ctx, conn, id)
		if err != nil {
			t.Fatal(err)
		}
		m.State = catalog.MigrationFailed
		if err := catalog.SaveMigrationProgress(ctx, conn, m); err != nil {
			t.Fatal(err)
		}
		s, err = Load(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.Tables[TableKey{"app", "public", "orders"}]; len(got.HiddenColumns) != 0 || len(got.VisibleColumns) != 0 {
			t.Fatalf("finished rewrite still hides columns: %+v", got)
		}
	})

	t.Run("consistency_from_catalog", func(t *testing.T) {
		s, err := Load(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		tr := NewConsistencyWatcher().Observe(s)
		if len(tr) != 1 || tr[0].To != Inconsistent || tr[0].Blocking[0].ShardID != 1 {
			t.Fatalf("expected shard 1 migrating to block: %+v", tr)
		}
	})

	for _, tc := range []struct {
		name          string
		disableListen bool
		wantFast      bool
	}{
		{"listen_propagates_fast", false, true},
		{"periodic_only_is_slow", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wctx, cancel := context.WithCancel(ctx)
			defer cancel()
			w := NewWatcher(dsn, Options{ReloadInterval: 10 * time.Second, DisableListen: tc.disableListen, Logf: t.Logf})
			changes, unsub := w.Subscribe()
			defer unsub()
			done := make(chan error, 1)
			go func() { done <- w.Run(wctx) }()
			select {
			case <-changes:
			case err := <-done:
				t.Fatalf("watcher exited: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("no initial snapshot")
			}
			// Let the listener finish its LISTEN before mutating.
			time.Sleep(300 * time.Millisecond)
			s0 := w.Current()
			before := s0.ShardMapGeneration
			mustExec(t, conn, `UPDATE pgshard.shard_map_generation SET generation = generation + 1`)
			select {
			case c := <-changes:
				if !tc.wantFast {
					t.Fatalf("periodic-only watcher observed change within 1s: %+v", c)
				}
				if c.ShardMapGeneration != before+1 || w.Current().ShardMapGeneration != before+1 {
					t.Fatalf("change %+v current %s", c, w.Current())
				}
			case <-time.After(time.Second):
				if tc.wantFast {
					t.Fatalf("shard_map_generation bump not observed within 1s (current %s)", w.Current())
				}
			}
			if tc.wantFast {
				mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('other')`)
				select {
				case c := <-changes:
					if c.DesiredGeneration <= s0.DesiredGeneration {
						t.Fatalf("desired generation did not advance: %+v", c)
					}
				case <-time.After(time.Second):
					t.Fatal("desired change not observed within 1s")
				}
			}
			cancel()
			<-done
		})
	}
}

// TestWatcherKeepsOneReloadConnection: a reload used to dial the catalog
// every time, so a router paid a TCP handshake, a TLS handshake and SCRAM
// before every snapshot -- on the periodic reload and again on every
// notification. It also has to survive that connection dying, since the
// catalog it points at can restart under it.
func TestWatcherKeepsOneReloadConnection(t *testing.T) {
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

	w := NewWatcher(dsn, Options{ReloadInterval: 200 * time.Millisecond, DisableListen: true, Logf: t.Logf})
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitUntil(t, 10*time.Second, "the first snapshot", func() bool { return w.Current() != nil })

	backends := func() int {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND application_name <> 'psql'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// Several reload periods: a watcher that dials per reload would leave
	// a trail of them, and one that keeps a connection stays at one.
	time.Sleep(time.Second)
	if n := backends(); n != 1 {
		t.Errorf("catalog has %d backends for the watcher, want the one it keeps", n)
	}

	// The catalog restarting under the connection must not strand it.
	first := w.Current().LoadedAt
	if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid() AND application_name <> 'psql'`); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 15*time.Second, "a snapshot after the connection was killed", func() bool {
		s := w.Current()
		return s != nil && s.LoadedAt.After(first)
	})
	if n := backends(); n != 1 {
		t.Errorf("after recovering the watcher holds %d backends, want one", n)
	}
	cancel()
	<-done
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
