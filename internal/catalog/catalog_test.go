package catalog

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type pgImage struct {
	name  string
	label string
	bare  bool
}

var candidateImages = []pgImage{
	{name: "ghcr.io/andrew01234567890/pgshard-postgres:18", label: "pg18", bare: true},
	{name: "postgres:18", label: "pg18"},
	{name: "ghcr.io/andrew01234567890/pgshard-postgres:19", label: "pg19", bare: true},
	{name: "postgres:19", label: "pg19"},
	{name: "postgres:19beta2", label: "pg19"},
}

func TestMigrations(t *testing.T) {
	ms, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) < 3 || ms[0].Version != 1 || ms[0].Name != "0001_roles_and_schema" {
		t.Fatalf("unexpected migrations: %+v", ms)
	}
}

func TestCatalog(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; skipping catalog integration tests")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon unavailable; skipping catalog integration tests")
	}
	seen := map[string]bool{}
	ran := 0
	for _, img := range candidateImages {
		if seen[img.label] || !imageAvailable(t, img.name) {
			continue
		}
		seen[img.label] = true
		ran++
		t.Run(img.label, func(t *testing.T) { runSuite(t, img) })
	}
	if ran == 0 {
		t.Fatal("no PostgreSQL image available")
	}
}

func imageAvailable(t *testing.T, name string) bool {
	t.Helper()
	if exec.Command("docker", "image", "inspect", name).Run() == nil {
		return true
	}
	out, err := exec.Command("docker", "pull", name).CombinedOutput()
	if err != nil {
		t.Logf("image %s unavailable: %v: %s", name, err, strings.TrimSpace(string(out)))
		return false
	}
	return true
}

func startPostgres(t *testing.T, img pgImage) string {
	t.Helper()
	port := freePort(t)
	args := []string{"run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port)}
	if img.bare {
		args = append(args, "--entrypoint", "sh", img.name, "-ec",
			`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
			 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
			 exec postgres -D /tmp/pgdata -c listen_addresses='*'`)
	} else {
		args = append(args, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", img.name)
	}
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v: %s", img.name, err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })

	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			_ = conn.Close(ctx)
			cancel()
			return dsn
		}
		cancel()
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
	t.Fatalf("postgres in %s did not become ready:\n%s", img.name, logs)
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func expectPgError(t *testing.T, err error, code string, substr string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected PostgreSQL error %s, got %v", code, err)
	}
	if pgErr.Code != code {
		t.Fatalf("expected SQLSTATE %s, got %s: %s", code, pgErr.Code, pgErr.Message)
	}
	if substr != "" && !strings.Contains(pgErr.Message, substr) {
		t.Fatalf("expected message containing %q, got %q", substr, pgErr.Message)
	}
}

func runSuite(t *testing.T, img pgImage) {
	dsn := startPostgres(t, img)
	ctx := context.Background()
	conn := connect(t, dsn)

	t.Run("migrate_twice", func(t *testing.T) {
		if err := Migrate(ctx, conn); err != nil {
			t.Fatal(err)
		}
		if err := Migrate(ctx, conn); err != nil {
			t.Fatalf("second Migrate: %v", err)
		}
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pgshard.schema_migrations`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		ms, _ := Migrations()
		if n != len(ms) {
			t.Fatalf("schema_migrations has %d rows, want %d", n, len(ms))
		}
		var owner string
		if err := conn.QueryRow(ctx, `SELECT tableowner FROM pg_tables WHERE schemaname = 'pgshard' AND tablename = 'tables'`).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if owner != RoleSystem {
			t.Fatalf("pgshard.tables owned by %s, want %s", owner, RoleSystem)
		}
	})

	t.Run("checksum_tamper", func(t *testing.T) {
		mustExec(t, conn, `UPDATE pgshard.schema_migrations SET checksum = 'tampered' WHERE version = 2`)
		t.Cleanup(func() {
			ms, _ := Migrations()
			mustExec(t, conn, `UPDATE pgshard.schema_migrations SET checksum = $1 WHERE version = 2`, ms[1].Checksum)
		})
		err := Migrate(ctx, conn)
		if !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("expected ErrChecksumMismatch, got %v", err)
		}
	})

	t.Run("shard_ranges", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
			('default', 0, '[,0)'), ('default', 1, '[0,)')`)
		got, err := ListShardRanges(ctx, conn, "default")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Lower != nil || *got[0].Upper != 0 || *got[1].Lower != 0 || got[1].Upper != nil {
			t.Fatalf("unexpected ranges %+v", got)
		}
		if got[0].DesiredGeneration == 0 || got[1].DesiredGeneration <= got[0].DesiredGeneration {
			t.Fatalf("desired_generation not stamped: %+v", got)
		}

		cases := map[string]string{
			"gap":         `('gap', 0, '[,0)'), ('gap', 1, '[10,)')`,
			"overlap":     `('ov', 0, '[,10)'), ('ov', 1, '[0,)')`,
			"open_bottom": `('ob', 0, '[0,)')`,
			"open_top":    `('ot', 0, '[,0)')`,
		}
		for name, values := range cases {
			t.Run(name, func(t *testing.T) {
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if _, err := tx.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES `+values); err != nil {
					t.Fatalf("insert must succeed before commit: %v", err)
				}
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("commit succeeded with invalid ranges")
				}
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || (pgErr.Code != "23514" && pgErr.Code != "23P01") {
					t.Fatalf("expected check/exclusion violation, got %v", err)
				}
			})
		}

		t.Run("move_between_shard_sets_checks_source", func(t *testing.T) {
			mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
				('src', 0, '[,0)'), ('src', 1, '[0,)'), ('dst', 0, '[,0)'), ('dst', 9, '[0,)')`)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			mustTx(t, tx, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'dst' AND shard_id = 9`)
			mustTx(t, tx, `UPDATE pgshard.shard_ranges SET shard_set = 'dst', shard_id = 1 WHERE shard_set = 'src' AND shard_id = 1`)
			err = tx.Commit(ctx)
			if err == nil {
				t.Fatal("commit succeeded although the source shard set was left without its top range")
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" || !strings.Contains(pgErr.Message, "src") {
				t.Fatalf("expected check violation naming the source shard set, got %v", err)
			}
		})

		t.Run("negative_shard_id_rejected", func(t *testing.T) {
			_, err := conn.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('neg', -1, '[,)')`)
			expectPgError(t, err, "23514", "shard_id")
		})

		t.Run("split_in_one_transaction", func(t *testing.T) {
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'default' AND shard_id = 1`)
			mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 2, '[100,)')`)
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("valid split rejected: %v", err)
			}
		})
	})

	t.Run("tables_constraints", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
		_, err := conn.Exec(ctx, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, hash_version)
			VALUES ('app', 'public', 'orders', 'reference', 99)`)
		expectPgError(t, err, "23503", "tables_hash_version_fkey")

		_, err = conn.Exec(ctx, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement)
			VALUES ('app', 'public', 'orders', 'sharded')`)
		expectPgError(t, err, "23514", "sharded_tables_need_shard_key")

		mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
			VALUES ('app', 'public', 'orders', 'sharded', 'customer_id')`)
		tables, err := ListTables(ctx, conn, "app")
		if err != nil {
			t.Fatal(err)
		}
		if len(tables) != 1 || *tables[0].ShardKey != "customer_id" || tables[0].DesiredGeneration == 0 {
			t.Fatalf("unexpected tables %+v", tables)
		}
	})

	t.Run("notify_on_desired_change", func(t *testing.T) {
		listener := connect(t, dsn)
		mustExec(t, listener, `LISTEN `+DesiredChannel)

		mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement)
			VALUES ('app', 'public', 'countries', 'reference')`)
		var gen int64
		if err := conn.QueryRow(ctx, `SELECT desired_generation FROM pgshard.tables WHERE table_name = 'countries'`).Scan(&gen); err != nil {
			t.Fatal(err)
		}

		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		n, err := listener.WaitForNotification(wctx)
		if err != nil {
			t.Fatal(err)
		}
		if n.Channel != DesiredChannel || n.Payload != fmt.Sprintf("tables:%d", gen) {
			t.Fatalf("unexpected notification %+v (want tables:%d)", n, gen)
		}
	})

	t.Run("admin_cannot_write_status", func(t *testing.T) {
		admin := connect(t, dsn)
		mustExec(t, admin, `SET ROLE `+RoleAdmin)
		_, err := admin.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state)
			VALUES ('default', 0, 'g0', 'serving')`)
		expectPgError(t, err, "42501", "shard_status")
		_, err = admin.Exec(ctx, `UPDATE pgshard.shard_map_generation SET generation = 5`)
		expectPgError(t, err, "42501", "shard_map_generation")
		if _, err := admin.Exec(ctx, `SELECT * FROM pgshard.shard_status`); err != nil {
			t.Fatalf("admin must be able to read status: %v", err)
		}
		mustExec(t, admin, `INSERT INTO pgshard.databases (name) VALUES ('by_admin')`)

		reader := connect(t, dsn)
		mustExec(t, reader, `SET ROLE `+RoleReader)
		_, err = reader.Exec(ctx, `INSERT INTO pgshard.databases (name) VALUES ('by_reader')`)
		expectPgError(t, err, "42501", "databases")
		mustExec(t, admin, `INSERT INTO pgshard.roles (rolname, verifier) VALUES ('app', 'SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy')`)
		if _, err := reader.Exec(ctx, `SELECT rolname, attributes FROM pgshard.roles`); err != nil {
			t.Fatalf("reader must be able to list roles without verifiers: %v", err)
		}
		_, err = reader.Exec(ctx, `SELECT verifier FROM pgshard.roles`)
		expectPgError(t, err, "42501", "roles")
		_, err = reader.Exec(ctx, `SELECT * FROM pgshard.roles`)
		expectPgError(t, err, "42501", "roles")
	})

	t.Run("status_read_api", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch)
			VALUES ('default', 0, 'g0', 'serving', 3)`)
		st, err := ListShardStatus(ctx, conn, "default")
		if err != nil {
			t.Fatal(err)
		}
		if len(st) != 1 || st[0].PrimaryEpoch != 3 || st[0].GroupName != "g0" {
			t.Fatalf("unexpected shard status %+v", st)
		}
		mustExec(t, conn, `INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement)
			VALUES ('app', 'public', 'orders', 'sharded')`)
		ts, err := ListTableStatus(ctx, conn, "app")
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) != 1 || *ts[0].EffectivePlacement != "sharded" || string(ts[0].Progress) != "{}" {
			t.Fatalf("unexpected table status %+v", ts)
		}
		dbs, err := ListDatabases(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(dbs) != 2 || dbs[0].Name != "app" {
			t.Fatalf("unexpected databases %+v", dbs)
		}
	})

	t.Run("multistep_migration_round_trips_through_meta", func(t *testing.T) {
		steps := []MigrationStep{
			{SQL: "ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0) NOT VALID", Skip: MigrationCheck{Kind: "constraint", Table: "t", Name: "c"}},
			{SQL: "ALTER TABLE t VALIDATE CONSTRAINT c", Skip: MigrationCheck{Kind: "constraint_valid", Table: "t", Name: "c"}, OnFail: "ALTER TABLE t DROP CONSTRAINT IF EXISTS c"},
		}
		id, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "app", Statement: "alter table t add check (x > 0)", Kind: "ALTER TABLE",
			Strategy: StrategyMultistep, Scope: "all", Meta: MigrationMeta{RunAs: "app", Steps: steps}})
		if err != nil {
			t.Fatal(err)
		}
		m, err := LoadMigration(ctx, conn, id)
		if err != nil {
			t.Fatal(err)
		}
		if m.Strategy != StrategyMultistep || len(m.Meta.Steps) != 2 || m.Meta.Steps[1].OnFail != steps[1].OnFail || m.Meta.Steps[0].Skip != steps[0].Skip {
			t.Fatalf("round trip %+v", m)
		}
		m.State, m.PerShard = MigrationRunning, map[string]ShardMigration{"0": {State: ShardRunning, Attempts: 1, Step: 1}}
		if err := SaveMigrationProgress(ctx, conn, m); err != nil {
			t.Fatal(err)
		}
		if m, err = LoadMigration(ctx, conn, id); err != nil || m.PerShard["0"].Step != 1 {
			t.Fatalf("step did not round-trip: %+v %v", m.PerShard, err)
		}
		if _, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "app", Statement: "x", Kind: "ALTER TABLE", Strategy: StrategyMultistep, Scope: "all"}); err == nil {
			t.Fatal("multistep without steps was accepted")
		}
	})

	t.Run("list_and_count_migrations", func(t *testing.T) {
		failedID, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "other", Statement: "create table z (id int)", Kind: "CREATE TABLE", Strategy: "direct", Scope: "all"})
		if err != nil {
			t.Fatal(err)
		}
		failed, _ := LoadMigration(ctx, conn, failedID)
		failed.State, failed.Error = MigrationFailed, "boom"
		if err := SaveMigrationProgress(ctx, conn, failed); err != nil {
			t.Fatal(err)
		}
		all, total, err := ListMigrations(ctx, conn, MigrationFilter{})
		if err != nil || total < 2 || len(all) != total || all[0].ID != failedID || all[0].FinishedAt == nil {
			t.Fatalf("list all: total=%d len=%d first=%v err=%v", total, len(all), all, err)
		}
		page, total2, err := ListMigrations(ctx, conn, MigrationFilter{Limit: 1, Offset: 1})
		if err != nil || total2 != total || len(page) != 1 || page[0].ID == failedID {
			t.Fatalf("paging: %v %d %v", page, total2, err)
		}
		byDB, _, err := ListMigrations(ctx, conn, MigrationFilter{Database: "other", State: MigrationFailed})
		if err != nil || len(byDB) != 1 || byDB[0].ID != failedID {
			t.Fatalf("filter: %v %v", byDB, err)
		}
		counts, err := CountMigrations(ctx, conn)
		if err != nil || counts.Failed != 1 || counts.Running != 1 {
			t.Fatalf("counts: %+v %v", counts, err)
		}
	})

	t.Run("sequence_blocks", func(t *testing.T) {
		mustExec(t, conn, `UPDATE pgshard.tables SET sequence_columns = '{id}' WHERE database = 'app' AND table_name = 'orders'`)
		tables, err := ListTables(ctx, conn, "app")
		if err != nil {
			t.Fatal(err)
		}
		var orders *Table
		for i := range tables {
			if tables[i].TableName == "orders" {
				orders = &tables[i]
			}
		}
		if orders == nil || len(orders.SequenceColumns) != 1 || orders.SequenceColumns[0] != "id" {
			t.Fatalf("sequence_columns did not round-trip: %+v", tables)
		}
		var start, end int64
		if err := conn.QueryRow(ctx, `SELECT * FROM pgshard.allocate_sequence_block('app.public.orders.id')`).Scan(&start, &end); err != nil {
			t.Fatal(err)
		}
		if start != 1 || end != 1000 {
			t.Fatalf("first block [%d, %d], want [1, 1000] (auto-created row, default block_size)", start, end)
		}
		if err := conn.QueryRow(ctx, `SELECT * FROM pgshard.allocate_sequence_block('app.public.orders.id', 5)`).Scan(&start, &end); err != nil {
			t.Fatal(err)
		}
		if start != 1001 || end != 1005 {
			t.Fatalf("second block [%d, %d], want [1001, 1005]", start, end)
		}
		_, err = conn.Exec(ctx, `SELECT * FROM pgshard.allocate_sequence_block('app.public.orders.id', 0)`)
		expectPgError(t, err, "22023", "block size")
		names, err := ListSequenceNames(ctx, conn)
		if err != nil || len(names) != 1 || names[0] != "app.public.orders.id" {
			t.Fatalf("sequence names %v %v", names, err)
		}
		// Concurrent callers get disjoint blocks.
		const workers, per = 8, 25
		type block struct{ start, end int64 }
		results := make(chan block, workers*per)
		errs := make(chan error, workers)
		for w := 0; w < workers; w++ {
			go func() {
				c, err := pgx.Connect(ctx, dsn)
				if err != nil {
					errs <- err
					return
				}
				defer func() { _ = c.Close(ctx) }()
				for i := 0; i < per; i++ {
					var b block
					if err := c.QueryRow(ctx, `SELECT * FROM pgshard.allocate_sequence_block('app.public.orders.id', 3)`).Scan(&b.start, &b.end); err != nil {
						errs <- err
						return
					}
					results <- b
				}
				errs <- nil
			}()
		}
		for w := 0; w < workers; w++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		close(results)
		seen := map[int64]bool{}
		for b := range results {
			if b.end-b.start != 2 {
				t.Fatalf("block %+v is not 3 wide", b)
			}
			for v := b.start; v <= b.end; v++ {
				if seen[v] {
					t.Fatalf("value %d handed out twice", v)
				}
				seen[v] = true
			}
		}
		if len(seen) != workers*per*3 {
			t.Fatalf("%d distinct values, want %d", len(seen), workers*per*3)
		}
		admin := connect(t, dsn)
		mustExec(t, admin, `SET ROLE `+RoleAdmin)
		if err := admin.QueryRow(ctx, `SELECT * FROM pgshard.allocate_sequence_block('by_admin')`).Scan(&start, &end); err != nil {
			t.Fatalf("admin must be able to allocate: %v", err)
		}
		mustExec(t, admin, `INSERT INTO pgshard.sequences (name, next_value, block_size) VALUES ('declared', 100, 10)`)
		if err := admin.QueryRow(ctx, `SELECT * FROM pgshard.allocate_sequence_block('declared')`).Scan(&start, &end); err != nil || start != 100 || end != 109 {
			t.Fatalf("declared sequence block [%d, %d] %v, want [100, 109]", start, end, err)
		}
		reader := connect(t, dsn)
		mustExec(t, reader, `SET ROLE `+RoleReader)
		_, err = reader.Exec(ctx, `SELECT * FROM pgshard.allocate_sequence_block('by_reader')`)
		expectPgError(t, err, "42501", "allocate_sequence_block")
	})
}

func mustTx(t *testing.T, tx pgx.Tx, sql string) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
