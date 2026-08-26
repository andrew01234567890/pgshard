package catalog

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/placement"
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

func TestMigrationsFrom(t *testing.T) {
	gap := fstest.MapFS{"schema/0001_a.sql": {Data: []byte("a")}, "schema/0003_c.sql": {Data: []byte("c")}, "schema/notes.txt": {Data: []byte("x")}}
	ms, err := migrationsFrom(gap)
	if err != nil || len(ms) != 2 || ms[1].Version != 3 || ms[1].Name != "0003_c" {
		t.Fatalf("gap: %v %+v", err, ms)
	}
	dup := fstest.MapFS{"schema/0001_a.sql": {Data: []byte("a")}, "schema/0001_b.sql": {Data: []byte("b")}}
	if _, err := migrationsFrom(dup); err == nil {
		t.Fatal("duplicate versions accepted")
	}
	bad := fstest.MapFS{"schema/x.sql": {Data: []byte("a")}}
	if _, err := migrationsFrom(bad); err == nil {
		t.Fatal("bad name accepted")
	}
}

func TestCatalog(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; skipping catalog integration tests")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon unavailable; skipping catalog integration tests")
	}
	selected, err := selectImages(candidateImages, os.Getenv(requireProjectImagesEnv) != "", func(name string) bool { return imageAvailable(t, name) })
	if err != nil {
		t.Fatal(err)
	}
	for _, img := range selected {
		t.Run(img.label, func(t *testing.T) { runSuite(t, img) })
	}
}

// requireProjectImagesEnv makes the suite fail instead of silently falling back
// to Docker Hub images when a project image is missing for any major.
const requireProjectImagesEnv = "PGSHARD_REQUIRE_PROJECT_IMAGES"

func selectImages(candidates []pgImage, requireProject bool, available func(string) bool) ([]pgImage, error) {
	var selected []pgImage
	seen := map[string]bool{}
	for _, img := range candidates {
		if seen[img.label] {
			continue
		}
		if requireProject && !img.bare {
			return nil, fmt.Errorf("%s: project image for %s unavailable", requireProjectImagesEnv, img.label)
		}
		if !available(img.name) {
			continue
		}
		seen[img.label] = true
		selected = append(selected, img)
	}
	if requireProject {
		for _, img := range candidates {
			if img.bare && !seen[img.label] {
				return nil, fmt.Errorf("%s: project image for %s unavailable", requireProjectImagesEnv, img.label)
			}
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("no PostgreSQL image available")
	}
	return selected, nil
}

func TestSelectImages(t *testing.T) {
	avail := func(names ...string) func(string) bool {
		return func(n string) bool {
			for _, x := range names {
				if x == n {
					return true
				}
			}
			return false
		}
	}
	project18, project19 := candidateImages[0].name, candidateImages[2].name
	t.Run("fallback picks one image per major", func(t *testing.T) {
		got, err := selectImages(candidateImages, false, avail("postgres:18", "postgres:19beta2"))
		if err != nil || len(got) != 2 || got[0].name != "postgres:18" || got[1].name != "postgres:19beta2" {
			t.Fatalf("got %+v err %v", got, err)
		}
	})
	t.Run("fallback fails only with no image at all", func(t *testing.T) {
		if _, err := selectImages(candidateImages, false, avail()); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("require rejects a missing project major even when hub image exists", func(t *testing.T) {
		if _, err := selectImages(candidateImages, true, avail(project18, "postgres:19")); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("require accepts both project images", func(t *testing.T) {
		got, err := selectImages(candidateImages, true, avail(project18, project19))
		if err != nil || len(got) != 2 || !got[0].bare || !got[1].bare {
			t.Fatalf("got %+v err %v", got, err)
		}
	})
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

	t.Run("upgrade_refuses_a_mis_numbered_catalog", func(t *testing.T) {
		mustExec(t, conn, `CREATE DATABASE pgshard_pre_0020`)
		t.Cleanup(func() { mustExec(t, conn, `DROP DATABASE pgshard_pre_0020 WITH (FORCE)`) })
		old := connect(t, strings.Replace(dsn, "/postgres?", "/pgshard_pre_0020?", 1))

		ms, err := Migrations()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := old.Exec(ctx, `
			CREATE SCHEMA IF NOT EXISTS pgshard;
			CREATE TABLE IF NOT EXISTS pgshard.schema_migrations (
				version    integer     PRIMARY KEY,
				applied_at timestamptz NOT NULL DEFAULT now(),
				checksum   text        NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}
		var last Migration
		for _, m := range ms {
			if m.Version == 20 {
				last = m
				break
			}
			if err := applyMigration(ctx, old, m); err != nil {
				t.Fatalf("migration %d: %v", m.Version, err)
			}
		}
		if last.Version != 20 {
			t.Fatal("migration 20 not found")
		}

		// The exact state PGS-268 describes: full key coverage, IDs the router
		// never used.
		mustExec(t, old, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
			('default', 10, '[,0)'), ('default', 20, '[0,)')`)

		err = applyMigration(ctx, old, last)
		if err == nil {
			t.Fatal("migration 20 accepted a catalog whose shard IDs routing had been ignoring")
		}
		for _, want := range []string{"not numbered 0..N-1", "shard 10", "shard 20"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("migration error must name the offending set and IDs; got %v", err)
			}
		}
		var applied bool
		if err := old.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pgshard.schema_migrations WHERE version = 20)`).Scan(&applied); err != nil {
			t.Fatal(err)
		}
		if applied {
			t.Fatal("migration 20 recorded itself despite failing")
		}

		// Renumbering the ranges is not enough on its own: a workflow created
		// before the migration holds its own snapshot of the old IDs, and the
		// copier dials the shards that snapshot names.
		mustExec(t, old, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
			('33333333-3333-3333-3333-333333333333', 'reshard', 'running',
			 '{"shard_set": "default", "ranges": [{"shard_id": 10}, {"shard_id": 20}]}'::jsonb)`)
		mustExec(t, old, `UPDATE pgshard.shard_ranges SET shard_id = 0 WHERE shard_id = 10`)
		mustExec(t, old, `UPDATE pgshard.shard_ranges SET shard_id = 1 WHERE shard_id = 20`)

		err = applyMigration(ctx, old, last)
		if err == nil {
			t.Fatal("migration 20 accepted a repair that left an in-flight workflow addressing shards that no longer exist")
		}
		for _, want := range []string{"mis-numbered shard ranges", "default", "shard 10"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("migration error must name the offending workflow and IDs; got %v", err)
			}
		}

		mustExec(t, old, `UPDATE pgshard.workflows SET state = 'cancelled' WHERE id = '33333333-3333-3333-3333-333333333333'`)

		// A workflow from before the source was recorded cannot have its source
		// protected, because which set it built its replication against is not
		// recoverable. The paused arm has to be exercised here too.
		mustExec(t, old, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
			('ffffffff-3333-3333-3333-333333333333', 'reshard', 'paused',
			 '{"shard_set": "gsourceless", "ranges": [{"shard_id": 0}]}'::jsonb,
			 '{"paused_from": "running"}'::jsonb)`)
		err = applyMigration(ctx, old, last)
		if err == nil {
			t.Fatal("migration 20 accepted a workflow whose copy source cannot be determined")
		}
		if !strings.Contains(err.Error(), "have not recorded the shard set they copy from") {
			t.Fatalf("migration error must name the unrecorded source; got %v", err)
		}

		mustExec(t, old, `UPDATE pgshard.workflows SET state = 'cancelled' WHERE id = 'ffffffff-3333-3333-3333-333333333333'`)
		if err := applyMigration(ctx, old, last); err != nil {
			t.Fatalf("migration 20 rejected a renumbered catalog with no workflow in flight: %v", err)
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
				('src', 0, '[,0)'), ('src', 1, '[0,)'), ('dst', 0, '[,0)'), ('dst', 1, '[0,)')`)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			mustTx(t, tx, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'dst' AND shard_id = 1`)
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

		t.Run("shard_ids_must_be_key_ordinals", func(t *testing.T) {
			for name, values := range map[string]string{
				"not_dense":       `('sparse', 10, '[,0)'), ('sparse', 20, '[0,)')`,
				"wrong_key_order": `('unordered', 1, '[,0)'), ('unordered', 0, '[0,)')`,
			} {
				t.Run(name, func(t *testing.T) {
					tx, err := conn.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = tx.Rollback(ctx) }()
					mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES `+values)
					err = tx.Commit(ctx)
					if err == nil {
						t.Fatal("commit succeeded although routing would silently renumber the shards")
					}
					expectPgError(t, err, "23514", "0..N-1")
				})
			}
		})

		t.Run("frozen_while_a_workflow_owns_them", func(t *testing.T) {
			// The set is serving, not provisioning: a cutover flips it well
			// before its workflow finishes, and the workflow keeps the reverse
			// subscription open for the whole rollback window.
			mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gowned', 30, 'serving')`)
			mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
				('gowned', 0, '[,0)'), ('gowned', 1, '[0,)')`)
			mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
				('11111111-1111-1111-1111-111111111111', 'reshard', 'running',
				 '{"shard_set": "gowned", "source_set": "gownedsrc", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb)`)

			refused := func(t *testing.T, stmts ...string) {
				t.Helper()
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				for _, stmt := range stmts {
					mustTx(t, tx, stmt)
				}
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("ranges were rewritten under a workflow that had already snapshotted them")
				}
				expectPgError(t, err, "23514", "owned by workflow")
			}

			refusedOn := func(t *testing.T, set string) {
				t.Helper()
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = $1 AND shard_id = 1`, set)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ($1, 2, '[100,)')`, set)
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatalf("shard set %s was reshaped although a workflow owns it", set)
				}
				expectPgError(t, err, "23514", "owned by workflow")
			}

			t.Run("merge_refused", func(t *testing.T) {
				refused(t,
					`DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gowned' AND shard_id = 1`,
					`UPDATE pgshard.shard_ranges SET range = '[,)' WHERE shard_set = 'gowned' AND shard_id = 0`)
			})

			t.Run("split_refused", func(t *testing.T) {
				refused(t,
					`UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gowned' AND shard_id = 1`,
					`INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gowned', 2, '[100,)')`)
			})

			// Moving a range between sets always breaks the coverage of both,
			// so either guard may be the one that refuses it; what matters is
			// that a set a workflow owns cannot be emptied out from under it.
			t.Run("moving_a_range_out_refused", func(t *testing.T) {
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET shard_set = 'gelsewhere' WHERE shard_set = 'gowned' AND shard_id = 1`)
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("a range was moved out of a set a workflow owns")
				}
				expectPgError(t, err, "23514", "gowned")
			})

			t.Run("an_insert_alone_cannot_reshape_a_covered_set", func(t *testing.T) {
				_, err := conn.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gowned', 2, '[100,200)')`)
				if err == nil {
					t.Fatal("an insert into a set that already covers the key space must overlap")
				}
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "23P01" {
					t.Fatalf("expected an exclusion violation, got %v", err)
				}
			})

			// Clearing only the shard_sets row would otherwise be a way to
			// reshape the ranges and put the row back afterwards.
			t.Run("clearing_only_the_set_row_is_not_an_escape", func(t *testing.T) {
				refused(t,
					`DELETE FROM pgshard.shard_sets WHERE shard_set = 'gowned'`,
					`DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gowned' AND shard_id = 1`,
					`UPDATE pgshard.shard_ranges SET range = '[,)' WHERE shard_set = 'gowned' AND shard_id = 0`)
			})

			t.Run("dropping_the_whole_set_is_allowed", func(t *testing.T) {
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if err := DropShardSet(ctx, tx, "gowned"); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("cancelling a reshard by dropping its target set was refused: %v", err)
				}
			})

			// Editing a serving set directly creates a workflow nothing drives
			// and nothing can terminate, so treating it as an owner would leave
			// the set unusable for good.
			t.Run("a_pending_workflow_owns_nothing", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gpending', 32, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gpending', 0, '[,0)'), ('gpending', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
					('44444444-4444-4444-4444-444444444444', 'reshard', 'pending',
					 '{"shard_set": "gpending", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gpending'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gpending'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gpending' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gpending', 2, '[100,)')`)
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("a set whose only workflow is pending must stay editable: %v", err)
				}
			})

			// The cutover rebuilds the source shard IDs and ranges from live
			// rows on every pass, not from the workflow's snapshot.
			t.Run("the_source_set_is_owned_too", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gsource', 33, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gsource', 0, '[,0)'), ('gsource', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
					('55555555-5555-5555-5555-555555555555', 'reshard', 'running',
					 '{"shard_set": "gtarget", "source_set": "gsource", "ranges": [{"shard_id": 0}]}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = '55555555-5555-5555-5555-555555555555'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gsource'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gsource'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gsource' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gsource', 2, '[100,)')`)
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("the source of a running reshard was reshaped while the cutover still reads it")
				}
				expectPgError(t, err, "23514", "owned by workflow")
			})

			// A workflow created before the source was recorded in the spec
			// resolves it on its first cutover pass and keeps it in status.
			t.Run("a_source_recorded_only_in_status_is_owned", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('glegacy', 34, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('glegacy', 0, '[,0)'), ('glegacy', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
					('66666666-6666-6666-6666-666666666666', 'reshard', 'running',
					 '{"shard_set": "gtarget2", "ranges": [{"shard_id": 0}]}'::jsonb,
					 '{"cutover": {"source_set": "glegacy"}}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = '66666666-6666-6666-6666-666666666666'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'glegacy'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'glegacy'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'glegacy' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('glegacy', 2, '[100,)')`)
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("the source of a running reshard was reshaped because the spec did not name it")
				}
				expectPgError(t, err, "23514", "owned by workflow")
			})

			// Dropping the set and re-creating it reshaped under the same name
			// would otherwise put the workflow back where it started.
			t.Run("recreating_a_dropped_set_under_the_same_name_is_refused", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gremade', 35, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gremade', 0, '[,0)'), ('gremade', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
					('77777777-7777-7777-7777-777777777777', 'reshard', 'running',
					 '{"shard_set": "gremade", "source_set": "gremadesrc", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = '77777777-7777-7777-7777-777777777777'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gremade'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gremade'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if err := DropShardSet(ctx, tx, "gremade"); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("dropping the set was refused: %v", err)
				}

				tx, err = conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gremade', 35, 'serving')`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gremade', 0, '[,)')`)
				err = tx.Commit(ctx)
				if err == nil {
					t.Fatal("a set its workflow still owns was re-created reshaped under the same name")
				}
				expectPgError(t, err, "23514", "owned by workflow")
			})

			// Pausing the workflow an ordinary edit creates must not freeze a
			// set that was editable a moment earlier.
			t.Run("pausing_a_pending_workflow_owns_nothing", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gpaused', 36, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gpaused', 0, '[,0)'), ('gpaused', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
					('88888888-8888-8888-8888-888888888888', 'reshard', 'paused',
					 '{"shard_set": "gpaused", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb,
					 '{"paused_from": "pending"}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = '88888888-8888-8888-8888-888888888888'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gpaused'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gpaused'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gpaused' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gpaused', 2, '[100,)')`)
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("pausing a workflow that never started froze the set: %v", err)
				}
			})

			// Resume restores a paused workflow to paused_from, or to pending
			// when the marker is missing, so an unmarked pause is inert too.
			t.Run("a_pause_with_no_marker_owns_nothing", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gunmarked', 37, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gunmarked', 0, '[,0)'), ('gunmarked', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
					('cccccccc-9999-9999-9999-999999999999', 'reshard', 'paused',
					 '{"shard_set": "gunmarked", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb, '{}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = 'cccccccc-9999-9999-9999-999999999999'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gunmarked'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gunmarked'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gunmarked' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gunmarked', 2, '[100,)')`)
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("a pause that resumes to pending froze the set: %v", err)
				}
			})

			// The paused arm has to refuse as well as release, or deleting it
			// entirely would still pass.
			t.Run("a_pause_from_running_still_owns_the_set", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gpr', 39, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gpr', 0, '[,0)'), ('gpr', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
					('eeeeeeee-9999-9999-9999-999999999999', 'reshard', 'paused',
					 '{"shard_set": "gpr", "source_set": "gprsrc", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb,
					 '{"paused_from": "running"}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE id = 'eeeeeeee-9999-9999-9999-999999999999'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gpr'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gpr'`)
				})
				refusedOn(t, "gpr")
			})

			t.Run("a_finished_workflow_releases_the_set", func(t *testing.T) {
				mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('gdone', 31, 'serving')`)
				mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
					('gdone', 0, '[,0)'), ('gdone', 1, '[0,)')`)
				mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec) VALUES
					('22222222-2222-2222-2222-222222222222', 'reshard', 'completed',
					 '{"shard_set": "gdone", "ranges": [{"shard_id": 0}, {"shard_id": 1}]}'::jsonb)`)
				t.Cleanup(func() {
					mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gdone'`)
					mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gdone'`)
				})
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				mustTx(t, tx, `UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'gdone' AND shard_id = 1`)
				mustTx(t, tx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gdone', 2, '[100,)')`)
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("a set whose workflow has finished must be editable again: %v", err)
				}
			})
		})

		// A reconcile pass covers every shard set in one transaction, so
		// waiting on a held row lock holds up work unrelated to the set being
		// locked.
		t.Run("locking_ranges_is_bounded", func(t *testing.T) {
			holder := connect(t, dsn)
			held, err := holder.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Rollback(ctx) }()
			mustTx(t, held, `UPDATE pgshard.shard_ranges SET updated_at = now() WHERE shard_set = 'default'`)

			waiter := connect(t, dsn)
			wtx, err := waiter.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = wtx.Rollback(ctx) }()

			started := time.Now()
			err = LockShardRangesOf(ctx, wtx, "default")
			if err == nil {
				t.Fatal("locking ranges another transaction holds succeeded")
			}
			expectPgError(t, err, "55P03", "")
			if elapsed := time.Since(started); elapsed > 30*time.Second {
				t.Fatalf("waited %s for a lock that must be bounded", elapsed)
			}
		})

		// Row-level constraint triggers do not fire on TRUNCATE, so it would
		// empty the shard map past every check that guards it.
		t.Run("truncate_refused", func(t *testing.T) {
			for _, table := range []string{"pgshard.shard_ranges", "pgshard.shard_sets"} {
				t.Run(table, func(t *testing.T) {
					_, err := conn.Exec(ctx, `TRUNCATE `+table)
					if err == nil {
						t.Fatalf("TRUNCATE %s emptied the shard map with no validation", table)
					}
					expectPgError(t, err, "0A000", "not supported")
				})
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

	t.Run("rewrite_migration_round_trips_and_lists_as_pending", func(t *testing.T) {
		rw := &RewriteChange{Schema: "public", Table: "orders", Column: "amount", NewType: "bigint",
			Using: "amount::bigint", BatchSize: 500}
		id, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "app", Statement: "alter table orders alter column amount type bigint",
			Kind: "ALTER TABLE", Strategy: StrategyRewrite, Scope: "all", Meta: MigrationMeta{RunAs: "app", Rewrite: rw}})
		if err != nil {
			t.Fatal(err)
		}
		m, err := LoadMigration(ctx, conn, id)
		if err != nil {
			t.Fatal(err)
		}
		if m.Strategy != StrategyRewrite || m.Meta.Rewrite == nil || !reflect.DeepEqual(m.Meta.Rewrite, rw) {
			t.Fatalf("round trip %+v", m)
		}
		if _, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "app", Statement: "x", Kind: "ALTER TABLE", Strategy: StrategyRewrite, Scope: "all"}); err == nil {
			t.Fatal("a rewrite migration without meta.rewrite must be refused")
		}
		pending, err := PendingRewrites(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range pending {
			if p.ID == id && p.Database == "app" && p.Rewrite.Column == "amount" {
				found = true
				if got := p.Rewrite.HiddenColumn(p.ID); !strings.HasPrefix(got, "_pgshard_amount_") || len(got) != len("_pgshard_amount_")+8 {
					t.Fatalf("hidden column %q", got)
				}
			}
		}
		if !found {
			t.Fatalf("pending rewrites: %+v", pending)
		}
		m.Meta.Rewrite.Columns = []string{"id", "amount"}
		if err := SaveMigrationMeta(ctx, conn, id, m.Meta); err != nil {
			t.Fatal(err)
		}
		again, err := LoadMigration(ctx, conn, id)
		if err != nil || len(again.Meta.Rewrite.Columns) != 2 {
			t.Fatalf("meta not saved: %+v %v", again.Meta.Rewrite, err)
		}
		m.State = MigrationComplete
		if err := SaveMigrationProgress(ctx, conn, m); err != nil {
			t.Fatal(err)
		}
		pending, err = PendingRewrites(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pending {
			if p.ID == id {
				t.Fatal("a complete rewrite must not be pending")
			}
		}
	})

	t.Run("repack_migration_round_trips_as_concurrent", func(t *testing.T) {
		id, err := EnqueueMigration(ctx, conn, DDLMigration{Database: "app", Statement: "vacuum (full) orders",
			Kind: "VACUUM", Strategy: StrategyRepack, Scope: "all",
			Meta: MigrationMeta{Object: MigrationObject{Kind: "relation", Name: "orders", Expect: "present"}}})
		if err != nil {
			t.Fatal(err)
		}
		m, err := LoadMigration(ctx, conn, id)
		if err != nil {
			t.Fatal(err)
		}
		if m.Strategy != StrategyRepack || !m.Meta.Repack {
			t.Fatalf("round trip %+v", m)
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

	t.Run("shard_sets", func(t *testing.T) {
		sets, err := ListShardSets(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(sets) == 0 || sets[0].Name != DefaultShardSet || sets[0].Generation != 1 || sets[0].State != ShardSetServing {
			t.Fatalf("default set must be serving generation 1: %+v", sets)
		}
		before := len(sets)
		admin := connect(t, dsn)
		mustExec(t, admin, `SET ROLE `+RoleAdmin)
		mustExec(t, admin, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('gnew', 0, '[,0)'), ('gnew', 1, '[0,)')`)
		sets, err = ListShardSets(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		last := sets[len(sets)-1]
		if len(sets) != before+1 || last.Name != "gnew" || last.State != ShardSetDesired || last.Generation <= sets[len(sets)-2].Generation || last.DesiredGeneration == 0 {
			t.Fatalf("inserting ranges into a new set must register it as desired with the next generation: %+v", sets)
		}
		_, err = admin.Exec(ctx, `UPDATE pgshard.shard_sets SET state = 'bogus' WHERE shard_set = 'gnew'`)
		expectPgError(t, err, "23514", "state")
		_, err = admin.Exec(ctx, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('dup', $1, 'desired')`, last.Generation)
		expectPgError(t, err, "23505", "generation")

		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ranges, _ := placement.Split(4)
		if err := MaterializeShardSet(ctx, tx, "g7", 7, ShardSetDesired, ranges); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		rows, err := ListShardRanges(ctx, conn, "g7")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 4 || rows[0].Lower != nil || rows[3].Upper != nil || *rows[1].Lower != ranges[1].Start || *rows[1].Upper != ranges[1].End+1 {
			t.Fatalf("materialized ranges round-trip: %+v", rows)
		}
		if got := RangeSet(rows); got.Validate() != nil || got[2] != ranges[2] {
			t.Fatalf("RangeSet: %v", got)
		}
		tx, _ = conn.Begin(ctx)
		if err := MaterializeShardSet(ctx, tx, "g7", 7, ShardSetDesired, ranges); err == nil {
			t.Fatal("materializing an existing set must fail")
		}
		_ = tx.Rollback(ctx)
		tx, _ = conn.Begin(ctx)
		if err := DropShardSet(ctx, tx, "g7"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_ranges WHERE shard_set = 'g7'`); n != 0 {
			t.Fatalf("%d ranges left after drop", n)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_sets WHERE shard_set = 'g7'`); n != 0 {
			t.Fatalf("%d sets left after drop", n)
		}
		mustExec(t, conn, `DELETE FROM pgshard.shard_ranges WHERE shard_set = 'gnew'`)
		mustExec(t, conn, `DELETE FROM pgshard.shard_sets WHERE shard_set = 'gnew'`)
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

	t.Run("stream_status_slot_health", func(t *testing.T) {
		if err := CreateStream(ctx, conn, Stream{Name: "health", Database: "app"}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = DeleteStream(ctx, conn, "health") })
		want := StreamStatus{Stream: "health", ShardSet: "default", ShardID: 3, Slot: "pgshard_health_shard3", WALStatus: "reserved",
			ConfirmedFlushLSN: 0x100000020, RestartLSN: 0x100000010, RetainedBytes: 123456, Active: true, Synced: true, Failover: true}
		if err := UpsertStreamStatus(ctx, conn, want); err != nil {
			t.Fatal(err)
		}
		rows, err := ListStreamStatus(ctx, conn, "health")
		if err != nil || len(rows) != 1 {
			t.Fatalf("rows %+v %v", rows, err)
		}
		got := rows[0]
		if got.UpdatedAt.IsZero() {
			t.Fatal("updated_at not read")
		}
		got.UpdatedAt = time.Time{}
		if got != want {
			t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
		}
		want.RetainedBytes, want.Synced, want.Failover = 0, false, false
		if err := UpsertStreamStatus(ctx, conn, want); err != nil {
			t.Fatal(err)
		}
		rows, _ = ListStreamStatus(ctx, conn, "health")
		if rows[0].RetainedBytes != 0 || rows[0].Synced || rows[0].Failover {
			t.Fatalf("upsert must overwrite slot health: %+v", rows[0])
		}
	})
}

func queryOne[T any](t *testing.T, conn *pgx.Conn, sql string, args ...any) T {
	t.Helper()
	var v T
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

func mustTx(t *testing.T, tx pgx.Tx, sql string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
