package catalog

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestTheBackfillReadsTheSetOutOfStreamStatus: pgshard.streams never kept
// the set its slots were made on, but pgshard.stream_status is keyed by it
// and the monitor has been writing that all along. So the answer is on
// record for any stream that has ever been swept, and assuming 'default'
// for all of them would move a stream created on another set onto a set it
// has no slots on -- which is the thing this column exists to prevent.
func TestTheBackfillReadsTheSetOutOfStreamStatus(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH")
	}
	selected, err := selectImages(candidateImages, os.Getenv(requireProjectImagesEnv) != "", func(name string) bool { return imageAvailable(t, name) })
	if err != nil || len(selected) == 0 {
		t.Skipf("no PostgreSQL image available: %v", err)
	}
	ctx := context.Background()
	conn := connect(t, startPostgres(t, selected[0]))

	// Up to 0049 only: the state a live cluster is in before this column
	// exists, so the rows below are the ones it really would have.
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS pgshard;
		CREATE TABLE IF NOT EXISTS pgshard.schema_migrations (
			version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now(), checksum text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version >= 50 {
			break
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			t.Fatalf("migration %d: %v", m.Version, err)
		}
	}
	for _, q := range []string{
		`INSERT INTO pgshard.streams (name, database, two_phase, state) VALUES ('on_g2', 'app', false, 'active')`,
		`INSERT INTO pgshard.streams (name, database, two_phase, state) VALUES ('on_default', 'app', false, 'active')`,
		`INSERT INTO pgshard.streams (name, database, two_phase, state) VALUES ('never_swept', 'app', false, 'creating')`,
		// onG2's slots are on g2: that shard reported a real wal_status.
		`INSERT INTO pgshard.stream_status (stream, shard_set, shard_id, slot, wal_status) VALUES ('on_g2', 'g2', 0, 's', 'reserved')`,
		// and the cluster-wide sweep also left it a row on default, with
		// no slot behind it. That row must not win.
		`INSERT INTO pgshard.stream_status (stream, shard_set, shard_id, slot, wal_status) VALUES ('on_g2', 'default', 0, 's', 'missing')`,
		`INSERT INTO pgshard.stream_status (stream, shard_set, shard_id, slot, wal_status) VALUES ('on_default', 'default', 0, 's', 'reserved')`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ stream, want string }{
		{"on_g2", "g2"},
		{"on_default", "default"},
		{"never_swept", "default"},
	} {
		var got string
		if err := conn.QueryRow(ctx, `SELECT shard_set FROM pgshard.streams WHERE name = $1`, tc.stream).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s backfilled to %q, want %q: the monitor would then sweep a set it has no slots on", tc.stream, got, tc.want)
		}
	}
}
