package catalog

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestStreamTablesAreReadOnlyToAdmin: status tables are the control plane's
// to write and everyone else's to read, and stream_status was the one that
// missed the rule -- 0009 granted it in the same statement as
// pgshard.streams, which an administrator does declare streams in.
//
// Nothing routes or fences on these rows, so this is monitoring integrity
// rather than control: with DML an administrator could report a stream as
// active and caught up while its slot was invalidated, or hide the WAL its
// slots are pinning.
func TestStreamTablesAreReadOnlyToAdmin(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH")
	}
	selected, err := selectImages(candidateImages, os.Getenv(requireProjectImagesEnv) != "", func(name string) bool { return imageAvailable(t, name) })
	if err != nil || len(selected) == 0 {
		t.Skipf("no PostgreSQL image available: %v", err)
	}
	ctx := context.Background()
	conn := connect(t, startPostgres(t, selected[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}

	// Reading is the point of the table: the admin UI reports slot health
	// from it, so taking SELECT away would break the console.
	var canRead bool
	if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, 'pgshard.stream_status', 'SELECT')`, RoleAdmin).Scan(&canRead); err != nil {
		t.Fatal(err)
	}
	if !canRead {
		t.Fatal("pgshard_admin cannot read stream_status; the admin UI reports stream health from it")
	}
	for _, priv := range []string{"INSERT", "UPDATE", "DELETE"} {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, 'pgshard.stream_status', $2)`, RoleAdmin, priv).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held {
			t.Errorf("pgshard_admin holds %s on stream_status: slot state is the controller's to report, and a console that can be written to reports whatever it is told", priv)
		}
	}
	// 0009 re-granted BOTH tables, and pgshard.streams is the half that
	// decides the console's "lost" verdict on its own, so leaving it
	// writable would have closed the smaller door and cemented the larger
	// one open. Streams are declared through the controller's CreateStream
	// RPC, not by writing this table.
	for _, priv := range []string{"INSERT", "UPDATE", "DELETE"} {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, 'pgshard.streams', $2)`, RoleAdmin, priv).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held {
			t.Errorf("pgshard_admin holds %s on pgshard.streams: its state column alone decides whether the console calls a stream lost", priv)
		}
	}
	var canReadStreams bool
	if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, 'pgshard.streams', 'SELECT')`, RoleAdmin).Scan(&canReadStreams); err != nil {
		t.Fatal(err)
	}
	if !canReadStreams {
		t.Fatal("pgshard_admin cannot read pgshard.streams; the console lists streams from it")
	}
}
