package catalog

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestAVanishedSlotMarksTheStreamLost: the monitor writes wal_status
// "missing" when pg_replication_slots has no row for a stream's slot, which
// is what a dropped slot or a promotion to a member it was never
// synchronised to leaves behind. Only "lost" used to move the stream's
// state, so a stream with no slot left stayed "active".
//
// And the exemption that makes it safe: a stream still being created has no
// slots yet, and the sweep reports every one of them missing until
// CreateStream has made them.
func TestAVanishedSlotMarksTheStreamLost(t *testing.T) {
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
	state := func(name string) string {
		var s string
		if err := conn.QueryRow(ctx, `SELECT state FROM pgshard.streams WHERE name = $1`, name).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	if err := CreateStream(ctx, conn, Stream{Name: "live", Database: "app"}); err != nil {
		t.Fatal(err)
	}
	if err := SetStreamState(ctx, conn, "live", StreamActive); err != nil {
		t.Fatal(err)
	}
	if err := UpsertStreamStatus(ctx, conn, StreamStatus{Stream: "live", ShardSet: "default", ShardID: 0, Slot: "s0", WALStatus: "missing"}); err != nil {
		t.Fatal(err)
	}
	if got := state("live"); got != StreamLost {
		t.Errorf("a stream whose slot has vanished is %q, want %q: nothing tells the consumer to re-baseline", got, StreamLost)
	}

	// Still creating: its slots are not supposed to exist yet.
	if err := CreateStream(ctx, conn, Stream{Name: "new", Database: "app"}); err != nil {
		t.Fatal(err)
	}
	if got := state("new"); got != StreamCreating {
		t.Fatalf("fixture stream is %q, want %q", got, StreamCreating)
	}
	if err := UpsertStreamStatus(ctx, conn, StreamStatus{Stream: "new", ShardSet: "default", ShardID: 0, Slot: "s0", WALStatus: "missing"}); err != nil {
		t.Fatal(err)
	}
	if got := state("new"); got != StreamCreating {
		t.Errorf("a stream still being created was marked %q for slots CreateStream has not made yet", got)
	}
}
