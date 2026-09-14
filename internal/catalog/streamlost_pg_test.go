package catalog

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// TestAVanishedSlotMarksTheStreamLost. The monitor writes wal_status
// "missing" when pg_replication_slots has no row for a slot -- a slot
// dropped, or one that did not survive a promotion to a member it was
// never synchronised to. Only PostgreSQL's "lost" used to move the
// stream's state, so a stream with no slot left stayed active and nothing
// told the consumer to re-baseline.
//
// Two consecutive sightings, not one: a -rw flip mid-failover can land a
// sweep on a member whose synced copy of the slot has not caught up, and
// that clears by the next tick. Lost is not reversible, so a glimpse is
// not enough.
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
	active := func(name string) {
		t.Helper()
		if err := CreateStream(ctx, conn, Stream{Name: name, Database: "app", ShardSet: "default"}); err != nil {
			t.Fatal(err)
		}
		if err := SetStreamState(ctx, conn, name, StreamActive); err != nil {
			t.Fatal(err)
		}
	}
	sweep := func(name, walStatus string) {
		t.Helper()
		if err := UpsertStreamStatus(ctx, conn, StreamStatus{
			Stream: name, ShardSet: "default", ShardID: 0, Slot: "s0", WALStatus: walStatus}); err != nil {
			t.Fatal(err)
		}
	}

	// A slot that was there and is not any more, seen twice.
	active("vanished")
	sweep("vanished", "reserved")
	sweep("vanished", "missing")
	if got := state("vanished"); got != StreamActive {
		t.Errorf("one sighting of a missing slot made the stream %q; a failover flip clears by the next tick and lost cannot be undone", got)
	}
	sweep("vanished", "missing")
	if got := state("vanished"); got != StreamLost {
		t.Errorf("a slot missing on two consecutive sweeps left the stream %q, want %q: nothing tells the consumer to re-baseline", got, StreamLost)
	}

	// A slot that came back between sweeps: the stream is fine.
	active("flapped")
	sweep("flapped", "reserved")
	sweep("flapped", "missing")
	sweep("flapped", "reserved")
	sweep("flapped", "missing")
	if got := state("flapped"); got != StreamActive {
		t.Errorf("a slot that reappeared between sightings left the stream %q, want %q", got, StreamActive)
	}

	// PostgreSQL's own "lost" needs no second look: the slot is there and
	// says the WAL it needed is gone.
	active("invalidated")
	sweep("invalidated", "lost")
	if got := state("invalidated"); got != StreamLost {
		t.Errorf("an invalidated slot left the stream %q, want %q", got, StreamLost)
	}

	// Still creating: its slots do not exist yet by definition.
	if err := CreateStream(ctx, conn, Stream{Name: "new_one", Database: "app", ShardSet: "default"}); err != nil {
		t.Fatal(err)
	}
	sweep("new_one", "missing")
	sweep("new_one", "missing")
	if got := state("new_one"); got != StreamCreating {
		t.Errorf("a stream still being created was marked %q for slots CreateStream has not made yet", got)
	}
}
