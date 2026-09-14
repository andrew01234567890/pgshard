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

	// A row left behind by the pre-PGS-391 cluster-wide sweep, on a set
	// this stream never had slots on. Nothing deletes those, so they are
	// still in the table after the upgrade -- and if one counted as the
	// first sighting, the first post-upgrade sweep would mark the stream
	// lost on a single real observation. That is PR #899's failure mode
	// arriving by the back door, so it is pinned rather than reasoned
	// about: the debounce is keyed on the shard being written, not on the
	// stream.
	active("stale_foreign")
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.stream_status (stream, shard_set, shard_id, slot, wal_status)
		VALUES ('stale_foreign', 'g2', 0, 'leftover', 'missing')`); err != nil {
		t.Fatal(err)
	}
	sweep("stale_foreign", "missing")
	if got := state("stale_foreign"); got != StreamActive {
		t.Errorf("a leftover missing row on a set this stream has no slots on counted as a sighting; the stream went %q on one real observation", got)
	}
	sweep("stale_foreign", "missing")
	if got := state("stale_foreign"); got != StreamLost {
		t.Errorf("two sightings on the stream's OWN shard still left it %q, want %q", got, StreamLost)
	}

	// "lost" gets the same two sightings. It reads as unambiguous -- the
	// slot is there and says its WAL is gone -- but only on a primary:
	// slotsync can invalidate a synced slot on a standby while it is valid
	// on the primary, then drop and recreate it next cycle, and a sweep
	// cannot tell which member answered it.
	active("invalidated")
	sweep("invalidated", "lost")
	if got := state("invalidated"); got != StreamActive {
		t.Errorf("one sighting of an invalidated slot made the stream %q; on a standby that clears by the next slotsync cycle", got)
	}
	sweep("invalidated", "lost")
	if got := state("invalidated"); got != StreamLost {
		t.Errorf("an invalidated slot seen twice left the stream %q, want %q", got, StreamLost)
	}

	// The rows a stream's own creation leaves behind must not become the
	// first of the two sightings. The sweep runs while Create is still
	// making slots and records every shard it has not reached as missing;
	// those are suppressed while the stream is creating, but they stay in
	// the table. Create clears them when it activates -- without that, the
	// first real sighting afterwards would condemn the stream on one
	// observation, which is the thing the debounce exists to prevent.
	if err := CreateStream(ctx, conn, Stream{Name: "half_made", Database: "app", ShardSet: "default"}); err != nil {
		t.Fatal(err)
	}
	sweep("half_made", "missing") // a sweep that ran mid-create
	if err := ClearUnresumableStatus(ctx, conn, "half_made"); err != nil {
		t.Fatal(err)
	}
	if err := SetStreamState(ctx, conn, "half_made", StreamActive); err != nil {
		t.Fatal(err)
	}
	sweep("half_made", "missing") // the first sighting since it went active
	if got := state("half_made"); got != StreamActive {
		t.Errorf("a row left by the stream's own creation counted as a sighting; it went %q on one observation", got)
	}
	sweep("half_made", "missing")
	if got := state("half_made"); got != StreamLost {
		t.Errorf("two sightings after activation left the stream %q, want %q", got, StreamLost)
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
