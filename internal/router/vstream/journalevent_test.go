package vstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestAJournalSaysWhereTheStreamContinues.
//
// stop_on_reshard exists so a consumer can follow a cutover instead of
// being told to start again. The event it gets carried the generation and
// the source shards -- and neither the journal id nor a single target
// position, though the proto has a field for "the LSN to resume from" and
// the cutover writes exactly those, per target, in the same transaction as
// the flip.
//
// So the consumer opened the new shard set with no position. Everything
// replicated to the targets between its last vgtid and the flip was never
// delivered to it: a silent gap, not an error.
func TestAJournalSaysWhereTheStreamContinues(t *testing.T) {
	h := newHarness(t, 2)
	h.server.Catalog = fakeCatalog{
		streams: map[string]catalog.Stream{"plain": {Name: "plain", Database: "app", State: catalog.StreamActive}},
		journal: &Journal{ID: "8f1d0f4e-0000-4000-8000-000000000001", Targets: map[int32]uint64{1: 4242, 0: 99}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain",
		Options: &pgshardv1.VStreamOptions{StopOnReshard: true, HeartbeatIntervalMs: 50}})
	recvN(t, st, 1, 2*time.Second)

	h.topo.reshard()
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 50, "1"))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ev, err := st.Recv()
		if err != nil {
			break
		}
		j, ok := ev.GetEvent().(*pgshardv1.VEvent_Journal_)
		if !ok {
			continue
		}
		if j.Journal.GetJournalId() == "" {
			t.Fatal("the journal event carries no id; a consumer cannot tell which cutover it is following")
		}
		if len(j.Journal.GetTargets()) != 2 {
			t.Fatalf("the journal carries %d targets; without them the consumer opens the new set with no position and never receives what was replicated before the flip", len(j.Journal.GetTargets()))
		}
		// Ordered, so what a consumer receives does not depend on a map.
		if got := j.Journal.GetTargets()[0]; got.GetShard().GetShardId() != 0 || got.GetLsn() != 99 {
			t.Fatalf("first target is shard %d at %d", got.GetShard().GetShardId(), got.GetLsn())
		}
		if got := j.Journal.GetTargets()[1]; got.GetShard().GetShardId() != 1 || got.GetLsn() != 4242 {
			t.Fatalf("second target is shard %d at %d", got.GetShard().GetShardId(), got.GetLsn())
		}
		if len(j.Journal.GetParticipants()) != 2 {
			t.Fatalf("participants %v", j.Journal.GetParticipants())
		}
		return
	}
	t.Fatal("no journal event arrived")
}

// TestAJournalThatCannotBeReadStillEndsTheStream: the stream is ending
// either way, and an event that says less is better than one that does not
// arrive.
func TestAJournalThatCannotBeReadStillEndsTheStream(t *testing.T) {
	h := newHarness(t, 2)
	h.server.Catalog = fakeCatalog{
		streams:    map[string]catalog.Stream{"plain": {Name: "plain", Database: "app", State: catalog.StreamActive}},
		journalErr: errors.New("catalog unreachable"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain",
		Options: &pgshardv1.VStreamOptions{StopOnReshard: true, HeartbeatIntervalMs: 50}})
	recvN(t, st, 1, 2*time.Second)
	h.topo.reshard()
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 50, "1"))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ev, err := st.Recv()
		if err != nil {
			break
		}
		if _, ok := ev.GetEvent().(*pgshardv1.VEvent_Journal_); ok {
			return
		}
	}
	t.Fatal("a journal the catalog could not answer for stopped the event reaching the consumer at all")
}
