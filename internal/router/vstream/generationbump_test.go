package vstream

import (
	"context"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestAGenerationBumpThatMovesNoRowsLeavesTheStreamAlone.
//
// The shard map generation counts every catalog change, and most of them
// move nothing: a shard set declared days before its cutover, a table
// placement published. Ending a stream whenever the counter moved ended it
// on all of those with RESHARDED, "restart the stream" -- and the restart
// could not work either, because the position the consumer had saved
// carried the old count and was refused on sight. The consumer looped
// until someone hand-edited the generation out of its saved position or
// re-copied everything.
//
// What actually invalidates a stream is the shard set changing under it.
func TestAGenerationBumpThatMovesNoRowsLeavesTheStreamAlone(t *testing.T) {
	h := newHarness(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain",
		Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 50}})
	first := recvN(t, st, 1, 2*time.Second)
	// What the consumer had saved BEFORE the bump. This is the one the old
	// code refused for ever afterwards.
	var saved *pgshardv1.VPosition
	if hb, ok := first[0].GetEvent().(*pgshardv1.VEvent_Heartbeat_); ok {
		saved = hb.Heartbeat.GetPosition()
	}
	if saved == nil {
		t.Fatalf("no position in the first event: %s", describe(first[0]))
	}

	h.topo.bumpGeneration()

	// The position from before the bump still resumes. Nothing moved, so
	// there is nothing for the consumer to re-copy.
	before := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: saved,
		Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 50}})
	for _, ev := range recvN(t, before, 1, 2*time.Second) {
		if e, ok := ev.GetEvent().(*pgshardv1.VEvent_Error_); ok {
			t.Fatalf("a position saved before a bump that moved nothing was refused: %v", e.Error.GetCode())
		}
	}

	// The stream goes on. A heartbeat is enough to say so: the old code
	// answered the very next loop with RESHARDED.
	h.pool[0].feed("plain", batch(0, evRelation(1, "t", "id")))
	h.pool[0].feed("plain", txn(1, 1, 1, 50, "1"))
	var pos *pgshardv1.VPosition
	for _, ev := range recvN(t, st, 6, 3*time.Second) {
		if e, ok := ev.GetEvent().(*pgshardv1.VEvent_Error_); ok {
			t.Fatalf("a generation bump that moved nothing ended the stream: %v", e.Error.GetCode())
		}
		switch e := ev.GetEvent().(type) {
		case *pgshardv1.VEvent_Vgtid:
			pos = e.Vgtid.GetPosition()
		case *pgshardv1.VEvent_Heartbeat_:
			if pos == nil {
				pos = e.Heartbeat.GetPosition()
			}
		}
	}
	if pos == nil {
		t.Fatal("no commit position to resume from")
	}
	if pos.GetShardSetFingerprint() == 0 {
		t.Fatal("a position carries the identity of the shard set it was taken in, or it can only be compared on the counter again")
	}

	// And the position it hands out resumes: the whole point of not ending
	// the stream is that what the consumer saved is still good.
	resumed := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: pos,
		Options: &pgshardv1.VStreamOptions{HeartbeatIntervalMs: 50}})
	for _, ev := range recvN(t, resumed, 1, 2*time.Second) {
		if e, ok := ev.GetEvent().(*pgshardv1.VEvent_Error_); ok {
			t.Fatalf("the position saved across the bump was refused: %v", e.Error.GetCode())
		}
	}

	// A reshard still ends it: the shards are different ones now.
	h.topo.reshard()
	h.pool[0].feed("plain", txn(1, 2, 60, 100, "2"))
	ended := false
	deadline := time.Now().Add(3 * time.Second)
	for !ended && time.Now().Before(deadline) {
		ev, err := st.Recv()
		if err != nil {
			break
		}
		if e, ok := ev.GetEvent().(*pgshardv1.VEvent_Error_); ok && e.Error.GetCode() == pgshardv1.VEvent_Error_CODE_RESHARDED {
			ended = true
		}
	}
	if !ended {
		t.Fatal("a reshard must still end the stream")
	}
}
