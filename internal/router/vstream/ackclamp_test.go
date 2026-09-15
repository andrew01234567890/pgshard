package vstream

import (
	"context"
	"testing"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// The clamp on a unary ack is supposed to hold it to what this router has
// delivered. It used to be seeded from the position the CALLER asked to
// start at, which made it agree with the consumer instead of checking it: a
// consumer that opened at a position it had never reached could ack straight
// back to that position, and the slot would advance past transactions this
// router had not sent.
func TestAnAckIsNotClampedToThePositionItsStreamAskedFor(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const claimed = 1 << 40
	h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: &pgshardv1.VPosition{
		ShardMapGeneration: 7, Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: claimed}}}})
	waitFor(t, func() bool { return h.server.liveStream("plain") != nil })

	if _, err := h.client.Ack(ctx, &pgshardv1.VStreamAckRequest{Stream: "plain", Position: &pgshardv1.VPosition{
		Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: claimed}}}}); err != nil {
		t.Fatalf("unary ack: %v", err)
	}
	if a := h.pool[0].ackedLSNs(); len(a) != 0 {
		t.Fatalf("the pooler was acked at %v for a stream that has delivered nothing; the position was the consumer's own claim, not a delivery", a)
	}
}

// PGS-743's acceptance: the clamp was published under the stream's NAME, and
// a later registration replaced the earlier one. A second stream opening at
// an inflated position therefore became the yardstick the first stream's
// acks were measured against, and the first could confirm past everything
// still in flight to it.
func TestASecondStreamCannotRaiseTheFirstsClamp(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(16384, "t", "id")))
	h.pool[0].feed("plain", txn(16384, 7, 1000, 2000, "1"))
	recvN(t, first, 4, 5*time.Second)

	h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: &pgshardv1.VPosition{
		ShardMapGeneration: 7, Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1 << 40}}}})
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 2 })

	if _, err := h.client.Ack(ctx, &pgshardv1.VStreamAckRequest{Stream: "plain", Position: &pgshardv1.VPosition{
		Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1 << 40}}}}); err != nil {
		t.Fatalf("unary ack: %v", err)
	}
	if a := h.pool[0].ackedLSNs(); len(a) != 1 || a[0] != 2000 {
		t.Fatalf("ack = %v, want exactly the 2000 delivered to the stream holding the name", a)
	}
}

// The in-stream ack was clamped to the merger's position vector, which also
// starts at the caller's claim, so the same inflated position confirmed the
// slot one RPC over from the unary path this fixes. Nothing below the router
// clamps it: the pooler seeds its own delivered mark from the LSN the router
// asked to start at.
func TestAnInStreamAckIsNotClampedToTheClaimEither(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const claimed = 1 << 40
	st := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain", Position: &pgshardv1.VPosition{
		ShardMapGeneration: 7, Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: claimed}}}})
	waitFor(t, func() bool { return h.server.liveStream("plain") != nil })

	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: &pgshardv1.VPosition{
		Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: claimed}}}}}); err != nil {
		t.Fatalf("in-stream ack: %v", err)
	}
	select {
	case <-h.pool[0].ackWake:
		t.Fatalf("the pooler was acked at %v for a stream that has delivered nothing", h.pool[0].ackedLSNs())
	case <-time.After(500 * time.Millisecond):
	}
}

// A second stream for a name does not take it from the first, but it does
// inherit it: when a consumer reconnects, the stream it left behind has not
// always noticed the connection is gone, and dropping the newcomer's
// registration outright left the name with no entry at all once the old
// stream finally died -- every unary ack from the live consumer refused
// with "not open on this router", which was false.
func TestTheNameIsInheritedWhenTheStreamHoldingItEnds(t *testing.T) {
	h := newHarness(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstCtx, endFirst := context.WithCancel(ctx)
	first := h.open(firstCtx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	h.pool[0].feed("plain", batch(0, evRelation(16384, "t", "id")))
	h.pool[0].feed("plain", txn(16384, 7, 1000, 2000, "1"))
	recvN(t, first, 4, 5*time.Second)

	second := h.open(ctx, &pgshardv1.VStreamRequest_Start{Stream: "plain"})
	held := h.server.liveStream("plain")
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 2 })

	endFirst()
	waitFor(t, func() bool {
		e := h.server.liveStream("plain")
		return e != nil && e != held
	})
	// The first stream's pooler call can still be reading when the router
	// has let go of it (PGS-853).
	waitFor(t, func() bool { return h.pool[0].slotTaken("plain") >= 2 })
	h.pool[0].feed("plain", batch(0, evRelation(16384, "t", "id")))
	h.pool[0].feed("plain", txn(16384, 8, 3000, 4000, "2"))
	recvN(t, second, 4, 5*time.Second)

	if _, err := h.client.Ack(ctx, &pgshardv1.VStreamAckRequest{Stream: "plain", Position: &pgshardv1.VPosition{
		Shards: []*pgshardv1.VPosition_Shard{{Shard: shardRef(shard0), Lsn: 1 << 40}}}}); err != nil {
		t.Fatalf("unary ack after the first stream ended: %v", err)
	}
	if a := h.pool[0].ackedLSNs(); len(a) != 1 || a[0] != 4000 {
		t.Fatalf("ack = %v, want the 4000 the stream that inherited the name delivered", a)
	}
}
