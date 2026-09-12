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
	waitFor(t, func() bool { return len(h.pool[0].startLSNs()) == 1 })

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
