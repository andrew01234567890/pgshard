package router

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestAWriteHeldByTheFenceIsStampedWithTheGenerationItRunsUnder (PGS-951):
// a write that waits out the write fence and finds the shard map moved is
// planned again against the new map -- but it went on being stamped with
// the generation it was first planned under. A pooler still serving that
// old generation then admits a plan made for a map it is not serving, which
// is the case the fence exists to refuse.
//
// The observable is the fake pooler's own fence, which admits only the
// generation it serves, so the test does not depend on how many times the
// router reads its snapshot. The other direction -- a pooler already at the
// new generation refusing the old stamp -- is not asserted: the router
// retries that refusal, so it costs a round trip and is invisible here.
func TestAWriteHeldByTheFenceIsStampedWithTheGenerationItRunsUnder(t *testing.T) {
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		t.Run(mode.String(), func(t *testing.T) {
			h := newTxnHarness(t)
			ctx := context.Background()
			a, _ := h.twoTenants(t)
			target := h.shardOf(t, a)
			conn := h.connect(t, h.dsn())
			h.fenced(true)

			done := make(chan error, 1)
			go func() {
				_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1::bigint, 1)", mode, a)
				done <- err
			}()
			deadline := time.Now().Add(5 * time.Second)
			for h.r.FenceWaiting() != 1 {
				select {
				case err := <-done:
					t.Fatalf("the write returned before the fence held it: %v", err)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("the write never reached the fence, so this test never reaches the state it is about")
				}
				time.Sleep(5 * time.Millisecond)
			}

			// The router learns of generation 8 and the fence lifts in the
			// same snapshot, while the poolers are still serving 7.
			moved := *h.snap
			moved.WriteFence = false
			moved.ShardMapGeneration = 8
			h.setSnap(&moved)
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
			}
			if h.ranOn(target, "insert into orders") {
				t.Fatal("a pooler serving generation 7 ran a write planned against generation 8: the replanned statement kept the stamp of the plan it replaced")
			}

			// The poolers catch up, and the write it held goes through.
			for _, p := range h.poolers {
				p.mu.Lock()
				p.gen = 8
				p.mu.Unlock()
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the write after the poolers caught up: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the write never completed once the poolers served generation 8")
			}
			if !h.ranOn(target, "insert into orders") {
				t.Fatal("the write answered success but its shard never ran it")
			}
		})
	}
}
