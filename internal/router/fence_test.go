package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// fenced publishes a copy of the harness snapshot with the write fence
// raised or released.
func (h *shardedHarness) fenced(active bool) {
	s := *h.snap
	s.WriteFence = active
	h.setSnap(&s)
}

func TestWriteFenceHoldsNewWritesAndPassesReads(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	h.fenced(true)

	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		t.Fatalf("reads must pass through the fence: %v", err)
	}
	if _, err := conn.Exec(ctx, "select * from orders where tenant_id = $1", a); err != nil {
		t.Fatalf("keyed reads must pass through the fence: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("write returned while fenced: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if h.r.FenceWaiting() != 1 {
		t.Fatalf("fence waiting = %d, want 1", h.r.FenceWaiting())
	}
	h.fenced(false)
	if err := <-done; err != nil {
		t.Fatalf("buffered write after the fence lifted: %v", err)
	}
	if h.r.FenceWaiting() != 0 {
		t.Fatalf("fence waiting = %d after release", h.r.FenceWaiting())
	}
}

func TestWriteFenceRefusesAfterTheWindow(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	h.fenced(true)
	start := time.Now()
	_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
	pe := expectRefusalCode(t, err, codeWriteFence)
	if !strings.Contains(pe.Message, "cluster write pause for a certified restore point") || pe.Hint == "" {
		t.Fatalf("refusal %q hint %q", pe.Message, pe.Hint)
	}
	if elapsed := time.Since(start); elapsed < 600*time.Millisecond {
		t.Fatalf("refused after %s, before the buffering window", elapsed)
	}
	if h.ranOn(h.shardOf(t, a), "insert into orders") {
		t.Fatal("refused write reached the shard")
	}
	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		t.Fatalf("session unusable after the refusal: %v", err)
	}
	// Extended protocol writes are gated too.
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", a, 2); err == nil || sqlstate(err) != codeWriteFence {
		t.Fatalf("extended write: %v", err)
	}
	// And simple protocol ones, including a reference-table write.
	if _, err := conn.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, 6)", a)); err == nil || sqlstate(err) != codeWriteFence {
		t.Fatalf("simple write: %v", err)
	}
	if _, err := conn.Exec(ctx, "insert into regions (id, name) values (9, 'fr')"); err == nil || sqlstate(err) != codeWriteFence {
		t.Fatalf("reference write: %v", err)
	}
	if h.allRan("insert into regions") != nil {
		t.Fatal("refused reference write reached a shard")
	}
	h.fenced(false)
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", a); err != nil {
		t.Fatalf("write after release: %v", err)
	}
}

func TestWriteFenceLetsOpenTransactionsFinish(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	h.fenced(true)
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", a); err != nil {
		t.Fatalf("later statement of a writing transaction must not wait: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("single-shard commit during the fence: %v", err)
	}

	// A transaction that has not written yet is a new write.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select * from orders where tenant_id = $1", a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", a); err == nil || sqlstate(err) != codeWriteFence {
		t.Fatalf("first write of an open transaction: %v", err)
	}
	_ = tx.Rollback(ctx)

	// A two-phase commit waits for the fence and is rolled back when the
	// window passes, so no participant prepares across a barrier.
	h.fenced(false)
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 4)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	h.fenced(true)
	if err := tx.Commit(ctx); err == nil || sqlstate(err) != codeWriteFence {
		t.Fatalf("two-phase commit during the fence: %v", err)
	}
	if got := h.allRan("prepare transaction"); len(got) != 0 {
		t.Fatalf("shards prepared during the fence: %v", got)
	}
	if got := h.log.log(); len(got) != 0 {
		t.Fatalf("decision log touched: %v", got)
	}
	if _, err := conn.Exec(ctx, "select 1"); err != nil {
		t.Fatalf("session after the rolled back commit: %v", err)
	}
}

func TestWriteFenceBufferIsBounded(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	h.fenced(true)
	errs := make(chan error, 3)
	for i := 0; i < 2; i++ {
		conn := h.connect(t, h.dsn())
		go func() {
			_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
			errs <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for h.r.FenceWaiting() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	third := h.connect(t, h.dsn())
	_, err := third.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
	if pe := expectRefusalCode(t, err, codeBufferFull); !strings.Contains(pe.Message, "cluster write pause") {
		t.Fatalf("refusal %q", pe.Message)
	}
	h.fenced(false)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("buffered write: %v", err)
		}
	}
}

func TestTwoPhaseCommitRecordsParticipantXIDs(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []int64{a, b} {
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.log.xids) != 2 || h.log.xids[0] == "" || h.log.xids[1] == "" || h.log.xids[0] == h.log.xids[1] {
		t.Fatalf("participant xids %v", h.log.xids)
	}
	for _, s := range []int{h.shardOf(t, a), h.shardOf(t, b)} {
		if !h.ranOn(s, "select pg_current_xact_id()::text") {
			t.Fatalf("shard %d did not report its transaction id: %v", s, h.poolers[s].ran())
		}
	}
}
