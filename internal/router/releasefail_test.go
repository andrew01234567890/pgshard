package router

import (
	"context"
	"testing"
	"time"
)

// heldSessions lists the session ids the fake pooler still holds a backend
// or a reservation for.
func (f *fakePooler) heldSessions() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	held := map[string]bool{}
	for sid := range f.backends {
		held[sid] = true
	}
	for sid := range f.reserved {
		held[sid] = true
	}
	return held
}

func (f *fakePooler) reservations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.reserves...)
}

// awaitReleased waits for the pooler to hold nothing under sid.
func awaitReleased(t *testing.T, f *fakePooler, sid string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for f.heldSessions()[sid] {
		if time.Now().After(deadline) {
			t.Fatalf("the pooler still holds session %s: the backend the failed release left behind was never handed back", sid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A COMMIT's tag is on the wire before the session hands its backend back.
// When that hand-back fails -- the pooler unreachable for a moment -- the
// client must still hear COMMIT: 08006 means "outcome unknown", and a
// correct client retries or writes off a transaction that committed.
//
// Dropping the error is not enough on its own. The pooler still holds the
// backend under the session's name, prepared statements and all, and the
// next statement would meet it once the pooler answers again: 42P05 on the
// statement the session replays.
func TestACommitIsReportedWhenTheReleaseAfterItFails(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Prepare(ctx, "q1", "select 1"); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select 1"); err != nil {
		t.Fatal(err)
	}
	h.fp.unreachableRelease.Store(true)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the transaction committed but the client was told %v", err)
	}
	h.fp.unreachableRelease.Store(false)

	var n int
	if err := conn.QueryRow(ctx, "q1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("the statement after the commit: %d, %v", n, err)
	}
	reserves := h.fp.reservations()
	if len(reserves) < 2 || reserves[len(reserves)-1] == reserves[0] {
		t.Fatalf("reservations %v: the next statement reserved the name the pooler may still hold a backend under", reserves)
	}
	awaitReleased(t, h.fp, reserves[0])
}

// The same for a transaction across shards: every participant's release
// fails after the decision is on the wire.
func TestAMultiShardCommitIsReportedWhenItsReleasesFail(t *testing.T) {
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
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b); err != nil {
		t.Fatal(err)
	}
	for _, fp := range h.poolers {
		fp.unreachableRelease.Store(true)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the transaction committed but the client was told %v", err)
	}
	for _, fp := range h.poolers {
		fp.unreachableRelease.Store(false)
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("the transaction after the commit: %v", err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", a); err != nil {
		t.Fatalf("the transaction after the commit: %v", err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 4)", b); err != nil {
		t.Fatalf("the transaction after the commit: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("the transaction after the commit: %v", err)
	}
	for _, sh := range []int64{a, b} {
		fp := h.poolers[h.shardOf(t, sh)]
		reserves := fp.reservations()
		if len(reserves) < 2 || reserves[len(reserves)-1] == reserves[0] {
			t.Fatalf("shard of %d reservations %v: the next transaction reserved the name the pooler may still hold a backend under", sh, reserves)
		}
		awaitReleased(t, fp, reserves[0])
	}
}
