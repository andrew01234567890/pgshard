package router

import (
	"context"
	"testing"
)

// A failed transaction whose backend is still there refuses a statement for
// another shard too. The backend would have answered 25P02 for a statement
// sent to it; one routed elsewhere moved the session and ran.
func TestAFailedTransactionDoesNotMoveToAnotherShard(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into items values (1)"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
	_ = expectRefusal(t, err, "two-phase commit is not available")
	other := h.shardOf(t, b)
	before := len(h.poolers[other].ran())
	_, err = conn.Exec(ctx, "select id from orders where tenant_id = $1", b)
	if sqlstate(err) != "25P02" {
		t.Fatalf("a read for another shard in the failed transaction: %v, want 25P02", err)
	}
	if ran := h.poolers[other].ran()[before:]; len(ran) != 0 {
		t.Fatalf("shard %d ran %v inside the failed transaction", other, ran)
	}
	tag, err := conn.Exec(ctx, "commit")
	if err != nil || tag.String() != "ROLLBACK" {
		t.Fatalf("COMMIT: %q %v, want ROLLBACK", tag, err)
	}
}
