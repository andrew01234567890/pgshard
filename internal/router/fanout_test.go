package router

import (
	"context"
	"strings"
	"testing"
)

// TestASessionCanRefuseAWiderFanout.
//
// A query that loses its shard-key predicate -- a refactor, an ORM change,
// a new WHERE clause -- silently becomes a fan-out to every shard, and is
// discovered in production as load. There was no way for an application or
// a test suite to say "this statement must stay on one shard".
//
// The levels are the routing shapes the planner already produces: a keyed
// statement is single, an IN list is a bounded multi, and a statement with
// no usable key predicate is a scatter.
func TestASessionCanRefuseAWiderFanout(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	a, b := h.twoTenants(t)

	if _, err := conn.Exec(ctx, "set pgshard.fanout = 'single'"); err != nil {
		t.Fatal(err)
	}

	// Keyed: one shard, allowed.
	if _, err := conn.Exec(ctx, "select * from orders where tenant_id = $1", a); err != nil {
		t.Fatalf("a keyed read is single-shard and must be allowed: %v", err)
	}
	// Unsharded: the home shard, also one.
	if _, err := conn.Exec(ctx, "select * from items"); err != nil {
		t.Fatalf("an unsharded read is single-shard: %v", err)
	}

	// Scatter: refused, and the message says what it was and what the
	// ceiling is.
	_, err := conn.Exec(ctx, "select * from orders")
	if err == nil {
		t.Fatal("a scatter ran under pgshard.fanout = single")
	}
	if !strings.Contains(err.Error(), "fan-out (scatter)") || !strings.Contains(err.Error(), "single") {
		t.Fatalf("the refusal does not name the shapes: %v", err)
	}

	// A bounded IN is multi: refused under single, allowed under multi.
	if _, err := conn.Exec(ctx, "select * from orders where tenant_id in ($1, $2)", a, b); err == nil {
		t.Fatal("an IN over two shards ran under pgshard.fanout = single")
	}
	if _, err := conn.Exec(ctx, "set pgshard.fanout = 'multi'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from orders where tenant_id in ($1, $2)", a, b); err != nil {
		t.Fatalf("a bounded IN is multi and must be allowed: %v", err)
	}
	if _, err := conn.Exec(ctx, "select * from orders"); err == nil {
		t.Fatal("a scatter ran under pgshard.fanout = multi")
	}

	// The default refuses nothing.
	if _, err := conn.Exec(ctx, "reset pgshard.fanout"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from orders"); err != nil {
		t.Fatalf("the default ceiling must refuse nothing: %v", err)
	}

	// An invalid value is refused where it is set, not carried to a backend.
	if _, err := conn.Exec(ctx, "set pgshard.fanout = 'sideways'"); err == nil {
		t.Fatal("an invalid ceiling was accepted")
	}
}

// TestAReferenceWriteIsExemptFromTheFanoutCeiling: a reference table lives
// on every shard, so writing one reaches every shard by definition.
// Refusing it for being wide would refuse the table's whole purpose --
// which is the exemption Neki's own documentation calls out too.
func TestAReferenceWriteIsExemptFromTheFanoutCeiling(t *testing.T) {
	h := newTxnHarness(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "set pgshard.fanout = 'single'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into regions (id) values (1)"); err != nil {
		t.Fatalf("a reference write must not be refused for its fan-out: %v", err)
	}
}
