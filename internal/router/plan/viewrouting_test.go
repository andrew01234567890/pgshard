package plan

import (
	"context"
	"strings"
	"testing"
)

// TestAViewIsRoutedByTheTableItProjects.
//
// A view used to be an undeclared relation, so it fell to the database
// default placement: a view over a sharded table was read from the HOME
// shard, returning that shard's rows and no error. Resolved through
// pgshard.views it inherits its base table's placement instead -- and the
// column map is what lets a predicate on the view's own column name reach
// the base table's shard key, which is exactly what a versioned-schema
// migration produces when it renames a column.
func TestAViewIsRoutedByTheTableItProjects(t *testing.T) {
	p := New()
	snap := fixture(t)
	plan := func(sql string) Plan {
		t.Helper()
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return pl
	}

	// The view exposes the shard key as "tenant". A predicate on that name
	// must route to ONE shard; before this it went to the home shard by
	// accident, and a scatter would at least have been correct.
	pl := plan("select sku from orders_v2 where tenant = 1")
	if pl.Kind != EqualUnique {
		t.Fatalf("a keyed read of a view is %v, want EqualUnique: the alias map was not applied", pl.Kind)
	}
	// It must be the SAME shard the base table's key routes to.
	base := plan("select sku from orders where tenant_id = 1")
	if len(pl.Shards) != 1 || len(base.Shards) != 1 || pl.Shards[0] != base.Shards[0] {
		t.Fatalf("view routed to %v, base table to %v: a view must land where its rows are", pl.Shards, base.Shards)
	}

	// Without a key predicate it scatters, exactly as the base table does.
	if got := plan("select sku from orders_v2").Kind; got != Scatter {
		t.Fatalf("an unkeyed read of a view is %v, want Scatter", got)
	}

	// A view over an unsharded table follows that table to the home shard.
	if got := plan("select name from items_v").Kind; got != Unsharded {
		t.Fatalf("a view over an unsharded table is %v, want Unsharded", got)
	}

	// A view pgshard cannot map is REFUSED, not guessed at. Being recorded
	// is what makes this possible: an unrecorded relation would fall back
	// to a placement and answer from one shard with no error.
	if _, err := p.Plan(context.Background(), session(snap), "select * from orders_agg"); err == nil {
		t.Fatal("an opaque view was planned; it must be refused rather than answered partially")
	} else if !strings.Contains(err.Error(), "not a projection of one table") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}
