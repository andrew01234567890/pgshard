package plan

import (
	"context"
	"strings"
	"testing"
)

// The merge planner recognised the shard key by the column's last name
// alone. A reference table is copied to every shard, so a same-named column
// on one -- regions.tenant_id joined to orders -- has rows for a single
// value on EVERY shard, and grouping by it is not shard-local. Accepting it
// marked the GROUP BY shard-local, skipped aggregate combination, and the
// router concatenated one partial count per shard for the same group.
func TestGroupingByAReferenceColumnIsNotShardLocal(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select r.tenant_id, count(*) from orders o join regions r on o.region = r.id group by r.tenant_id",
		"select distinct r.tenant_id from orders o join regions r on o.region = r.id",
	} {
		_, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil {
			t.Errorf("%s: planned; every shard would answer its own partial for the same group", sql)
			continue
		}
		if !strings.Contains(err.Error(), "shard key") {
			t.Errorf("%s: %v, want a refusal naming the shard key", sql, err)
		}
	}
}

// Grouping by the sharded relation's own key is still shard-local, by any
// spelling: every group lives on one shard, so concatenating is right.
func TestGroupingByTheShardedRelationsKeyStillMerges(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"select o.tenant_id, count(*) from orders o join regions r on o.region = r.id group by o.tenant_id",
		"select tenant_id, count(*) from orders group by tenant_id",
		"select orders.tenant_id, count(*) from orders group by orders.tenant_id",
		"select distinct o.tenant_id from orders o join regions r on o.region = r.id",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

// An unqualified key with a reference table in play cannot be attributed:
// the router does not know a reference table's columns, and PostgreSQL does
// not say which relation it resolved the name to. Refused rather than
// assumed to be the sharded one.
func TestAnUnqualifiedKeyIsNotAssumedWithAReferenceTableInPlay(t *testing.T) {
	snap := fixture(t)
	const sql = "select tenant_id, count(*) from orders o join regions r on o.region = r.id group by tenant_id"
	if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
		t.Error("an unqualified key was attributed to the sharded relation with a reference table joined in")
	}
}
