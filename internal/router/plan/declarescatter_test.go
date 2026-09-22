package plan

import (
	"context"
	"strings"
	"testing"
)

// Building the merge rewrote a SELECT a DECLARE does not have at its top,
// and the planner dereferenced nil. The statement gets the refusal the
// router documents for it instead.
func TestADeclaredMultiShardCursorIsRefusedNotAPanic(t *testing.T) {
	for _, sql := range []string{
		"declare c cursor for select id from orders order by id limit 2",
		"declare c cursor for select id from orders offset 3",
		"declare c cursor for select id from orders order by qty + 1",
		"declare c cursor for select avg(qty) from orders",
		"create view v as select avg(qty) from orders",
		"explain select avg(qty) from orders",
		"explain select id from orders order by id limit 2",
		"create view v as select id from orders order by id limit 2",
	} {
		_, err := New().Plan(context.Background(), session(fixture(t)), sql)
		if err == nil || !strings.Contains(err.Error(), "only a plain SELECT can run on multiple shards") {
			t.Fatalf("%s: err = %v", sql, err)
		}
	}
}

// On one shard nothing is merged, so the cursor plans as it always could.
func TestADeclaredSingleShardCursorWithAnAverageStillPlans(t *testing.T) {
	p, err := New().Plan(context.Background(), session(fixture(t)), "declare c cursor for select avg(qty) from orders where tenant_id = 1")
	if err != nil || len(p.Shards) != 1 {
		t.Fatalf("shards %v err %v, want one shard", p.Shards, err)
	}
}
