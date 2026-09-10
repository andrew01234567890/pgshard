package plan

import (
	"context"
	"testing"
)

// TestASubqueryInRETURNINGIsRouted.
//
// RETURNING is part of the statement, but the INSERT, UPDATE and DELETE
// walkers never walked it -- so the relations and routing requirements
// inside it were invisible to the planner. A statement was routed by its
// TARGET table alone, and a subquery over another sharded table was then
// answered from whichever shard the DML landed on: one shard's count, with
// no error.
//
// Reproduced by gpt-6-astra for all three forms, each accepted as
// EqualUnique on a single shard.
func TestASubqueryInRETURNINGIsRouted(t *testing.T) {
	p := New()
	snap := fixture(t)
	for _, sql := range []string{
		`delete from orders where tenant_id = 1 returning (select count(*) from order_lines)`,
		`update orders set sku = 'x' where tenant_id = 1 returning (select count(*) from order_lines)`,
		`insert into orders (tenant_id, sku) values (1, 'x') returning (select count(*) from order_lines)`,
	} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		// Either outcome is acceptable and both are correct; what is NOT
		// acceptable is silently answering from the one shard the DML
		// routed to. A refusal is the honest answer while the planner
		// cannot spread the subquery.
		if err != nil {
			continue
		}
		if pl.Kind == EqualUnique && len(pl.Shards) == 1 {
			t.Errorf("%s\n  planned as %v on one shard: the subquery over another sharded table is being answered from whichever shard the DML landed on", sql, pl.Kind)
		}
	}
}

// TestRETURNINGWithoutASubqueryStillRoutesToOneShard keeps the fix from
// over-reaching: an ordinary RETURNING names the target's own columns and
// must still route by the key, or every keyed write would start scattering.
func TestRETURNINGWithoutASubqueryStillRoutesToOneShard(t *testing.T) {
	p := New()
	snap := fixture(t)
	for _, sql := range []string{
		`delete from orders where tenant_id = 1 returning sku`,
		`update orders set sku = 'x' where tenant_id = 1 returning tenant_id, sku`,
		`insert into orders (tenant_id, sku) values (1, 'x') returning sku`,
	} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Kind != EqualUnique || len(pl.Shards) != 1 {
			t.Errorf("%s\n  planned as %v on %v; a plain RETURNING must still route by the key", sql, pl.Kind, pl.Shards)
		}
	}
}
