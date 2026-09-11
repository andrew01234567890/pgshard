package plan

import (
	"context"
	"strings"
	"testing"
)

func explain(t *testing.T, sql string) string {
	t.Helper()
	p, err := New().Plan(context.Background(), session(fixture(t)), sql)
	if err != nil {
		t.Fatalf("EXPLAIN itself must not fail: %v", err)
	}
	if p.Kind != SessionLocal || p.Explain == nil {
		t.Fatalf("the router answers this one itself: kind=%v explain=%v", p.Kind, p.Explain)
	}
	return strings.Join(p.Explain, "\n")
}

// The docs tell a user to write keyed predicates and to avoid accidental
// scatters, and gave them no way to check: a plain EXPLAIN is routed like
// any other statement, so it comes back with one shard's PostgreSQL plan
// and says nothing about which shards were chosen.
func TestExplainPgshardShowsTheRoutingDecision(t *testing.T) {
	keyed := explain(t, "explain (pgshard) select * from orders where tenant_id = 1")
	if !strings.Contains(keyed, "Route [EqualUnique]") {
		t.Errorf("a keyed read is routed to one shard, got:\n%s", keyed)
	}
	if !strings.Contains(keyed, "Fanout: single") {
		t.Errorf("a keyed read is a single-shard fanout, got:\n%s", keyed)
	}
	if !strings.Contains(keyed, "orders") {
		t.Errorf("the table it routed on must be named, got:\n%s", keyed)
	}

	scatter := explain(t, "explain (pgshard) select * from orders")
	if !strings.Contains(scatter, "Route [Scatter]") || !strings.Contains(scatter, "Fanout: scatter") {
		t.Errorf("an unkeyed read scatters, and saying so is the whole point, got:\n%s", scatter)
	}
	if strings.Contains(scatter, "Shards: none") {
		t.Errorf("a scatter names the shards it reaches, got:\n%s", scatter)
	}
}

// A refusal IS the routing decision, and the one a user most needs to see
// before running the statement: rendering it as an error would abort the
// EXPLAIN and, in a transaction, the transaction with it.
func TestExplainPgshardRendersARefusalInsteadOfRaisingIt(t *testing.T) {
	out := explain(t, "explain (pgshard) select nextval('invoice_numbers') + 1")
	if !strings.HasPrefix(out, "Refused") {
		t.Fatalf("want a rendered refusal, got:\n%s", out)
	}
	if !strings.Contains(out, "invoice_numbers") {
		t.Errorf("the refusal must carry its own reason, got:\n%s", out)
	}
}

// Without the option EXPLAIN keeps meaning what PostgreSQL means by it,
// including reaching a shard, so the existing plans stay reachable.
func TestPlainExplainIsStillRoutedToAShard(t *testing.T) {
	snap := fixture(t)
	p, err := New().Plan(context.Background(), session(snap), "explain select * from orders where tenant_id = 1")
	if err != nil {
		t.Fatalf("plain EXPLAIN must still plan: %v", err)
	}
	if p.Explain != nil {
		t.Errorf("plain EXPLAIN is not the router's to answer: %v", p.Explain)
	}
	if p.Kind != EqualUnique || len(p.Shards) != 1 {
		t.Errorf("plain EXPLAIN is routed like the statement it explains: kind=%v shards=%v", p.Kind, p.Shards)
	}
}

// ANALYZE promises the statement ran. This EXPLAIN never runs it, so the
// pair would report a plan for something that did not happen.
func TestExplainPgshardRefusesTheOtherExplainOptions(t *testing.T) {
	for _, sql := range []string{
		"explain (pgshard, analyze) select * from orders",
		"explain (analyze, pgshard) select * from orders",
		"explain (pgshard, format json) select * from orders",
		"explain (pgshard, costs off) select * from orders",
	} {
		_, err := New().Plan(context.Background(), session(fixture(t)), sql)
		if err == nil || !strings.HasPrefix(err.Error(), "0A000") {
			t.Errorf("%s: want a 0A000 refusal, got %v", sql, err)
		}
	}
}

// The option is boolean like every other EXPLAIN option, so switching it off
// must leave the statement meaning what PostgreSQL means by it.
func TestExplainPgshardOptionIsBoolean(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"explain (pgshard false) select * from orders where tenant_id = 1",
		"explain (pgshard off) select * from orders where tenant_id = 1",
		"explain (pgshard 0) select * from orders where tenant_id = 1",
	} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if p.Explain != nil {
			t.Errorf("%s: the option is off, the router must not answer it: %v", sql, p.Explain)
		}
	}
	for _, sql := range []string{
		"explain (pgshard true) select * from orders where tenant_id = 1",
		"explain (pgshard on) select * from orders where tenant_id = 1",
		"explain (pgshard 1) select * from orders where tenant_id = 1",
	} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if p.Explain == nil {
			t.Errorf("%s: the option is on, the router must answer it", sql)
		}
	}
}

// What the router does with the shards' rows is the other half of the cost
// a user is trying to see: an ORDER BY merged at the router and a LIMIT
// applied there are work the shards' own plans never mention.
func TestExplainPgshardDescribesTheMergeOnlyWhereItHappens(t *testing.T) {
	ordered := explain(t, "explain (pgshard) select * from orders order by id limit 10")
	if !strings.Contains(ordered, "merged in ORDER BY order") || !strings.Contains(ordered, "LIMIT 10 applied at the router") {
		t.Errorf("want the router's merge described, got:\n%s", ordered)
	}
	if agg := explain(t, "explain (pgshard) select count(*) from orders"); !strings.Contains(agg, "each shard returns one row") {
		t.Errorf("want the aggregate combination described, got:\n%s", agg)
	}
	// A single-shard read never reaches the merge, and saying the router
	// combines results it does not receive would be a plain untruth.
	if one := explain(t, "explain (pgshard) select * from orders where tenant_id = 1 order by id"); strings.Contains(one, "Merge:") {
		t.Errorf("a keyed read has no merge, got:\n%s", one)
	}
}

// The two routings that name no shard in the plan mean opposite things, and
// rendering both as "no shards" would read as "this costs nothing".
func TestExplainPgshardDistinguishesTheRoutingsThatNameNoShard(t *testing.T) {
	ref := explain(t, "explain (pgshard) select * from regions")
	if !strings.Contains(ref, "one serving shard, chosen at execution") {
		t.Errorf("a reference read still visits a shard, got:\n%s", ref)
	}
	deferred := explain(t, "explain (pgshard) select * from orders where tenant_id = $1")
	if !strings.Contains(deferred, "decided at Bind") {
		t.Errorf("a parameterised key is routed at Bind, got:\n%s", deferred)
	}
}
