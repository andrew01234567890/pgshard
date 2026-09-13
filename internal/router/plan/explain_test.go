package plan

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
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

// A reference write runs on every shard in one two-phase commit and never
// reaches the merge. Rendering its unused merge spec reported "refused at
// execution" for a statement that runs perfectly well, which is worse than
// saying nothing: the feature exists to be believed.
func TestExplainPgshardDoesNotCallAReferenceWriteRefused(t *testing.T) {
	for _, sql := range []string{
		"explain (pgshard) insert into regions (id, name) values (1, 'x')",
		"explain (pgshard) update regions set name = 'y' where id = 1",
		"explain (pgshard) delete from regions where id = 1",
	} {
		out := explain(t, sql)
		if strings.Contains(out, "refused") {
			t.Errorf("%s: the statement runs; got:\n%s", sql, out)
		}
		if !strings.Contains(out, "every shard in one two-phase commit") {
			t.Errorf("%s: want the write described, got:\n%s", sql, out)
		}
	}
}

// SessionLocal is not router-local. SET, EXECUTE and DECLARE are forwarded
// to whichever shard the session is on; only nextval() over a global
// sequence and this EXPLAIN are answered without one.
func TestExplainPgshardSeparatesForwardedFromRouterAnswered(t *testing.T) {
	forwarded := explain(t, "explain (pgshard) execute somestatement")
	if !strings.Contains(forwarded, "the shard this session is on") {
		t.Errorf("EXECUTE is forwarded, got:\n%s", forwarded)
	}
	answered := explain(t, "explain (pgshard) select nextval('invoice_numbers')")
	if !strings.Contains(answered, "the router answers this itself") {
		t.Errorf("a global nextval never reaches a shard, got:\n%s", answered)
	}
}

// The pgshard option takes exactly what defGetBoolean takes: nothing, 0, 1,
// and the words true, false, on and off.
//
// The refusal is ours to raise. Every other EXPLAIN option is PostgreSQL's,
// so a shard rejects what it cannot read; this one is stripped from the
// statement before a shard sees it, and a value we cannot read would
// otherwise be treated as false and run the statement the user did not ask
// for.
func TestThePgshardOptionTakesWhatDefGetBooleanTakes(t *testing.T) {
	ctx := context.Background()
	for _, on := range []string{"explain (pgshard) %s", "explain (pgshard true) %s", "explain (pgshard on) %s", "explain (pgshard 1) %s"} {
		p, err := New().Plan(ctx, session(fixture(t)), fmt.Sprintf(on, "select * from orders where tenant_id = 1"))
		if err != nil || p.Explain == nil {
			t.Errorf("%q: want the router's plan, got %v %v", on, p.Explain, err)
		}
	}
	for _, off := range []string{"explain (pgshard false) %s", "explain (pgshard off) %s", "explain (pgshard 0) %s"} {
		p, err := New().Plan(ctx, session(fixture(t)), fmt.Sprintf(off, "select * from orders where tenant_id = 1"))
		if err != nil {
			t.Errorf("%q: an explicit false is a plain EXPLAIN, not a refusal: %v", off, err)
		}
		if p.Explain != nil {
			t.Errorf("%q: the router answered a statement that asked for PostgreSQL's EXPLAIN", off)
		}
	}
	// yes/no/t/f are GUC spellings, not defGetBoolean's, and 2 is not a
	// boolean at all.
	for _, bad := range []string{"explain (pgshard 2) %s", "explain (pgshard yes) %s", "explain (pgshard t) %s", "explain (pgshard 'nope') %s"} {
		_, err := New().Plan(ctx, session(fixture(t)), fmt.Sprintf(bad, "select * from orders where tenant_id = 1"))
		if err == nil || !strings.Contains(err.Error(), "requires a Boolean value") {
			t.Errorf("%q: want defGetBoolean's own refusal, got %v", bad, err)
		}
	}
}

// The parameter types EXPLAIN (PGSHARD) reports are 0 -- the protocol's
// "unspecified", which a driver answers by inferring from the value it
// holds -- except for a parameter the plan routes on, where the catalog has
// recorded the shard key column's type and that is the type PostgreSQL
// would have inferred.
//
// Claiming a type we have not determined would be worse than saying
// nothing: no value is read here, and a driver that trusts a wrong one
// fails to encode a perfectly good argument.
func TestExplainReportsTheShardKeysTypeAndNothingElse(t *testing.T) {
	snap := fixture(t)
	key := snapshot.TableKey{Database: fixtureDB, SchemaName: "public", TableName: "orders"}
	pl := snap.Tables[key]
	pl.ShardKeyType = "bigint"
	snap.Tables[key] = pl

	p, err := New().Plan(context.Background(), session(snap),
		"explain (pgshard) select * from orders where tenant_id = $2 and note = $1")
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint32{0, 20}; !slices.Equal(p.ExplainParams, want) {
		t.Fatalf("ExplainParams = %v, want %v: $2 is the shard key (bigint), $1 is not determined", p.ExplainParams, want)
	}
}
