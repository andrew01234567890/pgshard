package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// A scatter that concatenates its shards is right for a scalar function and
// wrong for an aggregate, and a parse tree cannot tell them apart: a
// user-defined aggregate is a plain FuncCall with no aggregate flags. So the
// router refuses every function it cannot name as a PostgreSQL built-in --
// which took ST_AsText, uuid_generate_v4, similarity and every other
// extension scalar with it, on any multi-shard read.
//
// pgshard.functions is how an operator says yes. It is the only way the
// router can be told: CREATE FUNCTION, CREATE AGGREGATE and CREATE EXTENSION
// are all refused through the router, so there is no DDL path to learn from.
func TestADeclaredScalarIsProjectedByAScatter(t *testing.T) {
	snap := fixture(t)
	const sql = "select st_astext(geom), tenant_id from orders"
	if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
		t.Fatal("an undeclared function was projected by a scatter")
	} else if !strings.Contains(err.Error(), "st_astext") {
		t.Fatalf("the refusal does not name the function: %v", err)
	}

	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: fixtureDB, Name: "st_astext"}: true}
	p, err := New().Plan(context.Background(), session(snap), sql)
	if err != nil {
		t.Fatalf("a declared scalar is still refused: %v", err)
	}
	if p.Kind != Scatter {
		t.Errorf("kind %v, want a scatter", p.Kind)
	}

	// Another database's declaration is not this one's: the table is per
	// database because the functions are.
	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: "other", Name: "st_astext"}: true}
	if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
		t.Error("a declaration in another database let the call through")
	}
}

// A schema-qualified call asks the same question, because the lookup is by
// the last name element: the router does not resolve search_path, so
// ext.f(x) and f(x) cannot be told apart by it and must not be answered
// differently.
func TestAQualifiedCallAsksTheSameQuestion(t *testing.T) {
	snap := fixture(t)
	const sql = "select ext.st_astext(geom), tenant_id from orders"
	if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
		t.Fatal("an undeclared qualified function was projected by a scatter")
	}
	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: fixtureDB, Name: "st_astext"}: true}
	if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
		t.Errorf("a declared scalar is still refused when qualified: %v", err)
	}
}

// A declaration says a name is a scalar; it does not say the call is one.
// Aggregate syntax is decided from the parse tree before the declaration is
// consulted, so a declared name used with DISTINCT, FILTER, ORDER BY, OVER
// or an aggregate argument is still refused -- which is what stops a
// declaration from being a way to smuggle an aggregate past the merge.
func TestADeclarationDoesNotExcuseAggregateSyntax(t *testing.T) {
	snap := fixture(t)
	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: fixtureDB, Name: "first"}: true}
	// The control: the plain call is what the declaration is for.
	if _, err := New().Plan(context.Background(), session(snap), "select first(status) from orders"); err != nil {
		t.Fatalf("a declared scalar is refused, so this test proves nothing: %v", err)
	}
	for _, sql := range []string{
		"select first(distinct status) from orders",
		"select first(status) filter (where id > 1) from orders",
		"select first(status) over () from orders",
		"select first(status order by id) from orders",
		// An aggregate inside it is still an aggregate.
		"select first(sum(amount)) from orders",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err == nil {
			t.Errorf("%s: planned, so a declaration let aggregate syntax through", sql)
		}
	}
}

// And a built-in aggregate is never a declarable name: the built-in tables
// are consulted first, so sum() is merged whatever the catalog says.
func TestADeclarationCannotTurnABuiltinAggregateIntoAScalar(t *testing.T) {
	snap := fixture(t)
	snap.ScalarFunctions = map[snapshot.FunctionKey]bool{{Database: fixtureDB, Name: "sum"}: true}
	p, err := New().Plan(context.Background(), session(snap), "select sum(amount) from orders")
	if err != nil {
		t.Fatalf("a built-in aggregate stopped being planned: %v", err)
	}
	if p.merge == nil || len(p.merge.Aggregates) == 0 {
		t.Error("sum() was concatenated rather than merged, so each shard's partial answer is reported as the answer")
	}
}
