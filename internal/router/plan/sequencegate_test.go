package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// The pre-filter decides whether the sequence refusal runs at all, so every
// function that refusal has something to say about has to be in it. One
// missing is a refusal that silently stops happening -- so this plans a
// statement calling each and requires the refusal to still arrive.
func TestEverySequenceFunctionTheGateNamesIsStillRefused(t *testing.T) {
	p := New()
	snap := fixture(t)
	ctx := context.Background()
	for _, sql := range []string{
		"select lastval()",
		"select currval('invoice_numbers')",
		"select setval('invoice_numbers', 42)",
		"select pg_sequence_last_value('invoice_numbers'::regclass)",
	} {
		if _, err := p.Plan(ctx, session(snap), sql); err == nil {
			t.Errorf("%s: planned without a refusal", sql)
		}
	}
}

// And the scan notices each of them, which is what lets the refusal run.
func TestTheScanNoticesEverySequenceFunctionInTheGate(t *testing.T) {
	for name := range sequenceFuncs {
		sql := fmt.Sprintf("select %s()", name)
		res, err := pgparser.Parse(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		raw := res.Stmts[0].RawStmt.(*pgquerypb.RawStmt)
		if !scanStatement(raw.GetStmt()).sequenceFunc {
			t.Errorf("the scan did not notice %s, so its refusal would be skipped", name)
		}
	}
	// An ordinary statement does not pay for the walk that decides.
	res, err := pgparser.Parse("select * from orders where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	raw := res.Stmts[0].RawStmt.(*pgquerypb.RawStmt)
	if scanStatement(raw.GetStmt()).sequenceFunc {
		t.Fatal("an ordinary select was marked as calling a sequence function")
	}
	// Qualified names count too: pg_catalog.nextval is nextval.
	res, err = pgparser.Parse("select pg_catalog.currval('s')")
	if err != nil {
		t.Fatal(err)
	}
	raw = res.Stmts[0].RawStmt.(*pgquerypb.RawStmt)
	if !scanStatement(raw.GetStmt()).sequenceFunc {
		t.Fatal("a schema-qualified sequence function was missed")
	}
}
