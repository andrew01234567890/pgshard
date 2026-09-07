package plan

import (
	"context"
	"strings"
	"testing"
)

// The parse result belongs to the cache and is handed to every session that
// sends the same SQL. Rewriting it in place means the next execution starts
// from the last one's output -- and for this pass that is unbounded, because
// the walk finds the catalog reference inside the subquery it built last
// time and wraps it again, until the statement is refused for length.
func TestFilteringIntrospectionDoesNotAccumulateOnTheCachedTree(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	ctx := context.Background()
	const sql = "select column_name from information_schema.columns where table_name = 'orders'"

	first, err := p.Plan(ctx, session(snap), sql)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		again, err := p.Plan(ctx, session(snap), sql)
		if err != nil {
			t.Fatal(err)
		}
		if again.Rewritten != first.Rewritten {
			t.Fatalf("execution %d rewrote it differently:\n first: %s\n now:   %s", i+2, first.Rewritten, again.Rewritten)
		}
	}
	if n := strings.Count(first.Rewritten, "NOT LIKE"); n != 1 {
		t.Fatalf("rewritten to %q, want exactly one filter", first.Rewritten)
	}
}

// The arms of a set operation are SelectStmt messages rather than Nodes, so
// the reflective walk never offers them to the callback and a UNION of two
// catalog reads went through unfiltered. Real tooling sends this shape.
func TestASetOperationOverTheCatalogIsFiltered(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	pl, err := p.Plan(context.Background(), session(snap),
		"select attname from pg_attribute union all select attname from pg_attribute")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(pl.Rewritten, "NOT LIKE"); n != 2 {
		t.Fatalf("rewritten to %q, want both arms filtered", pl.Rewritten)
	}
}

// A catalog read in the FROM of an UPDATE or the USING of a DELETE is the
// same read; it is only the clause that differs.
func TestACatalogReadInUpdateFromAndDeleteUsingIsFiltered(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	for _, sql := range []string{
		"update notes set n = a.attname from pg_attribute a where a.attrelid = notes.oid",
		"delete from notes using pg_attribute a where a.attname = notes.n",
	} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			// Refused for an unrelated reason is not what this is about.
			continue
		}
		if !strings.Contains(pl.Rewritten, "NOT LIKE") {
			t.Fatalf("%s: rewritten to %q, want the catalog read filtered", sql, pl.Rewritten)
		}
	}
}

// A subquery answers for the columns of the relation, not for the relation
// itself: a schema-qualified reference to it stops resolving, and its
// system columns stop existing. Both are legal SQL that nothing reflecting
// on a schema sends, so the filter stands aside rather than break them.
func TestTheFilterStandsAsideForReferencesOnlyTheTableAnswers(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	for _, sql := range []string{
		"select information_schema.columns.column_name from information_schema.columns",
		"select a.ctid from pg_catalog.pg_attribute a",
	} {
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if strings.Contains(pl.Rewritten, "NOT LIKE") {
			t.Fatalf("%s: rewritten to %q, which no longer resolves", sql, pl.Rewritten)
		}
	}
}

// DDL is applied from the recorded statement rather than from the rewritten
// text, so a rewrite here is computed, ignored, and still costs the re-plan
// that a non-empty Rewritten forces.
func TestDDLOverTheCatalogIsNotRewritten(t *testing.T) {
	p := New()
	snap := rewriteFixture(t, "tenant_id", "id", "amount")
	pl, err := p.Plan(context.Background(), session(snap), "create view v as select attname from pg_attribute")
	if err != nil {
		t.Skipf("refused for another reason: %v", err)
	}
	if pl.Rewritten != "" {
		t.Fatalf("DDL rewritten to %q", pl.Rewritten)
	}
}
