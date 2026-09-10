package plan

import (
	"context"
	"testing"
)

// TestDroppingASchemaRecordsThatItsViewsWentWithIt.
//
// The statement never names the views, so nothing in the migration said
// they were gone and their routing rows stayed. The planner has to record
// what they belonged to.
func TestDroppingASchemaRecordsThatItsViewsWentWithIt(t *testing.T) {
	p := New()
	snap := fixture(t)
	plan := func(sql string) Plan {
		t.Helper()
		pl, err := p.Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Migration == nil {
			t.Fatalf("%s is not a migration", sql)
		}
		return pl
	}

	v := plan(`DROP SCHEMA public_01_add CASCADE`).Migration.View
	if v == nil || !v.DropSchema || v.Schema != "public_01_add" {
		t.Fatalf("DROP SCHEMA recorded %+v; the views in it are gone and their rows have to go with them", v)
	}

	// A table dropped with CASCADE takes the views over it. Without
	// CASCADE PostgreSQL refuses the drop while a view depends on it, so
	// there is nothing to forget.
	v = plan(`DROP TABLE orders CASCADE`).Migration.View
	if v == nil || !v.DropBase || v.BaseName != "orders" || v.BaseSchema != "public" {
		t.Fatalf("DROP TABLE CASCADE recorded %+v; the views over it are gone too", v)
	}
	if v := plan(`DROP TABLE orders`).Migration.View; v != nil {
		t.Fatalf("a plain DROP TABLE recorded %+v; PostgreSQL refuses it while a view depends on the table", v)
	}
}
