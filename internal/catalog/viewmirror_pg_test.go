package catalog

import (
	"context"
	"testing"
)

// TestAViewIsRecordedSoItCanBeRouted.
//
// A view with no row in pgshard.views is indistinguishable from an
// undeclared TABLE, and an undeclared relation falls to the database default
// placement -- which for a view over a sharded table is one shard's rows and
// no error. The row is what lets the planner route it instead: the base
// relation, and the map from each output column to the base column behind
// it, so a shard key the view exposes under another name is still visible.
func TestAViewIsRecordedSoItCanBeRouted(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.databases (name) VALUES ('app')`); err != nil {
		t.Fatal(err)
	}
	run := func(m MigrationMeta) {
		t.Helper()
		for _, s := range ViewMirrorStatements("app", m) {
			if _, err := conn.Exec(ctx, s.SQL, s.Args...); err != nil {
				t.Fatalf("%s: %v", s.SQL, err)
			}
		}
	}
	read := func(name string) (base, shape, cols string, found bool) {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT base_name, shape, columns::text FROM pgshard.views
			WHERE database = 'app' AND schema_name = 'public' AND view_name = $1`, name)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			return "", "", "", false
		}
		if err := rows.Scan(&base, &shape, &cols); err != nil {
			t.Fatal(err)
		}
		return base, shape, cols, true
	}

	run(MigrationMeta{View: &ViewChange{Schema: "public", Name: "v", BaseSchema: "public", BaseName: "orders",
		Shape: ViewSimple, Columns: map[string]string{"tenant": "tenant_id", "email": "_pgroll_new_email"}}})
	base, shape, cols, found := read("v")
	if !found || base != "orders" || shape != ViewSimple {
		t.Fatalf("simple view not recorded: base=%q shape=%q found=%v", base, shape, found)
	}
	// The alias map is the load-bearing part: without it the planner cannot
	// see that the view's "tenant" IS the base table's shard key.
	if cols != `{"email": "_pgroll_new_email", "tenant": "tenant_id"}` {
		t.Fatalf("column map not recorded: %s", cols)
	}

	// An opaque view is still recorded, so the router knows the relation is
	// a view and refuses it rather than treating it as an undeclared table.
	run(MigrationMeta{View: &ViewChange{Schema: "public", Name: "agg", Shape: ViewOpaque}})
	if _, shape, _, found := read("agg"); !found || shape != ViewOpaque {
		t.Fatalf("opaque view not recorded: shape=%q found=%v", shape, found)
	}

	// Replacing a view replaces its mapping rather than leaving the old one.
	run(MigrationMeta{View: &ViewChange{Schema: "public", Name: "v", BaseSchema: "public", BaseName: "orders",
		Shape: ViewSimple, Columns: map[string]string{"tenant": "tenant_id"}}})
	if _, _, cols, _ := read("v"); cols != `{"tenant": "tenant_id"}` {
		t.Fatalf("CREATE OR REPLACE left the old column map: %s", cols)
	}

	// A dropped view loses its row, or the planner keeps routing a relation
	// that is gone -- and a later table of the same name inherits its map.
	run(MigrationMeta{View: &ViewChange{Schema: "public", Name: "v", Drop: true}})
	if _, _, _, found := read("v"); found {
		t.Fatal("a dropped view kept its routing row")
	}
}
