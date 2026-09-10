package catalog

import "testing"

// TestADroppedSchemaTakesItsViewsRoutingRows.
//
// A routing row that outlives its view routes a relation that is gone --
// and then hands its column map to whatever is next created under that
// name, which is the danger the DROP VIEW path already names. DROP SCHEMA
// and DROP TABLE ... CASCADE remove views the statement never lists, so
// they have to say so by what the views belonged to.
//
// A versioned-schema migration tool drops a whole schema of views at every
// completed migration, so this is the common case, not the rare one.
func TestADroppedSchemaTakesItsViewsRoutingRows(t *testing.T) {
	schemaDrop := ViewMirrorStatements("app", MigrationMeta{View: &ViewChange{Schema: "public_01_x", DropSchema: true}})
	if len(schemaDrop) != 1 {
		t.Fatalf("DROP SCHEMA produced %d statements", len(schemaDrop))
	}
	if got := schemaDrop[0].Args; len(got) != 2 || got[0] != "app" || got[1] != "public_01_x" {
		t.Fatalf("deletes %v; it must delete every view of that schema", got)
	}

	baseDrop := ViewMirrorStatements("app", MigrationMeta{View: &ViewChange{BaseSchema: "public", BaseName: "orders", DropBase: true}})
	if len(baseDrop) != 1 {
		t.Fatalf("DROP TABLE CASCADE produced %d statements", len(baseDrop))
	}
	if got := baseDrop[0].Args; len(got) != 3 || got[2] != "orders" {
		t.Fatalf("deletes %v; it must delete the views over that table", got)
	}

	// An unqualified schema still means public, as everywhere else.
	if got := ViewMirrorStatements("app", MigrationMeta{View: &ViewChange{DropSchema: true}})[0].Args; got[1] != "public" {
		t.Fatalf("an unnamed schema deleted %v", got)
	}

	// A view delta that names nothing at all still does nothing.
	if got := ViewMirrorStatements("app", MigrationMeta{View: &ViewChange{}}); got != nil {
		t.Fatalf("an empty view change produced %v", got)
	}
}
