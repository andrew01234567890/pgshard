package controller

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAMoveKeepsAForeignKeyToAReferenceTable is PGS-255's last object class.
// A move used to refuse any table carrying a foreign key in either
// direction, because the shadow is built with LIKE INCLUDING ALL, which
// carries none, and a swap that dropped one would leave the table looking
// correct and enforcing no referential integrity at all.
//
// A key survives a move exactly when the referenced ROWS are on every shard
// the moved table lands on, and that is what a reference table is.
func TestAMoveKeepsAForeignKeyToAReferenceTable(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE TABLE regions (code text PRIMARY KEY, name text NOT NULL)`)
		mustExec(t, f.app(id), `INSERT INTO regions VALUES ('n', 'north'), ('s', 'south')`)
	}
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, region text NOT NULL REFERENCES regions(code), body text)`)
	mustExec(t, src, `INSERT INTO notes SELECT g, CASE WHEN g % 2 = 0 THEN 'n' ELSE 's' END, 'b' || g FROM generate_series(1, 16) g`)

	// regions is declared a reference table, which is what makes the key
	// movable: every shard holds all of its rows.
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'regions', 'reference')`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'regions', 'reference')`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("notes", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 16 {
			t.Errorf("shard %d: %d rows", id, n)
		}
		// The constraint is there and points at the right table.
		got := queryOne[string](t, c, `SELECT coalesce(string_agg(pg_get_constraintdef(oid), '; ' ORDER BY conname), '')
			FROM pg_constraint WHERE conrelid = 'public.notes'::regclass AND contype = 'f'`)
		if !strings.Contains(got, "REFERENCES regions(code)") {
			t.Errorf("shard %d: foreign key = %q", id, got)
		}
		// And it ENFORCES, which is the whole point: a row naming a region
		// that does not exist must be rejected, and one naming a region
		// that does must be accepted.
		if _, err := c.Exec(context.Background(), `INSERT INTO notes VALUES (900 + $1, 'nowhere', 'x')`, id); err == nil {
			t.Errorf("shard %d: a row referencing a missing region was accepted -- the key is not enforcing", id)
		}
		if _, err := c.Exec(context.Background(), `INSERT INTO notes VALUES (800 + $1, 'n', 'x')`, id); err != nil {
			t.Errorf("shard %d: a valid row was rejected: %v", id, err)
		}
	}
}

// A key pointing at anything other than a reference table cannot hold after
// the move: an unsharded table is on the home shard alone and a sharded one
// is split, so a row and the row it references can land apart. Both are
// refused, and the refusal says which table and what placement it has.
func TestAMoveRefusesAForeignKeyToANonReferenceTable(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE lookup (code text PRIMARY KEY)`)
	mustExec(t, src, `INSERT INTO lookup VALUES ('a')`)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, code text NOT NULL REFERENCES lookup(code))`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'lookup', 'unsharded')`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'lookup', 'unsharded')`)
	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()

	ctx := context.Background()
	state, msg := "", ""
	for range 40 {
		if _, err := f.placer.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		_, state, _, msg = f.workflow("notes")
		if state == StateFailed {
			break
		}
	}
	if state != StateFailed {
		t.Fatalf("a move with a key to an unsharded table must be refused, got %s %q", state, msg)
	}
	for _, want := range []string{"public.lookup", "unsharded", "reference table"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal must mention %q: %q", want, msg)
		}
	}
}
