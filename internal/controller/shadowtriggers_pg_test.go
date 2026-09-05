package controller

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAMoveKeepsTriggers is PGS-255's second object class. A move used to
// REFUSE a table with a user trigger, because the shadow is built with LIKE
// INCLUDING ALL, which carries none.
//
// The order matters as much as the reproduction. Triggers are created on
// the shadow and immediately disabled, so the copy writes through them: a
// BEFORE trigger would rewrite every copied row and an AFTER trigger would
// fire for a row the source has already fired on. The swap restores each to
// the state the source had.
func TestAMoveKeepsTriggers(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL, stamped text)`)
	mustExec(t, src, `CREATE TABLE audit (n bigint)`)
	// The function has to exist on every target, so it is created on both.
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE FUNCTION stamp() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN NEW.stamped := 'trigger'; RETURN NEW; END $$`)
		mustExec(t, f.app(id), `CREATE FUNCTION count_row() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN INSERT INTO audit VALUES (1); RETURN NULL; END $$`)
		if id > 0 {
			mustExec(t, f.app(id), `CREATE TABLE audit (n bigint)`)
		}
	}
	mustExec(t, src, `CREATE TRIGGER stamp_it BEFORE INSERT ON notes FOR EACH ROW EXECUTE FUNCTION stamp()`)
	mustExec(t, src, `CREATE TRIGGER count_it AFTER INSERT ON notes FOR EACH ROW EXECUTE FUNCTION count_row()`)
	mustExec(t, src, `CREATE TRIGGER off_it BEFORE UPDATE ON notes FOR EACH ROW EXECUTE FUNCTION stamp()`)
	mustExec(t, src, `ALTER TABLE notes DISABLE TRIGGER off_it`)
	// Rows written before the move carry what the source's trigger set.
	mustExec(t, src, `INSERT INTO notes (id, body) SELECT g, 'b' || g FROM generate_series(1, 20) g`)
	mustExec(t, src, `UPDATE notes SET stamped = 'source' WHERE id % 2 = 0`)
	// Those inserts fired the source's own AFTER trigger, so shard 0's
	// audit table is not empty and the copy has to leave it exactly as it
	// is. Shard 1 starts at zero.
	auditBefore := map[int32]int64{0: queryOne[int64](t, src, `SELECT count(*) FROM audit`), 1: 0}
	if auditBefore[0] != 20 {
		t.Fatalf("the fixture must have fired the source trigger 20 times, got %d", auditBefore[0])
	}

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("notes", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 20 {
			t.Errorf("shard %d: %d rows", id, n)
		}
		// The copy did not fire the triggers: every row still says what the
		// source said, and nothing was appended to audit.
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes WHERE stamped = 'source'`); n != 10 {
			t.Errorf("shard %d: %d rows still stamped by the source, want 10 -- a trigger fired during the copy", id, n)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM audit`); n != auditBefore[id] {
			t.Errorf("shard %d: audit has %d rows, want the %d it had before the move -- the AFTER trigger fired during the copy",
				id, n, auditBefore[id])
		}
		// Every trigger is back, in the state the source had.
		for _, want := range []struct{ name, enabled string }{
			{"stamp_it", "O"}, {"count_it", "O"}, {"off_it", "D"},
		} {
			got := queryOne[string](t, c, `SELECT tgenabled::text FROM pg_trigger WHERE tgrelid = 'public.notes'::regclass AND tgname = $1`, want.name)
			if got != want.enabled {
				t.Errorf("shard %d: trigger %s is %q, want %q", id, want.name, got, want.enabled)
			}
		}
		// And they fire: the moved table enforces what it enforced before.
		mustExec(t, c, `INSERT INTO notes (id, body) VALUES (100 + `+itoa(int64(id))+`, 'after')`)
		if got := queryOne[string](t, c, `SELECT stamped FROM notes WHERE id = 100 + `+itoa(int64(id))); got != "trigger" {
			t.Errorf("shard %d: the BEFORE trigger did not fire on the moved table: stamped = %q", id, got)
		}
		if n := queryOne[int64](t, c, `SELECT count(*) FROM audit`); n != auditBefore[id]+1 {
			t.Errorf("shard %d: the AFTER trigger did not fire on the moved table: %d audit rows, want %d", id, n, auditBefore[id]+1)
		}
	}
}

// TestAMoveRefusesATriggerFunctionTheTargetsLack: a trigger is reproduced by
// its definition, and the definition names a function. pgshard does not fan
// out function DDL, so one created on a single shard exists only there --
// and a CREATE TRIGGER naming a missing one would fail in the middle of
// building the shadow, where the workflow retries against a condition that
// will not change on its own.
func TestAMoveRefusesATriggerFunctionTheTargetsLack(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, body text)`)
	mustExec(t, src, `CREATE FUNCTION only_here() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`)
	mustExec(t, src, `CREATE TRIGGER t1 BEFORE INSERT ON notes FOR EACH ROW EXECUTE FUNCTION only_here()`)

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
	if state != StateFailed || !strings.Contains(msg, "only_here") {
		t.Fatalf("expected a refusal naming the function, got %s %q", state, msg)
	}
	if !strings.Contains(msg, "create them there") {
		t.Errorf("the refusal must say what to do: %q", msg)
	}
}
