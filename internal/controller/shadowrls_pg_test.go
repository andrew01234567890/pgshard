package controller

import (
	"testing"
	"time"
)

// TestAMoveKeepsRowLevelSecurity is PGS-255's first object class. A move
// used to REFUSE a table with row-level security, because the shadow is
// built with LIKE INCLUDING ALL, which carries neither the policies nor
// the flags that make them apply -- a swap would have left the table
// looking correct and enforcing nothing.
//
// Policies are created on the shadow while row-level security is still off
// and the swap turns it on, so that the copy cannot be filtered by the
// policies it is copying. WHAT THIS TEST DOES NOT PROVE: the fixture's
// copier connects as a superuser, which bypasses row-level security
// whatever the flags say, so enabling it early does not fail this test --
// checked, by doing it. The ordering is kept because whether the copy is
// filtered otherwise depends on the role the copier happens to hold
// (pgshard's own DDL role is NOBYPASSRLS), and because a shadow is not yet
// the table clients see, so nothing should be enforcing on it.
func TestAMoveKeepsRowLevelSecurity(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL, region text NOT NULL)`)
	mustExec(t, src, `INSERT INTO notes SELECT g, 'b' || g, 'r' || (g % 5) FROM generate_series(1, 50) g`)
	mustExec(t, src, `CREATE POLICY notes_region ON notes FOR ALL TO PUBLIC USING (region = 'r1')`)
	mustExec(t, src, `CREATE POLICY notes_not_r0 ON notes AS RESTRICTIVE FOR SELECT TO PUBLIC USING (region <> 'r0')`)
	mustExec(t, src, `ALTER TABLE notes ENABLE ROW LEVEL SECURITY`)
	mustExec(t, src, `ALTER TABLE notes FORCE ROW LEVEL SECURITY`)

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("notes", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		// Every row arrived. Read as a superuser, so this counts what is
		// there rather than what a policy allows.
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 50 {
			t.Errorf("shard %d: %d rows, want all 50", id, n)
		}
		if got := queryOne[bool](t, c, `SELECT relrowsecurity FROM pg_class WHERE oid = 'public.notes'::regclass`); !got {
			t.Errorf("shard %d: row-level security is not enabled", id)
		}
		if got := queryOne[bool](t, c, `SELECT relforcerowsecurity FROM pg_class WHERE oid = 'public.notes'::regclass`); !got {
			t.Errorf("shard %d: row-level security is not forced", id)
		}
		// The definitions, not just the count: a policy recreated as
		// PERMISSIVE where it was RESTRICTIVE, or for the wrong command,
		// enforces something else and still looks present.
		for _, want := range []struct{ name, cmd, permissive, qual string }{
			{"notes_not_r0", "r", "false", "(region <> 'r0'::text)"},
			{"notes_region", "*", "true", "(region = 'r1'::text)"},
		} {
			got := queryOne[string](t, c, `SELECT polcmd::text || ' ' || polpermissive::text || ' ' || coalesce(pg_get_expr(polqual, polrelid), '')
				FROM pg_policy WHERE polrelid = 'public.notes'::regclass AND polname = $1`, want.name)
			if exp := want.cmd + " " + want.permissive + " " + want.qual; got != exp {
				t.Errorf("shard %d: policy %s = %q, want %q", id, want.name, got, exp)
			}
		}
		// And it enforces. A superuser bypasses row-level security whatever
		// the flags say, so the check has to be made as someone else.
		mustExec(t, c, `CREATE ROLE notes_reader`)
		mustExec(t, c, `GRANT SELECT ON notes TO notes_reader`)
		mustExec(t, c, `SET ROLE notes_reader`)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 10 {
			t.Errorf("shard %d: a reader sees %d rows, want the 10 the policy allows", id, n)
		}
		mustExec(t, c, `RESET ROLE`)
	}
}
