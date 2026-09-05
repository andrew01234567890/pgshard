package controller

import (
	"testing"
	"time"
)

// TestAMoveKeepsOwnerAndGrants is PGS-255's privileges class. A move used to
// REFUSE a table with a non-default owner or any table or column grant,
// because the shadow is built by the controller and carries neither: the
// swap would have handed the application's table to the controller's role
// with every grant gone -- an outage for the roles that read it, and a
// table nobody but the controller could alter.
func TestAMoveKeepsOwnerAndGrants(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	// Roles are per PostgreSQL instance, so every shard needs them.
	for id := range int32(2) {
		mustExec(t, f.app(id), `CREATE ROLE app_owner`)
		mustExec(t, f.app(id), `CREATE ROLE reader`)
		mustExec(t, f.app(id), `CREATE ROLE writer`)
	}
	src := f.app(0)
	mustExec(t, src, `CREATE TABLE notes (id bigint PRIMARY KEY, body text, secret text)`)
	mustExec(t, src, `INSERT INTO notes SELECT g, 'b' || g, 's' || g FROM generate_series(1, 12) g`)
	mustExec(t, src, `ALTER TABLE notes OWNER TO app_owner`)
	mustExec(t, src, `GRANT SELECT ON notes TO reader`)
	mustExec(t, src, `GRANT SELECT, INSERT, UPDATE ON notes TO writer WITH GRANT OPTION`)
	mustExec(t, src, `GRANT UPDATE (body) ON notes TO reader`)

	mustExec(t, f.catalog, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notes', 'unsharded')`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.tables SET placement = 'reference' WHERE table_name = 'notes'`)
	f.reconcile()
	mustExec(t, f.catalog, `UPDATE pgshard.workflows SET spec = spec || '{"drop_old_after_seconds": 0}'`)
	f.driveUntil("notes", 2*time.Minute, StageCompleted)

	for id := range int32(2) {
		c := f.app(id)
		if n := queryOne[int64](t, c, `SELECT count(*) FROM notes`); n != 12 {
			t.Errorf("shard %d: %d rows", id, n)
		}
		if got := queryOne[string](t, c, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.notes'::regclass`); got != "app_owner" {
			t.Errorf("shard %d: owner = %q, want app_owner", id, got)
		}
		// The privileges themselves, asked of PostgreSQL rather than read
		// out of relacl: has_table_privilege answers the question the
		// application asks.
		for _, want := range []struct {
			role, priv string
			have       bool
		}{
			{"reader", "SELECT", true},
			{"reader", "INSERT", false},
			{"writer", "SELECT", true},
			{"writer", "INSERT", true},
			{"writer", "UPDATE", true},
			{"writer", "DELETE", false},
		} {
			got := queryOne[bool](t, c, `SELECT has_table_privilege($1, 'public.notes', $2)`, want.role, want.priv)
			if got != want.have {
				t.Errorf("shard %d: has_table_privilege(%s, %s) = %v, want %v", id, want.role, want.priv, got, want.have)
			}
		}
		// The column grant, and that it did not become a table-wide one.
		if got := queryOne[bool](t, c, `SELECT has_column_privilege('reader', 'public.notes', 'body', 'UPDATE')`); !got {
			t.Errorf("shard %d: reader lost UPDATE (body)", id)
		}
		if got := queryOne[bool](t, c, `SELECT has_column_privilege('reader', 'public.notes', 'secret', 'UPDATE')`); got {
			t.Errorf("shard %d: reader gained UPDATE on a column it was never granted -- a column grant became a table grant", id)
		}
		// WITH GRANT OPTION is part of the grant, not decoration.
		if got := queryOne[bool](t, c, `SELECT has_table_privilege('writer', 'public.notes', 'SELECT WITH GRANT OPTION')`); !got {
			t.Errorf("shard %d: writer lost the grant option", id)
		}
		if got := queryOne[bool](t, c, `SELECT has_table_privilege('reader', 'public.notes', 'SELECT WITH GRANT OPTION')`); got {
			t.Errorf("shard %d: reader gained a grant option it never had", id)
		}
	}
}
