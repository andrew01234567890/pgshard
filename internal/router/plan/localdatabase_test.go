package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// localFixture is the standard fixture with the database declared local:
// every object lives on the home shard, so nothing has to be fanned out.
func localFixture(t testing.TB) *snapshot.Snapshot {
	t.Helper()
	s := fixture(t)
	for k := range s.Tables {
		if k.Database == fixtureDB {
			delete(s.Tables, k)
		}
	}
	db := s.Databases[fixtureDB]
	db.LocalOnly = true
	s.Databases[fixtureDB] = db
	return s
}

// TestALocalDatabaseRunsItsOwnDDL.
//
// pgshard routes DDL through a catalog migration a controller applies to
// every shard in that shard's own transaction. Because that fan-out is not
// atomic it cannot honour the client's ROLLBACK, so DDL inside BEGIN/COMMIT
// is refused -- and statements that cannot be fanned out at all (CREATE
// FUNCTION, CREATE TRIGGER, COMMENT, CREATE EVENT TRIGGER, a writable CTE)
// are refused outright.
//
// None of those refusals are about the statement. They are about there
// being more than one place to run it. A database declared local has one,
// and every one of them becomes a plain statement on the home shard, in the
// client's own transaction, rolled back by PostgreSQL like anything else.
//
// This is what a migration tool that keeps its state in the database it
// migrates -- pgroll, sqitch, atlas, flyway -- needs in order to run at all.
func TestALocalDatabaseRunsItsOwnDDL(t *testing.T) {
	p := New()
	local, shared := localFixture(t), fixture(t)

	for _, sql := range []string{
		`CREATE SCHEMA pgroll`,
		`CREATE TABLE pgroll.migrations (name text primary key)`,
		`ALTER TABLE items ADD COLUMN _pgroll_new_name text`,
		`CREATE OR REPLACE FUNCTION _pgroll_trigger_items() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER _pgroll_trigger_items BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION _pgroll_trigger_items()`,
		`COMMENT ON COLUMN items.name IS 'x'`,
		`DROP FUNCTION IF EXISTS _pgroll_trigger_items CASCADE`,
		`CREATE EVENT TRIGGER pg_roll_handle_ddl ON ddl_command_end EXECUTE FUNCTION pgroll.raw_migration()`,
		`CREATE VIEW public_01_add.items AS SELECT id, name FROM public.items`,
		`WITH batch AS (SELECT id FROM items ORDER BY id LIMIT 10 FOR NO KEY UPDATE), upd AS (UPDATE items SET name = 'x' FROM batch WHERE items.id = batch.id RETURNING items.id) SELECT count(*) FROM upd`,
	} {
		pl, err := p.Plan(context.Background(), session(local), sql)
		if err != nil {
			t.Errorf("refused in a local database: %.60s\n  %v", sql, err)
			continue
		}
		if pl.Kind != Unsharded {
			t.Errorf("%.60s planned %v, want Unsharded: it must run on the client's own connection", sql, pl.Kind)
		}
		if pl.Migration != nil {
			t.Errorf("%.60s was queued as a migration; a local database has one shard and no fan-out to converge", sql)
		}
		if len(pl.Shards) != 1 || pl.Shards[0] != local.Databases[fixtureDB].HomeShard {
			t.Errorf("%.60s routed to %v, want the home shard", sql, pl.Shards)
		}
	}

	// The same statements in an ordinary database are unchanged: DDL is
	// still a migration, and what cannot be fanned out is still refused.
	pl, err := p.Plan(context.Background(), session(shared), `CREATE SCHEMA pgroll`)
	if err != nil {
		t.Fatal(err)
	}
	if pl.Migration == nil {
		t.Fatal("CREATE SCHEMA in an ordinary database must still be a fanned-out migration")
	}
	if _, err := p.Plan(context.Background(), session(shared), `CREATE EVENT TRIGGER e ON ddl_command_end EXECUTE FUNCTION f()`); err == nil {
		t.Fatal("CREATE EVENT TRIGGER in an ordinary database must still be refused; there is more than one place it would have to run")
	} else if !strings.Contains(err.Error(), "not supported through the router") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// DDL a local database runs on its home shard is marked, so the executor can
// refuse it while the operation queue holds something it would overlap; the
// same statement in a shared database is a migration, which waits instead.
func TestALocalDatabasesDDLIsMarkedAsHomeDDL(t *testing.T) {
	p := New()
	for _, c := range []struct {
		snap    *snapshot.Snapshot
		homeDDL bool
	}{{localFixture(t), true}, {fixture(t), false}} {
		pl, err := p.Plan(context.Background(), session(c.snap), `ALTER TABLE items ADD COLUMN extra int`)
		if err != nil || pl.HomeDDL != c.homeDDL {
			t.Fatalf("HomeDDL = %v (%v), want %v", pl.HomeDDL, err, c.homeDDL)
		}
	}
	if pl, err := p.Plan(context.Background(), session(localFixture(t)), `SELECT 1`); err != nil || pl.HomeDDL {
		t.Fatalf("a query in a local database is marked as DDL: %v", err)
	}

	// Every path that sends a local database's statement to its home shard,
	// so that none of them can lose the marking unnoticed: the migration
	// fallthrough, ALTER TABLE, the unfannable objects, and the
	// unrecognised-statement fallback.
	for _, sql := range []string{
		`CREATE TABLE more (id int)`,
		`ALTER TABLE items ADD COLUMN extra2 int`,
		`ALTER FUNCTION f() RENAME TO g`,
		`CREATE EVENT TRIGGER e ON ddl_command_end EXECUTE FUNCTION f()`,
	} {
		pl, err := p.Plan(context.Background(), session(localFixture(t)), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if !pl.HomeDDL {
			t.Errorf("%s: schema change not marked, so the executor cannot refuse it while a reshard runs", sql)
		}
	}

	// CALL and DO reach the same fallback and are not schema changes.
	// Marking them refused every stored procedure a session calls for as
	// long as any reshard in the cluster ran.
	for _, sql := range []string{
		`CALL recompute_totals(1)`,
		`DO $$ BEGIN PERFORM 1; END $$`,
	} {
		pl, err := p.Plan(context.Background(), session(localFixture(t)), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.HomeDDL {
			t.Errorf("%s: marked as a schema change, so a reshard would refuse it for hours", sql)
		}
	}
}
