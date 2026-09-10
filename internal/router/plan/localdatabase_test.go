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
	if _, err := p.Plan(context.Background(), session(shared), `COMMENT ON COLUMN items.name IS 'x'`); err == nil {
		t.Fatal("COMMENT in an ordinary database must still be refused; there is more than one place it would have to run")
	} else if !strings.Contains(err.Error(), "not supported through the router") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}
