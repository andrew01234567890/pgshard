package controller

import (
	"context"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestAResumedFunctionMigrationRecognisesTheFunctionItMade (PGS-867): a
// plain CREATE FUNCTION fails if it runs again, so a migration resumed after
// a crash has to recognise the function by the signature the router
// recorded, in the form the planner renders it.
func TestAResumedFunctionMigrationRecognisesTheFunctionItMade(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t))
	mustExec(t, conn, `CREATE SCHEMA app`)
	mustExec(t, conn, `CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS 'begin return new; end'`)
	mustExec(t, conn, `CREATE FUNCTION app.f(a int, b text[], OUT c int) LANGUAGE sql AS 'select 1'`)
	shard := pgxShardConn{conn}
	for _, c := range []struct {
		sig, expect string
		want        bool
	}{
		{`"f"()`, "present", true},
		{`"app"."f"("pg_catalog"."int4", "text"[])`, "present", true},
		{`"app"."f"("pg_catalog"."int4")`, "present", false},
		{`"g"()`, "absent", true},
		{`"f"()`, "absent", false},
	} {
		got, err := objectMatches(ctx, shard, catalog.MigrationObject{Kind: "function", Name: c.sig, Expect: c.expect})
		if err != nil {
			t.Fatalf("%s: %v", c.sig, err)
		}
		if got != c.want {
			t.Errorf("function %s expected %s: matches = %v, want %v", c.sig, c.expect, got, c.want)
		}
	}
}

// TestTheApplierRunsFunctionTriggerAndCommentDDLAsTheClient (PGS-867): the
// statements pgroll's start and complete send -- a trigger function, the
// trigger that calls it, a column comment, and the DROP FUNCTION ... CASCADE
// that removes both -- run through the applier as the client's role on the
// shard, as any other migration does.
func TestTheApplierRunsFunctionTriggerAndCommentDDLAsTheClient(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	run := func(id, kind, sql string, obj catalog.MigrationObject) {
		t.Helper()
		store.migrations = []catalog.DDLMigration{{ID: id, Database: "postgres", Statement: sql, Kind: kind, Strategy: "direct", Scope: "all",
			State: catalog.MigrationQueued, Meta: catalog.MigrationMeta{RunAs: "appowner", Object: obj}}}
		if _, err := a.RunOnce(ctx); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		store.mu.Lock()
		m := store.migrations[0]
		store.mu.Unlock()
		if m.State != catalog.MigrationComplete {
			t.Fatalf("%s: %s %q %+v", sql, m.State, m.Error, m.PerShard)
		}
	}
	run("20000000-0000-0000-0000-0000000000f1", "CREATE FUNCTION",
		`CREATE FUNCTION _pgroll_trigger_accounts_note() RETURNS trigger LANGUAGE plpgsql AS 'begin new.amount = upper(new.amount); return new; end'`,
		catalog.MigrationObject{Kind: "function", Name: `"_pgroll_trigger_accounts_note"()`, Expect: "present"})
	run("20000000-0000-0000-0000-0000000000f2", "CREATE TRIGGER",
		`CREATE TRIGGER _pgroll_trigger_accounts_note BEFORE INSERT OR UPDATE ON accounts FOR EACH ROW EXECUTE PROCEDURE _pgroll_trigger_accounts_note()`,
		catalog.MigrationObject{})
	run("20000000-0000-0000-0000-0000000000f3", "COMMENT", `COMMENT ON COLUMN accounts.amount IS 'migrated'`, catalog.MigrationObject{})

	if _, err := pool.Exec(ctx, `INSERT INTO accounts (id, tenant_id, amount) VALUES (-1, 1, 'abc')`); err != nil {
		t.Fatal(err)
	}
	var amount, comment string
	if err := pool.QueryRow(ctx, `SELECT amount, col_description('accounts'::regclass, 3) FROM accounts WHERE id = -1`).Scan(&amount, &comment); err != nil {
		t.Fatal(err)
	}
	if amount != "ABC" || comment != "migrated" {
		t.Fatalf("after the migrations the trigger wrote %q and the comment is %q", amount, comment)
	}

	run("20000000-0000-0000-0000-0000000000f4", "DROP FUNCTION", `DROP FUNCTION IF EXISTS _pgroll_trigger_accounts_note() CASCADE`,
		catalog.MigrationObject{Kind: "function", Name: `"_pgroll_trigger_accounts_note"()`, Expect: "absent"})
	var left int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM pg_proc WHERE proname = '_pgroll_trigger_accounts_note') + (SELECT count(*) FROM pg_trigger WHERE tgname = '_pgroll_trigger_accounts_note')`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("DROP FUNCTION ... CASCADE left %d objects: %v", left, err)
	}
}
