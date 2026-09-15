package controller

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// dropSchemaWatch records, at the first save that carries a DROP's
// resolved schema, whether the object it names still existed.
type dropSchemaWatch struct {
	*memStore
	pool            *pgxpool.Pool
	recorded        string
	existedAtRecord bool
}

func (w *dropSchemaWatch) Save(ctx context.Context, m catalog.DDLMigration, term int64) error {
	if ds := m.PerShard["0"].DropSchema; ds != "" && w.recorded == "" {
		w.recorded = ds
		_ = w.pool.QueryRow(ctx, `SELECT to_regclass($1 || '.dropped') IS NOT NULL`, ds).Scan(&w.existedAtRecord)
	}
	return w.memStore.Save(ctx, m, term)
}

// TestAResumedDropDoesNotDropTheSameNameLaterInThePath (PGS-879): DROP TABLE
// t with search_path app, public drops app.t. If the applier died after that
// commit and before saving progress, the resumed step looked for "t"
// anywhere in the path, found public.t, decided the DROP had not run, and
// ran it again -- dropping public.t. The schema the name resolved to is now
// recorded before the statement is sent, and a resume checks that schema.
func TestAResumedDropDoesNotDropTheSameNameLaterInThePath(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `CREATE SCHEMA app`)
	mustExecSQL(t, pool, `GRANT USAGE, CREATE ON SCHEMA app TO appowner`)
	mustExecSQL(t, pool, `CREATE TABLE app.dropped (id int)`)
	mustExecSQL(t, pool, `ALTER TABLE app.dropped OWNER TO appowner`)
	mustExecSQL(t, pool, `CREATE TABLE public.dropped (id int)`)
	mustExecSQL(t, pool, `ALTER TABLE public.dropped OWNER TO appowner`)
	watch := &dropSchemaWatch{memStore: store, pool: pool}
	a.Store = watch
	drop := func(id string, state catalog.ShardMigration) catalog.DDLMigration {
		t.Helper()
		store.migrations = []catalog.DDLMigration{{ID: id, Database: "postgres", Statement: `DROP TABLE dropped`, Kind: "DROP TABLE",
			Strategy: "direct", Scope: "existing", State: catalog.MigrationRunning, PerShard: map[string]catalog.ShardMigration{"0": state},
			Meta: catalog.MigrationMeta{RunAs: "appowner", SearchPath: "app, public",
				Object: catalog.MigrationObject{Kind: "relation", Name: "dropped", Expect: "absent"}}}}
		if _, err := a.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.migrations[0]
	}
	exists := func(schema string) bool {
		t.Helper()
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1 || '.dropped') IS NOT NULL`, schema).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}

	m := drop("30000000-0000-0000-0000-000000000001", catalog.ShardMigration{State: catalog.ShardPending})
	if m.State != catalog.MigrationComplete || exists("app") || !exists("public") {
		t.Fatalf("the DROP: %s %+v, app.dropped %v, public.dropped %v; want app's dropped and public's kept", m.State, m.PerShard, exists("app"), exists("public"))
	}
	if watch.recorded != "app" || !watch.existedAtRecord {
		t.Fatalf("recorded schema %q (object existed then: %v), want app recorded before the DROP ran", watch.recorded, watch.existedAtRecord)
	}

	// The same DROP, resumed as if the process died after it committed:
	// app.dropped is gone, public.dropped is what the name now resolves to.
	m = drop("30000000-0000-0000-0000-000000000002", catalog.ShardMigration{State: catalog.ShardRunning, DropSchema: "app"})
	if !exists("public") {
		t.Fatalf("a resumed DROP dropped public.dropped, a table the migration never touched (migration %s)", m.State)
	}
}

// TestAResumedCreateLooksWhereItCreates: CREATE TABLE t with search_path
// app, public creates app.t. Resumed, the step found public.t, called the
// CREATE applied, and app.t was never made.
func TestAResumedCreateLooksWhereItCreates(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `CREATE SCHEMA app`)
	mustExecSQL(t, pool, `GRANT USAGE, CREATE ON SCHEMA app TO appowner`)
	mustExecSQL(t, pool, `CREATE TABLE public.made (id int)`)
	store.migrations = []catalog.DDLMigration{{ID: "30000000-0000-0000-0000-000000000003", Database: "postgres", Statement: `CREATE TABLE made (id int)`,
		Kind: "CREATE TABLE", Strategy: "direct", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardRunning}},
		Meta: catalog.MigrationMeta{RunAs: "appowner", SearchPath: "app, public",
			Object: catalog.MigrationObject{Kind: "relation", Name: "made", Expect: "present"}}}}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var made bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('app.made') IS NOT NULL`).Scan(&made); err != nil {
		t.Fatal(err)
	}
	if !made {
		t.Fatal("a resumed CREATE TABLE found public.made and never created app.made")
	}
}

// TestAResumedCreateIndexFindsItInItsTablesSchema: an index is created in
// its table's schema, not in current_schema(). A resumed CREATE INDEX on a
// table later in the path that looked only in current_schema() ran again
// and failed on the index it had made.
func TestAResumedCreateIndexFindsItInItsTablesSchema(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `CREATE SCHEMA app`)
	mustExecSQL(t, pool, `GRANT USAGE, CREATE ON SCHEMA app TO appowner`)
	mustExecSQL(t, pool, `CREATE TABLE public.indexed (id int)`)
	mustExecSQL(t, pool, `ALTER TABLE public.indexed OWNER TO appowner`)
	mustExecSQL(t, pool, `CREATE INDEX indexed_id ON public.indexed (id)`)
	store.migrations = []catalog.DDLMigration{{ID: "30000000-0000-0000-0000-000000000004", Database: "postgres", Statement: `CREATE INDEX indexed_id ON indexed (id)`,
		Kind: "CREATE INDEX", Strategy: "direct", Scope: "all", State: catalog.MigrationRunning,
		PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardRunning}},
		Meta: catalog.MigrationMeta{RunAs: "appowner", SearchPath: "app, public",
			Object: catalog.MigrationObject{Kind: "relation", Name: "indexed_id", Expect: "present"}}}}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	m := store.migrations[0]
	store.mu.Unlock()
	if m.State != catalog.MigrationComplete {
		t.Fatalf("a resumed CREATE INDEX whose index exists in its table's schema is %s: %+v", m.State, m.PerShard)
	}
}

// TestAResumedCreateIndexIsLookedForOnItsTable (PGS-887): a resumed CREATE
// INDEX looked for any relation of the index's name in the search path, so
// a table of that name in an earlier schema made it report the index built
// when it never was. The index is now looked for through pg_index on the
// table the statement named; one left invalid there is rebuilt.
func TestAResumedCreateIndexIsLookedForOnItsTable(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `CREATE SCHEMA app`)
	mustExecSQL(t, pool, `GRANT USAGE, CREATE ON SCHEMA app TO appowner`)
	mustExecSQL(t, pool, `CREATE TABLE app.indexed_id (x int)`)
	mustExecSQL(t, pool, `CREATE TABLE public.indexed (id int)`)
	mustExecSQL(t, pool, `ALTER TABLE public.indexed OWNER TO appowner`)
	resume := func(id string) catalog.DDLMigration {
		t.Helper()
		store.migrations = []catalog.DDLMigration{{ID: id, Database: "postgres", Statement: `CREATE INDEX indexed_id ON indexed (id)`,
			Kind: "CREATE INDEX", Strategy: "direct", Scope: "all", State: catalog.MigrationRunning,
			PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardRunning}},
			Meta: catalog.MigrationMeta{RunAs: "appowner", SearchPath: "app, public",
				Object: catalog.MigrationObject{Kind: "relation", Name: "indexed_id", Table: "indexed", Expect: "present"}}}}
		if _, err := a.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.migrations[0]
	}
	onTable := func() (exists, valid bool) {
		t.Helper()
		err := pool.QueryRow(ctx, `SELECT count(*) > 0, coalesce(bool_and(i.indisvalid), false) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE i.indrelid = 'public.indexed'::regclass AND c.relname = 'indexed_id'`).Scan(&exists, &valid)
		if err != nil {
			t.Fatal(err)
		}
		return exists, valid
	}

	if m := resume("30000000-0000-0000-0000-000000000005"); m.State != catalog.MigrationComplete {
		t.Fatalf("resumed CREATE INDEX: %s %+v", m.State, m.PerShard)
	}
	if exists, _ := onTable(); !exists {
		t.Fatal("a table named like the index made the resumed CREATE INDEX report it built; it is not on public.indexed")
	}

	mustExecSQL(t, pool, `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.indexed_id'::regclass`)
	if m := resume("30000000-0000-0000-0000-000000000006"); m.State != catalog.MigrationComplete {
		t.Fatalf("resumed CREATE INDEX over an invalid index: %s %+v", m.State, m.PerShard)
	}
	if exists, valid := onTable(); !exists || !valid {
		t.Fatalf("an invalid index on the table was not rebuilt: exists %v valid %v", exists, valid)
	}
	var kept int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = 'indexed_id' AND relnamespace = 'app'::regnamespace AND relkind = 'r'`).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("the table in app named like the index was touched: %d %v", kept, err)
	}
}
