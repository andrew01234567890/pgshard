package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

const rewriteRows = 10000

func rewritePGFixture(t *testing.T) (*pgxpool.Pool, *Applier, *memStore) {
	t.Helper()
	dsn := startPostgres(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExecSQL(t, pool, `CREATE ROLE appowner LOGIN NOSUPERUSER`)
	mustExecSQL(t, pool, `CREATE TABLE accounts (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, amount text NOT NULL DEFAULT '0')`)
	mustExecSQL(t, pool, `ALTER TABLE accounts OWNER TO appowner`)
	mustExecSQL(t, pool, `GRANT CREATE ON SCHEMA public TO appowner`)
	mustExecSQL(t, pool, `INSERT INTO accounts (id, tenant_id, amount) SELECT g, g % 7, g::text FROM generate_series(1, `+fmt.Sprint(rewriteRows)+`) g`)
	store := &memStore{shards: []int32{0}, dbs: []string{"postgres"}}
	a := &Applier{Store: store, RewriteSettle: -1,
		Shards: &PgxShardDialer{Pool: pool, DSNs: map[ShardRef]string{{Set: "default", ID: 0}: dsn}}}
	return pool, a, store
}

func mustExecSQL(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func tableOID(t *testing.T, pool *pgxpool.Pool) uint32 {
	t.Helper()
	var oid uint32
	if err := pool.QueryRow(context.Background(), `SELECT 'accounts'::regclass::oid`).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	return oid
}

func columnType(t *testing.T, pool *pgxpool.Pool, col string) string {
	t.Helper()
	var typ string
	if err := pool.QueryRow(context.Background(),
		`SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a WHERE a.attrelid = 'accounts'::regclass AND a.attname = $1`,
		col).Scan(&typ); err != nil {
		t.Fatal(err)
	}
	return typ
}

func hiddenArtifacts(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT attname FROM pg_attribute
		WHERE attrelid = 'accounts'::regclass AND NOT attisdropped AND attname LIKE '\_pgshard\_%'
		UNION ALL SELECT tgname FROM pg_trigger WHERE tgrelid = 'accounts'::regclass AND NOT tgisinternal
		UNION ALL SELECT proname FROM pg_proc WHERE proname LIKE '\_pgshard\_%'`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestRewriteTypeChangeOnPostgres converts a 10k-row text column to bigint
// online under concurrent reads and writes: no client error, values
// preserved and converted, pg_class.oid unchanged, no artifacts left.
func TestRewriteTypeChangeOnPostgres(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	oid := tableOID(t, pool)
	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000a1", Database: "postgres",
		Statement: "alter table accounts alter column amount type bigint using amount::bigint",
		Kind:      "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{RunAs: "appowner", Rewrite: &catalog.RewriteChange{Schema: "public", Table: "accounts",
			Column: "amount", NewType: "bigint", Using: "amount::bigint", BatchSize: 700}}}
	store.migrations = []catalog.DDLMigration{m}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes, reads, failures atomic.Int64
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := int64(r.Intn(rewriteRows) + 1)
				// Simple protocol: literals stay untyped, the way a
				// router client's text values arrive, so they fit the
				// column before and after the cutover.
				if r.Intn(2) == 0 {
					if _, err := pool.Exec(ctx, `UPDATE accounts SET amount = $1 WHERE id = $2`, pgx.QueryExecModeSimpleProtocol, fmt.Sprint(id*2), id); err != nil {
						t.Errorf("concurrent write: %v", err)
						failures.Add(1)
						return
					}
					writes.Add(1)
				} else {
					var n int
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE id = $1 AND amount IS NOT NULL`, pgx.QueryExecModeSimpleProtocol, id).Scan(&n); err != nil || n != 1 {
						t.Errorf("concurrent read: n=%d %v", n, err)
						failures.Add(1)
						return
					}
					reads.Add(1)
				}
			}
		}(int64(i))
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d concurrent statements failed", n)
	}
	if writes.Load() == 0 || reads.Load() == 0 {
		t.Fatalf("no concurrency: %d writes %d reads", writes.Load(), reads.Load())
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", got.State, got.Error)
	}
	if typ := columnType(t, pool, "amount"); typ != "bigint" {
		t.Fatalf("amount type = %s", typ)
	}
	if tableOID(t, pool) != oid {
		t.Fatal("pg_class.oid changed: the rewrite was not in place")
	}
	var n, bad int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE amount IS NULL) FROM accounts`).Scan(&n, &bad); err != nil {
		t.Fatal(err)
	}
	if n != rewriteRows || bad != 0 {
		t.Fatalf("rows = %d nulls = %d", n, bad)
	}
	var notNull bool
	if err := pool.QueryRow(ctx, `SELECT attnotnull FROM pg_attribute WHERE attrelid = 'accounts'::regclass AND attname = 'amount'`).Scan(&notNull); err != nil {
		t.Fatal(err)
	}
	if !notNull {
		t.Fatal("NOT NULL was not restored")
	}
	var def string
	if err := pool.QueryRow(ctx, `SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'accounts'::regclass`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(def, "0") {
		t.Fatalf("default = %q", def)
	}
	var v int64
	if err := pool.QueryRow(ctx, `SELECT amount FROM accounts WHERE id = 5`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 5 && v != 10 {
		t.Fatalf("value not preserved: %d", v)
	}
	if left := hiddenArtifacts(t, pool); len(left) != 0 {
		t.Fatalf("artifacts left: %v", left)
	}
}

// TestRewriteAddVolatileDefaultOnPostgres adds a uuid column with a
// volatile default online.
func TestRewriteAddVolatileDefaultOnPostgres(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000a2", Database: "postgres",
		Statement: "alter table accounts add column token uuid default gen_random_uuid()",
		Kind:      "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{RunAs: "appowner", Rewrite: &catalog.RewriteChange{Schema: "public", Table: "accounts",
			Column: "token", NewType: "uuid", Default: "gen_random_uuid()", Add: true, BatchSize: 900}}}
	store.migrations = []catalog.DDLMigration{m}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", got.State, got.Error)
	}
	var n, distinct int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE token IS NULL), count(DISTINCT token) FROM accounts`).Scan(&n, &distinct); err != nil {
		t.Fatal(err)
	}
	if n != 0 || distinct != rewriteRows {
		t.Fatalf("nulls = %d distinct = %d", n, distinct)
	}
	var def string
	if err := pool.QueryRow(ctx, `SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE a.attname = 'token'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(def, "gen_random_uuid") {
		t.Fatalf("default = %q", def)
	}
	var newRow struct{}
	_ = newRow
	if _, err := pool.Exec(ctx, `INSERT INTO accounts (id, tenant_id, amount) VALUES (999999, 1, '1')`); err != nil {
		t.Fatal(err)
	}
	var tok *string
	if err := pool.QueryRow(ctx, `SELECT token::text FROM accounts WHERE id = 999999`).Scan(&tok); err != nil {
		t.Fatal(err)
	}
	if tok == nil {
		t.Fatal("new row has no token")
	}
	if left := hiddenArtifacts(t, pool); len(left) != 0 {
		t.Fatalf("artifacts left: %v", left)
	}
}

// TestRewriteFailureRevertsOnPostgres fails the backfill on a
// non-convertible value: the migration fails, the table keeps its old
// column and stays fully usable, and no artifacts remain.
func TestRewriteFailureRevertsOnPostgres(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `UPDATE accounts SET amount = 'not a number' WHERE id = 4321`)
	oid := tableOID(t, pool)
	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000a3", Database: "postgres",
		Statement: "alter table accounts alter column amount type bigint using amount::bigint",
		Kind:      "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{RunAs: "appowner", Rewrite: &catalog.RewriteChange{Schema: "public", Table: "accounts",
			Column: "amount", NewType: "bigint", Using: "amount::bigint", BatchSize: 512}}}
	store.migrations = []catalog.DDLMigration{m}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationFailed || !strings.Contains(got.Error, "invalid input syntax") {
		t.Fatalf("state = %s error %q", got.State, got.Error)
	}
	if typ := columnType(t, pool, "amount"); typ != "text" {
		t.Fatalf("amount type = %s", typ)
	}
	if tableOID(t, pool) != oid {
		t.Fatal("oid changed")
	}
	if left := hiddenArtifacts(t, pool); len(left) != 0 {
		t.Fatalf("artifacts left after the revert: %v", left)
	}
	if _, err := pool.Exec(ctx, `UPDATE accounts SET amount = '7' WHERE id = 1`); err != nil {
		t.Fatalf("table unusable after the revert: %v", err)
	}
}

// TestRepackOnPostgres19 runs the repack step against a PostgreSQL 19
// server when its image is available.
func TestRepackOnPostgres19(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres19(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExecSQL(t, pool, `CREATE ROLE bigowner LOGIN NOSUPERUSER`)
	mustExecSQL(t, pool, `CREATE TABLE big (id bigint PRIMARY KEY, v text)`)
	mustExecSQL(t, pool, `ALTER TABLE big OWNER TO bigowner`)
	mustExecSQL(t, pool, `INSERT INTO big SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g`)
	mustExecSQL(t, pool, `DELETE FROM big WHERE id % 2 = 0`)
	store := &memStore{shards: []int32{0}}
	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000a4", Database: "postgres",
		Statement: "vacuum (full) big", Kind: "VACUUM", Strategy: catalog.StrategyRepack, Scope: "all",
		State: catalog.MigrationQueued, Meta: catalog.MigrationMeta{Repack: true, RunAs: "bigowner",
			Object: catalog.MigrationObject{Kind: "relation", Schema: "public", Name: "big", Expect: "present"}}}
	store.migrations = []catalog.DDLMigration{m}
	a := &Applier{Store: store, Shards: &PgxShardDialer{Pool: pool, DSNs: map[ShardRef]string{{Set: "default", ID: 0}: dsn}}}
	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationComplete {
		t.Fatalf("state = %s error %q", got.State, got.Error)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM big`).Scan(&n); err != nil || n != 1000 {
		t.Fatalf("rows = %d err %v", n, err)
	}
}

const pg19Image = "ghcr.io/andrew01234567890/pgshard-postgres:19"

func startPostgres19(t *testing.T) string {
	t.Helper()
	return startPostgresImage(t, pg19Image, nil)
}

// TestRewriteReturningStarHidesWorkingColumnOnPostgres executes the router's
// rewritten RETURNING * against a shard that carries a rewrite working
// column: the raw statement would leak it, the rewritten one must not.
func TestRewriteReturningStarHidesWorkingColumnOnPostgres(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExecSQL(t, pool, `CREATE TABLE accounts (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, amount text NOT NULL DEFAULT '0')`)
	mustExecSQL(t, pool, `ALTER TABLE accounts ADD COLUMN _pgshard_amount_deadbeef bigint`)
	mustExecSQL(t, pool, `INSERT INTO accounts (id, tenant_id, amount) VALUES (1, 1, '10')`)

	snap := &snapshot.Snapshot{
		Databases: map[string]catalog.Database{"postgres": {Name: "postgres", DefaultPlacement: "unsharded", HomeShard: 0}},
		Tables: map[snapshot.TableKey]snapshot.Placement{
			{Database: "postgres", SchemaName: "public", TableName: "accounts"}: {
				Placement:      "unsharded",
				HiddenColumns:  []string{"_pgshard_amount_deadbeef"},
				VisibleColumns: []string{"id", "tenant_id", "amount"},
			},
		},
	}
	sess := plan.Session{Database: "postgres", Snapshot: snap}
	planner := plan.New()

	leaks := func(sql string) bool {
		rows, err := pool.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		for _, fd := range rows.FieldDescriptions() {
			if strings.HasPrefix(fd.Name, "_pgshard_") {
				return true
			}
		}
		return false
	}
	if !leaks(`UPDATE accounts SET amount = '20' WHERE id = 1 RETURNING *`) {
		t.Fatal("raw RETURNING * did not include the working column; fixture is wrong")
	}
	for _, sql := range []string{
		`UPDATE accounts SET amount = '30' WHERE id = 1 RETURNING *`,
		`INSERT INTO accounts (id, tenant_id, amount) VALUES (2, 1, '5') RETURNING *`,
		`DELETE FROM accounts WHERE id = 2 RETURNING *`,
	} {
		pl, err := planner.Plan(ctx, sess, sql)
		if err != nil {
			t.Fatalf("plan %s: %v", sql, err)
		}
		if pl.Rewritten == "" {
			t.Fatalf("%s was not rewritten", sql)
		}
		if leaks(pl.Rewritten) {
			t.Fatalf("rewritten %q leaked the working column", pl.Rewritten)
		}
	}
}

// TestRewriteRefusesDependentColumnOnPostgres: an online type rewrite whose
// target column carries a dependent object the cutover cannot recreate (here a
// UNIQUE index) is refused before any schema change, leaving the table intact.
func TestRewriteRefusesDependentColumnOnPostgres(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	mustExecSQL(t, pool, `CREATE UNIQUE INDEX accounts_amount_key ON accounts (lower(amount))`)
	oid := tableOID(t, pool)
	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000d1", Database: "postgres",
		Statement: "alter table accounts alter column amount type bigint using amount::bigint",
		Kind:      "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{RunAs: "appowner", Rewrite: &catalog.RewriteChange{Schema: "public", Table: "accounts",
			Column: "amount", NewType: "bigint", Using: "amount::bigint", BatchSize: 700}}}
	store.migrations = []catalog.DDLMigration{m}

	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationFailed || !strings.Contains(got.Error, "dependent objects") {
		t.Fatalf("state = %s error %q; want failed with a dependency refusal", got.State, got.Error)
	}
	// The column, its type, the table OID and the unique index are untouched.
	if typ := columnType(t, pool, "amount"); typ != "text" {
		t.Fatalf("amount type changed to %s; the rewrite must not have started", typ)
	}
	if tableOID(t, pool) != oid {
		t.Fatal("table OID changed; the rewrite touched the table")
	}
	var idx int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE tablename = 'accounts' AND indexname = 'accounts_amount_key'`).Scan(&idx); err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Fatal("the dependent unique index was dropped")
	}
	if left := hiddenArtifacts(t, pool); len(left) != 0 {
		t.Fatalf("artifacts left behind: %v", left)
	}
}

// TestRewriteBackfillBatchesWithoutASinglePrimaryKey: only tables with a
// single-column primary key were batched. Everything else -- composite keys,
// and tables with no primary key at all -- fell through to one unbounded
// UPDATE of the whole table, which is the opposite of what an online rewrite
// is for: unbounded WAL, row locks held for the length of the rewrite, and a
// cancellation that has to roll all of it back.
func TestRewriteBackfillBatchesWithoutASinglePrimaryKey(t *testing.T) {
	parallelPG(t)
	pool, a, store := rewritePGFixture(t)
	ctx := context.Background()
	// A composite primary key: the single-column keyset path cannot apply.
	mustExecSQL(t, pool, `CREATE TABLE ledger (tenant_id bigint NOT NULL, id bigint NOT NULL, amount text NOT NULL DEFAULT '0', PRIMARY KEY (tenant_id, id))`)
	mustExecSQL(t, pool, `ALTER TABLE ledger OWNER TO appowner`)
	mustExecSQL(t, pool, `INSERT INTO ledger (tenant_id, id, amount) SELECT g % 7, g, g::text FROM generate_series(1, `+fmt.Sprint(rewriteRows)+`) g`)

	m := catalog.DDLMigration{ID: "10000000-0000-0000-0000-0000000000c1", Database: "postgres",
		Statement: "alter table ledger alter column amount type bigint using amount::bigint",
		Kind:      "ALTER TABLE", Strategy: catalog.StrategyRewrite, Scope: "all", State: catalog.MigrationQueued,
		Meta: catalog.MigrationMeta{RunAs: "appowner", Rewrite: &catalog.RewriteChange{Schema: "public", Table: "ledger",
			Column: "amount", NewType: "bigint", Using: "amount::bigint", BatchSize: 100}}}
	store.migrations = []catalog.DDLMigration{m}

	if _, err := a.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := store.get(t, m.ID)
	if got.State != catalog.MigrationComplete {
		t.Fatalf("state %s: %s", got.State, got.Error)
	}

	var typ string
	if err := pool.QueryRow(ctx, `SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
		WHERE a.attrelid = 'public.ledger'::regclass AND a.attname = 'amount'`).Scan(&typ); err != nil {
		t.Fatal(err)
	}
	if typ != "bigint" {
		t.Fatalf("column type %q after the rewrite, want bigint", typ)
	}
	var wrong int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger WHERE amount <> id`).Scan(&wrong); err != nil {
		t.Fatal(err)
	}
	if wrong != 0 {
		t.Fatalf("%d rows did not carry their value across the rewrite", wrong)
	}
	// The end state is identical either way, so correctness alone does not
	// show the backfill was batched. Each committed batch stamps its rows
	// with its own transaction id, so a single unbounded UPDATE leaves every
	// row sharing one xmin and batching leaves many.
	var versions int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT DISTINCT xmin::text FROM ledger) v`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions < 2 {
		t.Fatalf("every row shares one transaction id: the backfill ran as a single unbounded UPDATE of all %d rows", rewriteRows)
	}
}
