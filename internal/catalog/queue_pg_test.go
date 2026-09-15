package catalog

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func queueCatalog(t *testing.T) (*pgx.Conn, *pgxpool.Pool, string) {
	t.Helper()
	requireDocker(t)
	ctx := context.Background()
	dsn := startPostgres(t, candidateImages[0])
	conn := connect(t, dsn)
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app'), ('other')`)
	return conn, pool, dsn
}

var opSeq atomic.Int64

type queueOp struct {
	name, kind, state, stage, database, migrationKind string
	arrival                                           int64
	sourceSet                                         bool
}

// insertOps writes each operation with its arrival and returns the ids by
// name.
func insertOps(t *testing.T, conn *pgx.Conn, ops []queueOp) map[string]string {
	t.Helper()
	ids := map[string]string{}
	for _, op := range ops {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", opSeq.Add(1))
		ids[op.name] = id
		switch op.kind {
		case OperationDDL:
			kind := op.migrationKind
			if kind == "" {
				kind = "CREATE INDEX"
			}
			mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, arrival)
				VALUES ($1, $2, 'ddl ' || $3, $4, 'direct', 'all', $5, $6)`, id, op.database, op.name, kind, op.state, op.arrival)
		default:
			spec := map[string]any{}
			if op.kind == OperationPlacement {
				spec["database"] = op.database
			} else if op.sourceSet {
				spec["source_set"] = "default"
			}
			status := map[string]any{}
			if op.stage != "" {
				status["stage"] = op.stage
			}
			mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status, arrival) VALUES ($1, $2, $3, $4, $5, $6)`,
				id, op.kind, op.state, spec, status, op.arrival)
		}
	}
	return ids
}

func blockersOf(t *testing.T, conn *pgx.Conn, kind, id string) []Blocker {
	t.Helper()
	got, err := OperationBlockers(context.Background(), conn, kind, id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestOperationBlockersFollowTheQueueRule (PGS-898): an operation that has
// not started waits for every conflicting operation that has, and for every
// conflicting one that arrived before it and holds its place.
func TestOperationBlockersFollowTheQueueRule(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	named := func(ids map[string]string, bs []Blocker) []string {
		var out []string
		for _, b := range bs {
			for name, id := range ids {
				if id == b.ID {
					out = append(out, name+":"+b.Reason)
				}
			}
		}
		return out
	}
	for _, c := range []struct {
		what string
		ops  []queueOp
		// wait maps an operation to what it must wait for, in arrival order.
		wait map[string][]string
	}{
		{
			what: "DDL issued during a reshard's copy waits for the reshard",
			ops: []queueOp{
				{name: "reshard", kind: OperationReshard, state: "running", stage: "copying", arrival: 1, sourceSet: true},
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 2},
			},
			wait: map[string][]string{"index": {"reshard:started"}, "reshard": nil},
		},
		{
			what: "and keeps waiting through the switched window",
			ops: []queueOp{
				{name: "reshard", kind: OperationUpgrade, state: "running", stage: "switched", arrival: 1, sourceSet: true},
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 2},
			},
			wait: map[string][]string{"index": {"reshard:started"}},
		},
		{
			what: "a reshard not yet copying waits for DDL that arrived first, and the DDL does not wait for it",
			ops: []queueOp{
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 1},
				{name: "reshard", kind: OperationReshard, state: "running", stage: "ready_for_copy", arrival: 2, sourceSet: true},
			},
			wait: map[string][]string{"index": nil, "reshard": {"index:earlier"}},
		},
		{
			what: "a finished reshard holds nothing, a cancelled one still cleaning up does",
			ops: []queueOp{
				{name: "done", kind: OperationReshard, state: "completed", stage: "completed", arrival: 1, sourceSet: true},
				{name: "cleaning", kind: OperationReshard, state: "cancelled", stage: "cancelling", arrival: 2, sourceSet: true},
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 3},
			},
			wait: map[string][]string{"index": {"cleaning:started"}},
		},
		{
			what: "a placement holds DDL on its own database only, until it swaps",
			ops: []queueOp{
				{name: "move", kind: OperationPlacement, state: "running", stage: "copying", database: "app", arrival: 1},
				{name: "retired", kind: OperationPlacement, state: "running", stage: "retiring", database: "other", arrival: 2},
				{name: "app_index", kind: OperationDDL, state: "queued", database: "app", arrival: 3},
				{name: "other_index", kind: OperationDDL, state: "queued", database: "other", arrival: 4},
			},
			wait: map[string][]string{"app_index": {"move:started"}, "other_index": nil},
		},
		{
			what: "role DDL keeps its order against DDL in every database",
			ops: []queueOp{
				{name: "role", kind: OperationDDL, state: "queued", database: "other", migrationKind: "CREATE ROLE", arrival: 1},
				{name: "grant", kind: OperationDDL, state: "queued", database: "app", migrationKind: "GRANT", arrival: 2},
				{name: "unrelated", kind: OperationDDL, state: "queued", database: "other", arrival: 0},
			},
			wait: map[string][]string{"grant": {"role:earlier"}, "role": {"unrelated:earlier"}, "unrelated": nil},
		},
		{
			what: "DDL in another database does not hold DDL here",
			ops: []queueOp{
				{name: "there", kind: OperationDDL, state: "running", database: "other", arrival: 1},
				{name: "here", kind: OperationDDL, state: "queued", database: "app", arrival: 2},
			},
			wait: map[string][]string{"here": nil},
		},
		{
			what: "paused and undriven operations that have not started hold nothing",
			ops: []queueOp{
				{name: "paused_move", kind: OperationPlacement, state: "paused", stage: "preparing", database: "app", arrival: 1},
				{name: "inplace", kind: OperationReshard, state: "pending", arrival: 2},
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 3},
			},
			wait: map[string][]string{"index": nil},
		},
		{
			what: "a paused reshard that has started still holds",
			ops: []queueOp{
				{name: "reshard", kind: OperationReshard, state: "paused", stage: "copying", arrival: 1, sourceSet: true},
				{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 2},
			},
			wait: map[string][]string{"index": {"reshard:started"}},
		},
		{
			what: "a placement waits for a reshard that arrived first, a reshard for a placement that has started, placements not for each other",
			ops: []queueOp{
				{name: "reshard", kind: OperationReshard, state: "provisioning", stage: "provisioning", arrival: 1, sourceSet: true},
				{name: "move", kind: OperationPlacement, state: "running", stage: "preparing", database: "app", arrival: 2},
				{name: "moving", kind: OperationPlacement, state: "running", stage: "copying", database: "other", arrival: 3},
			},
			wait: map[string][]string{"move": {"reshard:earlier"}, "reshard": {"moving:started"}, "moving": nil},
		},
	} {
		t.Run(c.what, func(t *testing.T) {
			mustExec(t, conn, `DELETE FROM pgshard.migrations`)
			mustExec(t, conn, `DELETE FROM pgshard.workflows`)
			ids := insertOps(t, conn, c.ops)
			for _, op := range c.ops {
				want, checked := c.wait[op.name]
				if !checked {
					continue
				}
				if got := named(ids, blockersOf(t, conn, op.kind, ids[op.name])); !slices.Equal(got, want) {
					t.Errorf("%s waits for %v, want %v", op.name, got, want)
				}
			}
		})
	}
}

// TestTheQueueHasNoCycleAndSomethingCanAlwaysStart builds every combination
// of a DDL migration, a reshard and a placement in each state they reach,
// in every arrival order, and checks the rule's two promises: a started
// operation waits for nothing, and whatever has not started, one of them
// may start or something has started.
func TestTheQueueHasNoCycleAndSomethingCanAlwaysStart(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	ddl := []queueOp{
		{kind: OperationDDL, state: "queued", database: "app"},
		{kind: OperationDDL, state: "running", database: "app"},
	}
	copies := []queueOp{
		{kind: OperationReshard, state: "provisioning", stage: "provisioning", sourceSet: true},
		{kind: OperationReshard, state: "running", stage: "ready_for_copy", sourceSet: true},
		{kind: OperationReshard, state: "running", stage: "copying", sourceSet: true},
		{kind: OperationReshard, state: "running", stage: "switched", sourceSet: true},
		{kind: OperationReshard, state: "paused", stage: "ready_for_copy", sourceSet: true},
		{kind: OperationReshard, state: "cancelled", stage: "cancelling", sourceSet: true},
		{kind: OperationReshard, state: "pending"},
	}
	moves := []queueOp{
		{kind: OperationPlacement, state: "running", stage: "preparing", database: "app"},
		{kind: OperationPlacement, state: "running", stage: "copying", database: "app"},
		{kind: OperationPlacement, state: "running", stage: "retiring", database: "app"},
		{kind: OperationPlacement, state: "paused", stage: "preparing", database: "app"},
	}
	orders := [][3]int64{{1, 2, 3}, {1, 3, 2}, {2, 1, 3}, {2, 3, 1}, {3, 1, 2}, {3, 2, 1}}
	combos := 0
	for _, d := range ddl {
		for _, c := range copies {
			for _, m := range moves {
				for _, order := range orders {
					d.name, c.name, m.name = "ddl", "copy", "move"
					d.arrival, c.arrival, m.arrival = order[0], order[1], order[2]
					mustExec(t, conn, `DELETE FROM pgshard.migrations`)
					mustExec(t, conn, `DELETE FROM pgshard.workflows`)
					ops := []queueOp{d, c, m}
					ids := insertOps(t, conn, ops)
					waits := map[string][]string{}
					started := map[string]bool{}
					for _, op := range ops {
						for _, b := range blockersOf(t, conn, op.kind, ids[op.name]) {
							waits[op.name] = append(waits[op.name], b.ID)
						}
						started[op.name] = queryOne[bool](t, conn, `SELECT coalesce((SELECT started FROM pgshard.operations WHERE id = $1), true)`, ids[op.name])
					}
					label := fmt.Sprintf("%s/%s + %s/%s/%s + %s/%s/%s order %v", d.state, d.database, c.state, c.stage, fmt.Sprint(c.sourceSet), m.state, m.stage, m.database, order)
					var free, anyStarted, pending bool
					for _, op := range ops {
						if started[op.name] {
							anyStarted = true
							if len(waits[op.name]) > 0 {
								t.Errorf("%s: %s has started and still waits for %v", label, op.name, waits[op.name])
							}
							continue
						}
						pending = true
						if len(waits[op.name]) == 0 {
							free = true
						}
					}
					if pending && !free && !anyStarted {
						t.Errorf("%s: nothing has started and nothing may start: %v", label, waits)
					}
					combos++
				}
			}
		}
	}
	if combos != len(ddl)*len(copies)*len(moves)*len(orders) {
		t.Fatalf("checked %d combinations", combos)
	}
}

// TestIdenticalDDLAttachesToTheMigrationAlreadyQueued (PGS-898): a client
// whose statement timed out while its migration waited sends it again, and
// must be handed the migration already queued, not a second one that runs
// the same statement after the first.
func TestIdenticalDDLAttachesToTheMigrationAlreadyQueued(t *testing.T) {
	conn, pool, _ := queueCatalog(t)
	ctx := context.Background()
	index := func(sql string) DDLMigration {
		m := DDLMigration{Database: "app", Statement: sql, Kind: "CREATE INDEX", Strategy: "concurrent", Scope: "all",
			Meta: MigrationMeta{RunAs: "app", SearchPath: "public", Object: MigrationObject{Kind: "relation", Name: "orders_note_idx", Expect: "present"}}}
		m.DedupKey = MigrationDedupKey(m, sql)
		return m
	}
	stmt := "CREATE INDEX orders_note_idx ON orders (note)"

	var wg sync.WaitGroup
	results := make([]EnqueueResult, 20)
	errs := make([]error, 20)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = EnqueueMigrationOnce(ctx, pool, index(stmt), DefaultRetryWindow)
		}(i)
	}
	wg.Wait()
	attached := 0
	for i, r := range results {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if r.ID != results[0].ID {
			t.Fatalf("concurrent identical enqueues got different migrations: %s and %s", results[0].ID, r.ID)
		}
		if r.Attached {
			attached++
		}
	}
	if attached != 19 || queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.migrations`) != 1 {
		t.Fatalf("%d attached, %d rows; want 19 attached to one row", attached, queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.migrations`))
	}
	first := results[0].ID

	retry := func(sql string) EnqueueResult {
		t.Helper()
		r, err := EnqueueMigrationOnce(ctx, pool, index(sql), DefaultRetryWindow)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := retry("create   index orders_note_idx on orders (note);"); r.ID == first {
		t.Fatal("the key is made from the statement the router normalised; a differently spelled one must not match here")
	}
	mustExec(t, conn, `DELETE FROM pgshard.migrations WHERE id <> $1`, first)

	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'running' WHERE id = $1`, first)
	if r := retry(stmt); r.ID != first || !r.Attached || r.State != MigrationRunning {
		t.Fatalf("a retry while the migration runs got %+v, want it attached to %s", r, first)
	}

	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'complete', finished_at = now() WHERE id = $1`, first)
	if r := retry(stmt); r.ID != first || r.State != MigrationComplete {
		t.Fatalf("a retry just after the migration completed got %+v, want the completed one", r)
	}
	mustExec(t, conn, `UPDATE pgshard.migrations SET finished_at = now() - interval '11 minutes' WHERE id = $1`, first)
	if r := retry(stmt); r.ID == first || r.Attached {
		t.Fatalf("a run long after the migration completed attached to it: %+v", r)
	}
	mustExec(t, conn, `DELETE FROM pgshard.migrations WHERE id <> $1`, first)

	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'failed', finished_at = now() WHERE id = $1`, first)
	if r := retry(stmt); r.ID == first {
		t.Fatal("a statement run again after its migration failed attached to the failure")
	}
}

// An attach must never reorder: once a different migration has arrived in
// the same scope, a repeat of an earlier statement is a new migration.
func TestAnAttachNeverOvertakesALaterDifferentMigration(t *testing.T) {
	conn, pool, _ := queueCatalog(t)
	ctx := context.Background()
	mk := func(database, kind, sql string) DDLMigration {
		m := DDLMigration{Database: database, Statement: sql, Kind: kind, Strategy: "direct", Scope: "all", Meta: MigrationMeta{RunAs: "app"}}
		m.DedupKey = MigrationDedupKey(m, sql)
		return m
	}
	enqueue := func(m DDLMigration) EnqueueResult {
		t.Helper()
		r, err := EnqueueMigrationOnce(ctx, pool, m, DefaultRetryWindow)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	add := enqueue(mk("app", "ALTER TABLE", "ALTER TABLE t ADD COLUMN c int"))
	enqueue(mk("other", "ALTER TABLE", "ALTER TABLE t DROP COLUMN c"))
	if r := enqueue(mk("app", "ALTER TABLE", "ALTER TABLE t ADD COLUMN c int")); r.ID != add.ID {
		t.Fatalf("DDL in another database came between; the repeat should still attach: %+v", r)
	}
	enqueue(mk("app", "ALTER TABLE", "ALTER TABLE t DROP COLUMN c"))
	if r := enqueue(mk("app", "ALTER TABLE", "ALTER TABLE t ADD COLUMN c int")); r.ID == add.ID || r.Attached {
		t.Fatalf("an ADD after a DROP in the same database was folded into the ADD before it: %+v", r)
	}

	mustExec(t, conn, `DELETE FROM pgshard.migrations`)
	grant := enqueue(mk("app", "GRANT", "GRANT SELECT ON t TO r"))
	enqueue(mk("other", "DROP ROLE", "DROP ROLE r"))
	if r := enqueue(mk("app", "GRANT", "GRANT SELECT ON t TO r")); r.ID == grant.ID {
		t.Fatal("a role statement in any database comes between; the repeat must not attach")
	}
}

func TestMigrationDedupKey(t *testing.T) {
	base := DDLMigration{Database: "app", Statement: "CREATE INDEX i ON t (c)", Kind: "CREATE INDEX", Strategy: "concurrent", Scope: "all",
		Meta: MigrationMeta{RunAs: "app", SearchPath: "public"}}
	key := MigrationDedupKey(base, base.Statement)
	if key == "" || MigrationDedupKey(base, "  CREATE INDEX i ON t (c) ; ") != key {
		t.Fatalf("surrounding space and a terminator change the key")
	}
	pinned := base
	pinned.Meta.ShardSet = "g2"
	if MigrationDedupKey(pinned, base.Statement) != key {
		t.Error("the set the applier pins at start changes the key")
	}
	for what, change := range map[string]func(*DDLMigration){
		"database":    func(m *DDLMigration) { m.Database = "other" },
		"role":        func(m *DDLMigration) { m.Meta.RunAs = "admin" },
		"search_path": func(m *DDLMigration) { m.Meta.SearchPath = "app, public" },
		"strategy":    func(m *DDLMigration) { m.Strategy = "direct" },
	} {
		m := base
		change(&m)
		if MigrationDedupKey(m, m.Statement) == key {
			t.Errorf("a different %s gives the same key", what)
		}
	}
	secret := DDLMigration{Database: "app", Statement: "ALTER ROLE r PASSWORD 'SCRAM-SHA-256$4096:x'", Kind: "ALTER ROLE", Scope: "all",
		Meta: MigrationMeta{Role: "r", Verifier: "SCRAM-SHA-256$4096:x"}}
	if MigrationDedupKey(secret, secret.Statement) != "" {
		t.Error("a statement carrying a verifier got a key")
	}
	if MigrationDedupKey(DDLMigration{Statement: "CREATE ROLE r"}, "CREATE ROLE r PASSWORD 'plain'") != "" {
		t.Error("a statement carrying a password got a key")
	}
}

// Rows already in the catalog when the queue arrives keep their order.
func TestTheQueueMigrationOrdersExistingRows(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	mustExec(t, conn, `CREATE SCHEMA IF NOT EXISTS pgshard;
		CREATE TABLE IF NOT EXISTS pgshard.schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now(), checksum text NOT NULL)`)
	all, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all {
		if m.Version >= 58 {
			break
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, created_at)
		VALUES ('00000000-0000-0000-0000-000000000002', 'app', 'b', 'CREATE TABLE', 'direct', 'all', 'queued', now() - interval '1 minute'),
		       ('00000000-0000-0000-0000-000000000001', 'app', 'a', 'CREATE TABLE', 'direct', 'all', 'queued', now() - interval '2 minutes')`)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, created_at) VALUES ('00000000-0000-0000-0000-000000000003', 'reshard', 'pending', now() - interval '90 seconds')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('g2', 2, 'desired')`)
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// Every arrival is distinct, and what is drawn next comes after all of
	// them: a shard set declared before the queue existed must not share
	// its place with an operation enqueued after.
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state) VALUES ('00000000-0000-0000-0000-000000000004', 'reshard', 'pending')`)
	if dup := queryOne[int64](t, conn, `SELECT count(*) - count(DISTINCT arrival) FROM (
		SELECT arrival FROM pgshard.migrations UNION ALL SELECT arrival FROM pgshard.workflows
		UNION ALL SELECT arrival FROM pgshard.shard_sets) x`); dup != 0 {
		t.Fatalf("%d arrivals are shared after the queue migration", dup)
	}
	if !queryOne[bool](t, conn, `SELECT (SELECT arrival FROM pgshard.workflows WHERE id = '00000000-0000-0000-0000-000000000004') >
		(SELECT max(arrival) FROM pgshard.shard_sets)`) {
		t.Fatal("a workflow enqueued after the migration arrives before a shard set declared before it")
	}
	got := queryOne[string](t, conn, `SELECT string_agg(k, ',' ORDER BY arrival) FROM (
		SELECT statement AS k, arrival FROM pgshard.migrations UNION ALL SELECT kind, arrival FROM pgshard.workflows) x`)
	if !strings.HasPrefix(got, "a,reshard,b") {
		t.Fatalf("existing rows arrive as %s, want a,reshard,b then the default shard set", got)
	}
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state) VALUES (gen_random_uuid(), 'app', 'c', 'CREATE TABLE', 'direct', 'all', 'queued')`)
	if last := queryOne[string](t, conn, `SELECT statement FROM pgshard.migrations ORDER BY arrival DESC LIMIT 1`); last != "c" {
		t.Fatalf("a new migration arrives before existing ones: last is %s", last)
	}
}

// A reader may ask what an operation waits for, but not read the rows the
// answer is computed from: migrations carry verifiers.
func TestAReaderMayAskWhatBlocksButNotReadTheQueueTables(t *testing.T) {
	conn, _, dsn := queueCatalog(t)
	ctx := context.Background()
	mustExec(t, conn, `CREATE ROLE queue_watcher LOGIN PASSWORD 'watching' IN ROLE pgshard_reader`)
	ids := insertOps(t, conn, []queueOp{
		{name: "reshard", kind: OperationReshard, state: "running", stage: "copying", arrival: 1, sourceSet: true},
		{name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 2},
	})
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password = "queue_watcher", "watching"
	rc, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close(ctx) }()
	got, err := OperationBlockers(ctx, rc, OperationDDL, ids["index"])
	if err != nil || len(got) != 1 || got[0].ID != ids["reshard"] {
		t.Fatalf("a reader asking what blocks the index: %v %v", got, err)
	}
	if _, err := rc.Exec(ctx, `SELECT * FROM pgshard.operations`); err == nil {
		t.Error("a reader can read pgshard.operations")
	}
	if _, err := rc.Exec(ctx, `SELECT statement FROM pgshard.migrations`); err == nil {
		t.Error("a reader can read migration statements")
	}
	if ok, err := QueueSchema(ctx, rc); err != nil || !ok {
		t.Fatalf("QueueSchema = %v, %v", ok, err)
	}
}

func TestHomeDDLBlockers(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	ctx := context.Background()
	insertOps(t, conn, []queueOp{
		{name: "move", kind: OperationPlacement, state: "running", stage: "copying", database: "app", arrival: 1},
		{name: "swapped", kind: OperationPlacement, state: "running", stage: "retiring", database: "other", arrival: 2},
	})
	for db, want := range map[string]int{"app": 1, "other": 0} {
		got, err := HomeDDLBlockers(ctx, conn, db)
		if err != nil || len(got) != want {
			t.Fatalf("%s: %v %v, want %d", db, got, err, want)
		}
	}
	insertOps(t, conn, []queueOp{{name: "reshard", kind: OperationReshard, state: "running", stage: "switched", arrival: 3, sourceSet: true}})
	if got, err := HomeDDLBlockers(ctx, conn, "other"); err != nil || len(got) != 1 || got[0].Kind != OperationReshard {
		t.Fatalf("a reshard with work left must hold home DDL everywhere: %v %v", got, err)
	}
}

func TestTheClusterScopedKindsAgree(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	for _, kind := range append(slices.Clone(ClusterScopedMigrationKinds), "CREATE TABLE", "GRANT", "ALTER DATABASE SET") {
		want := slices.Contains(ClusterScopedMigrationKinds, kind)
		if got := queryOne[bool](t, conn, `SELECT pgshard.cluster_scoped_migration($1)`, kind); got != want {
			t.Errorf("%s: the catalog says cluster-scoped %v, Go says %v", kind, got, want)
		}
	}
}

// A beat from a controller whose leadership has passed must not make a
// dead leader look alive.
func TestAHeartbeatFromAnOldTermWritesNothing(t *testing.T) {
	conn, pool, _ := queueCatalog(t)
	ctx := context.Background()
	if _, found, err := ControllerHeartbeatAge(ctx, pool, HeartbeatApplier); err != nil || found {
		t.Fatalf("before any beat: found %v, %v", found, err)
	}
	mustExec(t, conn, `UPDATE pgshard.leader_term SET term = 7`)
	if err := BeatController(ctx, pool, HeartbeatApplier, 6); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := ControllerHeartbeatAge(ctx, pool, HeartbeatApplier); found {
		t.Fatal("a beat from term 6 was recorded under term 7")
	}
	if err := BeatController(ctx, pool, HeartbeatApplier, 7); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.controller_heartbeat SET beat_at = now() - interval '1 hour'`)
	if err := BeatController(ctx, pool, HeartbeatApplier, 7); err != nil {
		t.Fatal(err)
	}
	if age, found, err := ControllerHeartbeatAge(ctx, pool, HeartbeatApplier); err != nil || !found || age > time.Minute {
		t.Fatalf("after a current beat: age %s found %v %v", age, found, err)
	}
}

// A start_error is a place given up; an empty one, as a successful pass
// writes it, is not.
func TestAFailingStartGivesUpItsPlace(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	for _, first := range []queueOp{
		{name: "first", kind: OperationPlacement, state: "pending", stage: "preparing", database: "app", arrival: 1},
		{name: "first", kind: OperationUpgrade, state: "running", stage: "ready_for_copy", arrival: 1, sourceSet: true},
	} {
		mustExec(t, conn, `DELETE FROM pgshard.migrations`)
		mustExec(t, conn, `DELETE FROM pgshard.workflows`)
		ids := insertOps(t, conn, []queueOp{first, {name: "index", kind: OperationDDL, state: "queued", database: "app", arrival: 2}})
		for _, c := range []struct {
			startError string
			waits      int
		}{{"", 1}, {"the upgrade's target image is not available", 0}, {"", 1}} {
			mustExec(t, conn, `UPDATE pgshard.workflows SET status = status || jsonb_build_object('start_error', $2::text) WHERE id = $1`, ids["first"], c.startError)
			if got := blockersOf(t, conn, OperationDDL, ids["index"]); len(got) != c.waits {
				t.Fatalf("%s with start_error %q: the index waits for %v, want %d", first.kind, c.startError, got, c.waits)
			}
		}
	}
}

func TestEnqueueEdges(t *testing.T) {
	conn, pool, _ := queueCatalog(t)
	ctx := context.Background()
	enqueue := func(m DDLMigration) EnqueueResult {
		t.Helper()
		r, err := EnqueueMigrationOnce(ctx, pool, m, DefaultRetryWindow)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	keyed := func(kind, sql string) DDLMigration {
		m := DDLMigration{Database: "app", Statement: sql, Kind: kind, Strategy: "concurrent", Scope: "all"}
		m.DedupKey = MigrationDedupKey(m, sql)
		return m
	}

	unkeyed := DDLMigration{Database: "app", Statement: "CREATE ROLE r PASSWORD 'x'", Kind: "CREATE ROLE", Strategy: "direct", Scope: "all"}
	if a, b := enqueue(unkeyed), enqueue(unkeyed); a.ID == b.ID || a.Attached || b.Attached {
		t.Fatalf("a statement with no key attached: %+v %+v", a, b)
	}

	reindex := enqueue(keyed("REINDEX", "REINDEX INDEX CONCURRENTLY i"))
	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'complete', finished_at = now() WHERE id = $1`, reindex.ID)
	if r := enqueue(keyed("REINDEX", "REINDEX INDEX CONCURRENTLY i")); r.ID == reindex.ID {
		t.Fatal("a REINDEX run again just after one completed attached to it; running it again is the point")
	}

	mustExec(t, conn, `DELETE FROM pgshard.migrations`)
	grant := enqueue(keyed("GRANT", "GRANT SELECT ON t TO r"))
	drop := DDLMigration{Database: "other", Statement: "DROP DATABASE app", Kind: "DROP DATABASE", Strategy: "direct", Scope: "all", Meta: MigrationMeta{Database: "app", DatabaseOp: "drop"}}
	drop.DedupKey = MigrationDedupKey(drop, drop.Statement)
	enqueue(drop)
	if r := enqueue(keyed("GRANT", "GRANT SELECT ON t TO r")); r.ID == grant.ID {
		t.Fatal("a statement dropping the database came between; the repeat must not attach")
	}
}

func TestQueueSchemaIsAbsentBeforeTheMigration(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	mustExec(t, conn, `CREATE SCHEMA IF NOT EXISTS pgshard;
		CREATE TABLE IF NOT EXISTS pgshard.schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now(), checksum text NOT NULL)`)
	all, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all {
		if m.Version >= 58 {
			break
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := QueueSchema(ctx, conn); err != nil || ok {
		t.Fatalf("before 0058 QueueSchema = %v, %v", ok, err)
	}
}
