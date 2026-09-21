package catalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
			// PGS-866, the symptom itself: the row an in-place edit of
			// pgshard.shard_ranges records. Nothing drives it -- it stays
			// pending until somebody cancels it or reshards through
			// spec.shards -- so a placement queued behind it waited at
			// prepare for as long as the row stood.
			what: "a placement does not wait for a reshard nothing drives",
			ops: []queueOp{
				{name: "edit", kind: OperationReshard, state: "pending", arrival: 1},
				{name: "move", kind: OperationPlacement, state: "pending", stage: "preparing", database: "app", arrival: 2},
			},
			wait: map[string][]string{"move": nil},
		},
		{
			// The contrast, and why the rule reads the source set and not
			// the state alone: a reshard the controller has just created is
			// pending too, for the pass that moves it on. Letting a
			// placement past that one would start a move the copy about to
			// begin then has to carry.
			what: "but it does wait for a driven reshard still pending",
			ops: []queueOp{
				{name: "reshard", kind: OperationReshard, state: "pending", arrival: 1, sourceSet: true},
				{name: "move", kind: OperationPlacement, state: "pending", stage: "preparing", database: "app", arrival: 2},
			},
			wait: map[string][]string{"move": {"reshard:earlier"}},
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
	// A router from before placements were recorded queues without them,
	// and a retry through one from after must still attach (PGS-971).
	placed := base
	placed.Meta.Placements = []TablePlacement{{Schema: "public", Table: "t", Placement: "sharded", ShardKey: "c"}}
	if MigrationDedupKey(placed, base.Statement) != key {
		t.Error("the placements a router read change the key, so a retry through an upgraded router queues a second migration")
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

	// Every kind whose second run is the work again rather than a repeat
	// of a statement already applied. Attaching one of these to a
	// completed migration answers the client with a success for work that
	// did not happen: an ALTER SEQUENCE ... RESTART sent again after the
	// application has consumed values is asking to restart from where the
	// sequence is now.
	for _, c := range []struct{ kind, sql string }{
		{"REINDEX", "REINDEX INDEX CONCURRENTLY i"},
		{"VACUUM", "VACUUM FULL t"},
		{"ALTER SEQUENCE", "ALTER SEQUENCE order_no RESTART"},
	} {
		first := enqueue(keyed(c.kind, c.sql))
		mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'complete', finished_at = now() WHERE id = $1`, first.ID)
		if again := enqueue(keyed(c.kind, c.sql)); again.ID == first.ID || again.Attached {
			t.Errorf("%s run again just after one completed attached to it; running it again is the point", c.kind)
		}
		mustExec(t, conn, `DELETE FROM pgshard.migrations`)
	}

	// The kind this does not apply to: a timed-out CREATE INDEX sent again
	// inside the window is the same work, and attaching is what keeps the
	// client's retry loop from building it twice.
	idx := enqueue(keyed("CREATE INDEX", "CREATE INDEX CONCURRENTLY orders_note_idx ON orders (note)"))
	mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'complete', finished_at = now() WHERE id = $1`, idx.ID)
	if again := enqueue(keyed("CREATE INDEX", "CREATE INDEX CONCURRENTLY orders_note_idx ON orders (note)")); again.ID != idx.ID || !again.Attached {
		t.Errorf("a completed CREATE INDEX no longer stands for the same statement sent again: %+v", again)
	}
	mustExec(t, conn, `DELETE FROM pgshard.migrations`)

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

// TestTheOperationQueueReadsAsPeopleNeedIt (PGS-903): the queue view names
// each operation, says where it stands and what it waits for, draws a
// progress bar, keeps statement text for administrators, and survives a
// status it cannot read.
func TestTheOperationQueueReadsAsPeopleNeedIt(t *testing.T) {
	conn, _, dsn := queueCatalog(t)
	ctx := context.Background()
	const reshard, index, role, move, switched = "00000000-0000-0000-0000-000000000901", "00000000-0000-0000-0000-000000000902",
		"00000000-0000-0000-0000-000000000903", "00000000-0000-0000-0000-000000000904", "00000000-0000-0000-0000-000000000905"
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status, arrival) VALUES
		($1, 'reshard', 'running', '{"shard_set": "g2", "source_set": "default", "source_shards": 2, "ranges": [{}, {}, {}, {}]}',
		 '{"stage": "copying", "message": "copying", "progress": {"tables_ready": 12, "tables_total": 40}}', 1)`, reshard)
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, meta, arrival) VALUES
		($1, 'app', 'CREATE INDEX   orders_note_idx
		   ON orders (note)', 'CREATE INDEX', 'concurrent', 'all', 'queued', '{"object": {"kind": "relation", "name": "orders_note_idx"}}', 2),
		($2, 'app', 'ALTER ROLE app PASSWORD ''SCRAM-SHA-256$4096:secret''', 'ALTER ROLE', 'direct', 'all', 'queued', '{"role": "app", "verifier": "SCRAM-SHA-256$4096:secret"}', 3)`, index, role)
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status, arrival) VALUES
		($1, 'table_placement', 'pending', '{"database": "app", "schema_name": "public", "table_name": "items", "to": {"placement": "sharded", "shard_key": "id"}}', '{"stage": "preparing"}', 4)`, move)

	mustExec(t, conn, `CREATE ROLE queue_reader LOGIN PASSWORD 'reading' IN ROLE pgshard_reader`)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password = "queue_reader", "reading"
	rc, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close(ctx) }()
	entries, total, err := ListOperationQueue(ctx, rc, false)
	if err != nil {
		t.Fatalf("a reader reading the queue: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("%d entries, want 4: %+v", len(entries), entries)
	}
	if total != 4 {
		t.Errorf("total %d, want 4", total)
	}
	byID := map[string]QueueEntry{}
	for i, e := range entries {
		if e.Position != int64(i+1) {
			t.Errorf("%s at position %d, want %d", e.Command, e.Position, i+1)
		}
		if e.Statement != nil {
			t.Errorf("a reader got statement text for %s", e.Command)
		}
		byID[e.ID] = e
	}
	r := byID[reshard]
	if r.Command != "reshard 2 to 4 shards" || r.State != "running" || r.Progress == nil || *r.Progress != 0.28 ||
		r.ProgressBar == nil || *r.ProgressBar != "[#####---------------]  28%" || r.Detail == nil || !strings.HasPrefix(*r.Detail, "copying 12/40 tables") {
		t.Errorf("reshard entry %+v (progress %v, bar %v, detail %v)", r, deref(r.Progress), deref(r.ProgressBar), deref(r.Detail))
	}
	i := byID[index]
	if i.Command != "CREATE INDEX orders_note_idx" || i.State != "waiting" || i.WaitingFor == nil || *i.WaitingFor != "reshard "+reshard+" (in progress)" ||
		len(i.Blockers) != 1 || i.Blockers[0].ID != reshard || i.ProgressBar == nil || *i.ProgressBar != "[--------------------]   0%" {
		t.Errorf("index entry %+v (waiting for %v)", i, deref(i.WaitingFor))
	}
	if c := byID[role].Command; c != "ALTER ROLE app" {
		t.Errorf("role entry command %q", c)
	}
	// The role statement waits for the reshard, which started, and for the
	// index, which arrived before it. The first of them is the one holding
	// the queue and the one a reader is sent after -- and it is the reshard,
	// which arrived first, not whichever of the two sorts first by kind and
	// id. Sorting them put "ddl" ahead of "reshard" and named the newer
	// blocker.
	if b := byID[role].Blockers; len(b) != 2 || b[0].ID != reshard || b[1].ID != index {
		t.Errorf("the role statement's blockers are not in arrival order: %+v", b)
	}
	if m := byID[move]; m.Command != "move app.public.items to sharded(id)" || m.State != "waiting" || m.Database == nil || *m.Database != "app" {
		t.Errorf("placement entry %+v", m)
	}
	if _, err := rc.Exec(ctx, `SELECT * FROM pgshard.operation_queue_detail`); err == nil {
		t.Error("a reader can read the queue with statements")
	}
	// Asked of the view, not of what the Go reader scanned. ListOperationQueue
	// selects NULL::text for the statement when detail is off, so checking the
	// scanned field cannot fail whatever the view exposes: adding statement to
	// pgshard.operation_queue would hand every tenant's DDL to a reader and
	// leave the whole suite green.
	if _, err := rc.Exec(ctx, `SELECT statement FROM pgshard.operation_queue`); err == nil {
		t.Error("the reader's queue has a statement column")
	}
	// And the whole row, against a marker that is in a plain statement rather
	// than in a verifier: the verifier-carrying one is rewritten to a command
	// before it reaches any view, so searching for SCRAM alone proves nothing
	// about ordinary DDL text.
	if got := queryOne[string](t, rc, `SELECT string_agg(row_to_json(q)::text, '') FROM pgshard.operation_queue q`); strings.Contains(got, "SCRAM") || strings.Contains(got, "secret") || strings.Contains(got, "ON orders (note)") {
		t.Errorf("the reader's queue carries statement text: %s", got)
	}

	detail, _, err := ListOperationQueue(ctx, conn, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range detail {
		switch e.ID {
		case index:
			if e.Statement == nil || *e.Statement != "CREATE INDEX orders_note_idx ON orders (note)" {
				t.Errorf("index statement %v", deref(e.Statement))
			}
		case role:
			if e.Statement == nil || strings.Contains(*e.Statement, "SCRAM") || !strings.Contains(*e.Statement, "not shown") {
				t.Errorf("role statement %v", deref(e.Statement))
			}
		}
	}

	mustExec(t, conn, `DELETE FROM pgshard.migrations`)
	// The status a controller actually writes once it has switched: when
	// the switch happened, and the window the spec asked for. The deadline
	// is derived from those. Seeding a cutover.retire_at here instead --
	// which is what this test used to do -- proved only that the view can
	// read a key the test invented.
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status, arrival) VALUES
		($1, 'upgrade', 'running', '{"pg_major": 19, "source_pg_major": 18, "retire_after_seconds": 86400}',
		 jsonb_build_object('stage', 'switched', 'cutover', jsonb_build_object('switched_at', now() - interval '1 hour')), 5)`, switched)
	mustExec(t, conn, `UPDATE pgshard.workflows SET status = '{"stage": "copying", "progress": {"tables_ready": "twelve", "tables_total": [40]}, "started_at": "not a time"}' WHERE id = $1`, reshard)
	entries, _, err = ListOperationQueue(ctx, conn, false)
	if err != nil {
		t.Fatalf("a status the view cannot read broke it: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.ID != switched {
			continue
		}
		found = true
		if e.Command != "upgrade PostgreSQL 18 to 19" || e.State != "retiring" {
			t.Errorf("switched upgrade entry %+v", e)
		}
		if e.Detail == nil || !strings.HasPrefix(*e.Detail, "old groups retire in 22h5") {
			t.Errorf("the countdown is not derived from the switch and the window: %v", deref(e.Detail))
		}
		if e.RetireAt == nil {
			t.Error("retire_at is not answered for a switched workflow")
		}
		// An hour into a day-long window, not pinned at the top of the
		// bar: the ramp reads the same two values the countdown does.
		if e.Progress == nil || *e.Progress < 0.90 || *e.Progress > 0.92 {
			t.Errorf("progress %v an hour into a 24h retirement window", deref(e.Progress))
		}
	}
	if !found {
		t.Fatal("the switched upgrade is not in the queue at all, so nothing above was asserted")
	}
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// TestTheAdminUILoginReadsWhatItShows (PGS-902): the admin UI reads the
// catalog as its own login. It must see the queue and the migrations the
// pages show, and nothing a reader is kept from: the verifiers.
func TestTheAdminUILoginReadsWhatItShows(t *testing.T) {
	conn, _, dsn := queueCatalog(t)
	ctx := context.Background()
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, meta) VALUES
		(gen_random_uuid(), 'app', 'CREATE INDEX i ON t (c)', 'CREATE INDEX', 'direct', 'all', 'queued', '{}'),
		(gen_random_uuid(), 'app', 'ALTER ROLE app PASSWORD ''SCRAM-SHA-256$4096:hidden''', 'ALTER ROLE', 'direct', 'all', 'queued', '{"role": "app", "verifier": "SCRAM-SHA-256$4096:hidden"}')`)
	mustExec(t, conn, `ALTER ROLE `+AdminUIRole+` LOGIN PASSWORD 'ui-secret'`)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password = AdminUIRole, "ui-secret"
	ui, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("the admin UI login cannot connect: %v", err)
	}
	defer func() { _ = ui.Close(ctx) }()

	if _, _, err := ListOperationQueue(ctx, ui, true); err != nil {
		t.Errorf("reading the queue with statements: %v", err)
	}
	ms, total, err := ListMigrationsFrom(ctx, ui, MigrationsDetailView, MigrationFilter{})
	if err != nil || total != 2 {
		t.Fatalf("listing migrations: %d (%v)", total, err)
	}
	for _, m := range ms {
		if strings.Contains(m.Statement, "SCRAM") || m.Meta.Verifier != "" {
			t.Errorf("the admin UI login can read a verifier: %q %q", m.Statement, m.Meta.Verifier)
		}
		if m.Kind == "ALTER ROLE" && !strings.Contains(m.Statement, "not shown") {
			t.Errorf("a password-setting statement is shown as %q", m.Statement)
		}
	}
	if _, err := CountMigrationsFrom(ctx, ui, MigrationsDetailView); err != nil {
		t.Errorf("counting migrations: %v", err)
	}
	for _, sql := range []string{
		`SELECT statement FROM pgshard.migrations`,
		`SELECT verifier FROM pgshard.roles`,
		`SELECT * FROM pgshard.operations`,
	} {
		if _, err := ui.Exec(ctx, sql); err == nil {
			t.Errorf("the admin UI login may run %q", sql)
		}
	}
	if ro := queryOne[string](t, ui, `SHOW default_transaction_read_only`); ro != "on" {
		t.Errorf("the admin UI login's transactions are %s", ro)
	}
	// The read-only default is a convenience, not the boundary: it is
	// PGC_USERSET, so the login can turn it off on its own connection. A
	// write refused only by it is refused with "cannot execute INSERT in a
	// read-only transaction" -- ExecCheckXactReadOnly runs before the
	// privilege check ever does -- so granting this role INSERT would leave
	// the assertion passing. The boundary is the grant, and it is asked for
	// with the GUC out of the way.
	if _, err := ui.Exec(ctx, `SET default_transaction_read_only = off`); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state) VALUES (gen_random_uuid(), 'app', 'x', 'CREATE TABLE', 'direct', 'all', 'queued')`,
		`UPDATE pgshard.migrations SET state = 'complete'`,
		`DELETE FROM pgshard.workflows`,
	} {
		_, err := ui.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Errorf("the admin UI login may run %q with writes on: %v", sql, err)
			continue
		}
		if pgErr.Code != "42501" {
			t.Errorf("%q was refused with %s, not a privilege denial: %s", sql, pgErr.Code, pgErr.Message)
		}
	}
}

// TestTheQueueNamesTheObjectOfEveryMigration (PGS-923): meta.object is
// recorded only for the kinds the applier has to check for when it resumes.
// Every ALTER TABLE, COMMENT and rename carries none, and three of those
// waiting together read as "ALTER TABLE", "ALTER TABLE", "COMMENT" -- in
// the one view that withholds the statement they would have been told apart
// by. The router records the name it planned as meta.target.
func TestTheQueueNamesTheObjectOfEveryMigration(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	ctx := context.Background()
	mustExec(t, conn, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, state, meta, arrival) VALUES
		('00000000-0000-0000-0000-000000000a01', 'app', 'ALTER TABLE orders ADD COLUMN extra int', 'ALTER TABLE', 'direct', 'all', 'queued', '{"target": "public.orders"}', 1),
		('00000000-0000-0000-0000-000000000a02', 'app', 'ALTER TABLE items DROP COLUMN note', 'ALTER TABLE', 'direct', 'all', 'queued', '{"target": "items"}', 2),
		('00000000-0000-0000-0000-000000000a03', 'app', 'COMMENT ON TABLE orders IS ''x''', 'COMMENT', 'direct', 'all', 'queued', '{"target": "public.orders"}', 3),
		('00000000-0000-0000-0000-000000000a04', 'app', 'CREATE INDEX i ON orders (note)', 'CREATE INDEX', 'concurrent', 'all', 'queued', '{"object": {"kind": "relation", "schema": "public", "name": "i"}, "target": "orders"}', 4),
		('00000000-0000-0000-0000-000000000a05', 'app', 'ALTER TABLE orders ADD COLUMN older int', 'ALTER TABLE', 'direct', 'all', 'queued', '{}', 5)`)

	entries, _, err := ListOperationQueue(ctx, conn, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range entries {
		got[e.ID] = e.Command
	}
	for id, want := range map[string]string{
		"00000000-0000-0000-0000-000000000a01": "ALTER TABLE public.orders",
		"00000000-0000-0000-0000-000000000a02": "ALTER TABLE items",
		"00000000-0000-0000-0000-000000000a03": "COMMENT public.orders",
		// The applier's own record of the object wins: it is the name the
		// statement creates, which is what a reader watching a CREATE INDEX
		// is looking for.
		"00000000-0000-0000-0000-000000000a04": "CREATE INDEX public.i",
		// A migration queued before this upgrade has no target and reads as
		// it did.
		"00000000-0000-0000-0000-000000000a05": "ALTER TABLE",
	} {
		if got[id] != want {
			t.Errorf("%s command = %q, want %q", id, got[id], want)
		}
	}
}

// TestAWedgedPassGoesStaleWhileLivenessKeepsBeating (PGS-907): the applier's
// liveness goroutine beats on its own timer, so it keeps beating through a
// pass that never returns. A router waiting on a migration read that beat and
// waited for ever, where its no-progress rule would have given up. The pass
// signal is what it reads now, and a fresh liveness beat must not refresh it.
func TestAWedgedPassGoesStaleWhileLivenessKeepsBeating(t *testing.T) {
	conn, pool, _ := queueCatalog(t)
	ctx := context.Background()
	// BeatController writes only under the current term.
	mustExec(t, conn, `UPDATE pgshard.leader_term SET term = 1`)

	// Before any pass has finished, the liveness beat stands in -- that is
	// the documented residual, and a controller too old to record passes
	// must not read as dead.
	if err := BeatController(ctx, pool, HeartbeatApplier, 1); err != nil {
		t.Fatal(err)
	}
	if age, found, err := ApplierProgressAge(ctx, pool); err != nil || !found || age > time.Minute {
		t.Fatalf("with no pass recorded the liveness beat must stand in: age %s found %v %v", age, found, err)
	}

	// Once a pass has finished, that is the signal.
	if err := BeatController(ctx, pool, HeartbeatApplierPass, 1); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `UPDATE pgshard.controller_heartbeat SET beat_at = now() - interval '1 hour' WHERE component = $1`, HeartbeatApplierPass)
	// The liveness goroutine keeps beating, as it does through a wedged pass.
	if err := BeatController(ctx, pool, HeartbeatApplier, 1); err != nil {
		t.Fatal(err)
	}
	age, found, err := ApplierProgressAge(ctx, pool)
	if err != nil || !found {
		t.Fatalf("progress age: found %v, %v", found, err)
	}
	if age < 30*time.Minute {
		t.Fatalf("progress age is %s: a fresh liveness beat refreshed it, so a wedged pass still reads as progress", age)
	}
}

// TestAPlacementWithNoStageSeesTheDDLQueuedAheadOfIt (PGS-939 review): the
// queue read ddl_hold_ends as
//
//	w.status->>'stage' = 'retiring'
//
// which is NULL -- not false -- for a workflow whose status carries no
// stage, which is exactly what a placement looks like when it is created.
// The rule then evaluated "... AND NOT a_hold_ends" to NULL, and the
// blocker query keeps only rows whose conflict is true, so a fresh
// placement saw no DDL queued ahead of it and the DDL saw no placement.
//
// The window is the one that matters: the placer's first act is to describe
// the table, and a DDL running beside it changes the shape it just read.
func TestAPlacementWithNoStageSeesTheDDLQueuedAheadOfIt(t *testing.T) {
	conn, _, _ := queueCatalog(t)
	ids := insertOps(t, conn, []queueOp{
		{name: "ddl", kind: OperationDDL, state: "queued", database: "app", arrival: 1},
		{name: "placement", kind: OperationPlacement, state: "pending", database: "app", arrival: 2},
	})
	blockers := blockersOf(t, conn, OperationPlacement, ids["placement"])
	if len(blockers) != 1 || blockers[0].ID != ids["ddl"] {
		t.Fatalf("a placement with no stage yet is blocked by %+v, want the DDL queued before it", blockers)
	}
	// And the same in the other direction, since either side's NULL was
	// enough to lose the conflict.
	if b := blockersOf(t, conn, OperationDDL, ids["ddl"]); len(b) != 0 {
		t.Fatalf("the DDL arrived first, so nothing holds it: %+v", b)
	}
	ids2 := insertOps(t, conn, []queueOp{
		{name: "placement2", kind: OperationPlacement, state: "pending", database: "other", arrival: 3},
		{name: "ddl2", kind: OperationDDL, state: "queued", database: "other", arrival: 4},
	})
	if b := blockersOf(t, conn, OperationDDL, ids2["ddl2"]); len(b) != 1 || b[0].ID != ids2["placement2"] {
		t.Fatalf("a DDL behind a placement with no stage is blocked by %+v, want the placement", b)
	}
}
