//go:build integration

package router

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPgrollAgainstAShardedDatabase runs the real pgroll binary against a
// database that is not local: a table sharded over two shards, alongside a
// reference table.
//
// This is the acceptance gate for stage 1 of PGS-765. The six pgshard
// changes it asked for are all merged -- functions, triggers and comments
// fan out; a pgroll migration of a shard key or a reference table is
// refused at start; a database can run a transaction's DDL statement by
// statement; a schema can be pinned to the home shard; a migration is
// answered once the router's snapshot has it; and DDL during a reshard is
// queued rather than refused. Whether pgroll actually works over them is a
// different question, and only the real binary answers it.
//
//	go install github.com/xataio/pgroll@v0.16.3
func TestPgrollAgainstAShardedDatabase(t *testing.T) {
	bin := os.Getenv("PGSHARD_TEST_PGROLL")
	if bin == "" {
		bin = os.Getenv("HOME") + "/go/bin/pgroll"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skip("no pgroll binary: go install github.com/xataio/pgroll@v0.16.3")
	}
	s := startShardedStackWith(t, nil, nil)
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	s.awaitReference(t, conn)

	// The database stays sharded. What it gains is what stage 1 gives
	// pgroll: its state schema pinned to the home shard, and DDL inside a
	// transaction run statement by statement.
	pool, err := pgxpool.New(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `UPDATE pgshard.databases SET local_schemas = '{pgroll}', ddl_transactions = 'sequential' WHERE name = $1`, appDatabase); err != nil {
		t.Fatal(err)
	}
	// pgroll installs event triggers, which PostgreSQL allows only to a
	// superuser, on any PostgreSQL and not only through pgshard. pgshard
	// refuses a superuser's fanned-out DDL, because the applier runs a
	// client's statement by SET ROLE into that client and a superuser there
	// would be an escalation. The two are only compatible in sequence: the
	// role is a superuser while pgroll installs its state, and a plain role
	// for every migration afterwards.
	superuser := func(on bool) {
		t.Helper()
		verb := "NOSUPERUSER"
		if on {
			verb = "SUPERUSER"
		}
		for id, dsn := range s.shardDSNs {
			sh, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatalf("shard %d: %v", id, err)
			}
			if _, err := sh.Exec(ctx, `ALTER ROLE `+appRole+` `+verb); err != nil {
				sh.Close()
				t.Fatalf("shard %d: %v", id, err)
			}
			sh.Close()
		}
	}
	superuser(true)
	// pgroll alters the table, so the role it connects as has to own it.
	// The fixture creates it as the superuser and grants; ownership is what
	// ALTER TABLE asks for, on any PostgreSQL.
	for id, dsn := range s.shardDSNs {
		sh, err := pgxpool.New(ctx, strings.Replace(dsn, "/postgres?", "/"+appDatabase+"?", 1))
		if err != nil {
			t.Fatalf("shard %d: %v", id, err)
		}
		for _, table := range []string{"orders", "regions"} {
			if _, err := sh.Exec(ctx, `ALTER TABLE `+table+` OWNER TO `+appRole); err != nil {
				sh.Close()
				t.Fatalf("shard %d: %s: %v", id, table, err)
			}
		}
		sh.Close()
	}
	// Let the router pick the catalog change up.
	time.Sleep(3 * time.Second)

	url := s.dsn(appRole, appPassword, appDatabase)
	run := func(what string, args ...string) (string, error) {
		c, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()
		out, err := exec.CommandContext(c, bin, append(args, "--postgres-url", url)...).CombinedOutput()
		fmt.Printf("PGROLL %s %v -> err=%v\n%s\n", what, args, err, out)
		return string(out), err
	}

	if _, err := run("init", "init"); err != nil {
		t.Fatalf("pgroll init against a sharded database: %v", err)
	}
	// Idempotent: the tool is run again by anyone who is not sure.
	if _, err := run("init again", "init"); err != nil {
		t.Errorf("pgroll init is not idempotent: %v", err)
	}

	// Its state is installed; from here it is an ordinary client, and every
	// migration below is a fanned-out one.
	superuser(false)

	// Rows on both shards, so anything pgroll does has to reach both.
	t0, t1 := twoTenants(t)
	for _, v := range []struct {
		tenant int64
		id     int
	}{{t0, 1}, {t0, 2}, {t1, 3}} {
		if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", v.tenant, v.id); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	// The schema is not empty -- the sharded table and the reference table
	// are already there -- and pgroll refuses to start a migration over a
	// schema with no history of its own until it has a baseline. It says
	// so and exits 0, so the exit code is not the thing to read.
	if _, err := run("baseline", "baseline", "00_existing", dir, "--yes"); err != nil {
		t.Fatalf("pgroll baseline: %v", err)
	}

	addColumn := dir + "/01_note.json"
	if err := os.WriteFile(addColumn, []byte(`{
	  "operations": [
	    {"add_column": {"table": "orders", "column": {"name": "note", "type": "text", "nullable": true}}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// pgroll exits 0 on a refusal it prints, so the output is the verdict.
	out, err := run("start add_column", "start", addColumn)
	if err != nil || strings.Contains(out, "Run `pgroll baseline`") {
		t.Fatalf("pgroll start of an add_column on a sharded table: %v\n%s", err, out)
	}

	// Every shard has the column: a migration that reached one of them is
	// the failure this whole ticket is about.
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "orders", "_pgroll_new_note") && !s.columnOn(t, id, "orders", "note") {
			t.Errorf("shard %d has neither note nor _pgroll_new_note after start", id)
		}
	}

	// The version schema exists and routes: reading through it must reach
	// both shards, not the home shard alone.
	var version string
	if err := conn.QueryRow(ctx, `SELECT pgroll.latest_version('public')`).Scan(&version); err != nil {
		t.Fatalf("no version schema after start: %v", err)
	}
	vc := s.connect(t)
	if _, err := vc.Exec(ctx, `SET search_path TO "public_`+version+`"`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := vc.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatalf("reading through the version view: %v", err)
	}
	if n != 3 {
		t.Errorf("the version view answered %d rows, want 3 -- a view routed to one shard answers 2", n)
	}
	// And the schema and its view are on EVERY shard, not only the one the
	// read happened to reach: a view that exists on one shard answers until
	// a statement routes to another (PGS-873).
	for id := range s.shardDSNs {
		if !s.viewOn(t, id, "public_"+version, "orders") {
			t.Errorf("shard %d has no orders view in the version schema public_%s", id, version)
		}
		// The check is only worth its line if it can fail.
		if s.viewOn(t, id, "public_no_such_version", "orders") {
			t.Fatalf("shard %d reports a view in a schema that does not exist, so the check above proves nothing", id)
		}
		// And while the migration is in flight the shard DOES hold
		// _pgroll_ state, so the emptiness asserted after the refusal
		// below is a real absence.
		if left := s.pgrollArtefacts(t, id); len(left) == 0 {
			t.Fatalf("shard %d reports no _pgroll_ state mid-migration, so the check after the refusal proves nothing", id)
		}
	}

	if _, err := run("complete", "complete"); err != nil {
		t.Fatalf("pgroll complete: %v", err)
	}
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "orders", "note") {
			t.Errorf("shard %d does not have the completed column", id)
		}
		if s.columnOn(t, id, "orders", "_pgroll_new_note") {
			t.Errorf("shard %d still has the temporary column after complete", id)
		}
	}

	// A migration pgshard cannot complete is refused at its start, and
	// pgroll is left able to start another rather than wedged.
	shardKey := dir + "/02_shardkey.json"
	if err := os.WriteFile(shardKey, []byte(`{
	  "operations": [
	    {"alter_column": {"table": "orders", "column": "tenant_id", "type": "numeric",
	      "up": "tenant_id", "down": "tenant_id"}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = run("start shard key", "start", shardKey)
	if err == nil {
		t.Error("a pgroll migration of the shard key was accepted; completing it drops and renames the shard key")
	} else if !strings.Contains(out, "shard key") {
		t.Errorf("the refusal does not say why: %s", out)
	}

	// A refused start still leaves pgroll holding a migration for the
	// schema, so the next one is refused with "already in progress" until
	// the failed one is rolled back. That is pgroll's own protocol, and the
	// point of refusing at start rather than at complete: rollback can undo
	// a start, and nothing can undo a half-completed rename.
	if _, err := run("rollback", "rollback"); err != nil {
		t.Fatalf("a refused start could not be rolled back, which leaves pgroll wedged: %v", err)
	}
	// Nothing of the refused migration is left anywhere: a _pgroll_ column
	// or table on any shard is state the next migration would trip over
	// (PGS-873).
	for id := range s.shardDSNs {
		if left := s.pgrollArtefacts(t, id); len(left) > 0 {
			t.Errorf("shard %d kept %v after the refused start was rolled back", id, left)
		}
	}

	// And the tool still works afterwards, which is the part that matters:
	// a refusal that wedges pgroll is worse than one that does not happen.
	index := dir + "/03_index.json"
	if err := os.WriteFile(index, []byte(`{
	  "operations": [
	    {"create_index": {"table": "orders", "name": "orders_note_idx", "columns": [{"column": "note"}]}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("start create_index", "start", index); err != nil {
		t.Fatalf("pgroll was wedged by the refusal: %v", err)
	}
	if _, err := run("complete index", "complete"); err != nil {
		t.Fatalf("pgroll complete after the refusal: %v", err)
	}
	for id := range s.shardDSNs {
		if !s.indexOn(t, id, "orders_note_idx") {
			t.Errorf("shard %d does not have the index pgroll created", id)
		}
	}

	// The rest of what stage 1 claims (PGS-929). add_column and
	// create_index above were the only two proved; these five are the
	// remainder, and each is run to its complete and checked on EVERY
	// shard, because reaching one shard is the failure this whole area is
	// about.
	//
	// Each step records what pgroll actually did rather than what the
	// design expects. A step that pgshard cannot carry is a finding, not a
	// test failure to paper over: it is written down here and on the
	// ticket, and the guide says untested rather than guaranteed.
	step := func(name, body string, check func()) {
		t.Helper()
		file := dir + "/" + name + ".json"
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := run("start "+name, "start", file)
		if err != nil || strings.Contains(out, "Run `pgroll baseline`") {
			t.Errorf("pgroll start of %s on a sharded table: %v\n%s", name, err, out)
			// Leave pgroll able to run the next one; a refused start holds
			// the schema until it is rolled back.
			_, _ = run("rollback "+name, "rollback")
			return
		}
		if _, err := run("complete "+name, "complete"); err != nil {
			t.Errorf("pgroll complete of %s: %v", name, err)
			_, _ = run("rollback "+name, "rollback")
			return
		}
		check()
	}

	step("04_drop_index", `{
	  "operations": [
	    {"drop_index": {"name": "orders_note_idx"}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if s.indexOn(t, id, "orders_note_idx") {
				t.Errorf("shard %d still has the index after drop_index completed", id)
			}
		}
	})

	step("05_rename_column", `{
	  "operations": [
	    {"rename_column": {"table": "orders", "from": "note", "to": "remark"}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if !s.columnOn(t, id, "orders", "remark") {
				t.Errorf("shard %d does not have the renamed column", id)
			}
			if s.columnOn(t, id, "orders", "note") {
				t.Errorf("shard %d still has the old column name", id)
			}
		}
	})

	// create_constraint is REFUSED, and the reason is worth pinning: pgroll
	// backfills the column to validate the constraint, and its backfill is a
	// data-modifying CTE, which the router does not support. Asserted rather
	// than skipped, so that this becoming possible -- or failing some other
	// way -- is noticed here rather than in somebody's migration.
	constraint := dir + "/06_create_constraint.json"
	if err := os.WriteFile(constraint, []byte(`{
	  "operations": [
	    {"create_constraint": {"table": "orders", "name": "orders_remark_short", "type": "check",
	      "check": "length(remark) < 100", "columns": ["remark"],
	      "up": {"remark": "remark"}, "down": {"remark": "remark"}}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = run("start create_constraint", "start", constraint)
	switch {
	case err == nil:
		t.Error("pgroll create_constraint was accepted; it backfills, and the backfill is a data-modifying CTE the router does not support -- if this now works, the guide and PGS-929 are out of date")
		_, _ = run("complete create_constraint", "complete")
	case !strings.Contains(out, "backfill"):
		t.Errorf("create_constraint failed for a reason other than the backfill, which is the documented limit: %s", out)
	}

	// The constraint the next step renames is made with plain DDL through
	// the router, which fans it out: rename_constraint is a metadata change
	// and is testable on its own, and blocking it behind an operation that
	// cannot work would leave it untested for the wrong reason.
	if _, err := conn.Exec(ctx, `ALTER TABLE orders ADD CONSTRAINT orders_remark_short CHECK (length(remark) < 100) NOT VALID`); err != nil {
		t.Fatalf("adding the constraint through the router: %v", err)
	}
	for id := range s.shardDSNs {
		if !s.constraintOn(t, id, "orders", "orders_remark_short") {
			t.Fatalf("shard %d did not get the constraint, so the rename below would prove nothing", id)
		}
	}

	step("07_rename_constraint", `{
	  "operations": [
	    {"rename_constraint": {"table": "orders", "from": "orders_remark_short", "to": "orders_remark_len"}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if !s.constraintOn(t, id, "orders", "orders_remark_len") {
				t.Errorf("shard %d does not have the renamed constraint", id)
			}
			if s.constraintOn(t, id, "orders", "orders_remark_short") {
				t.Errorf("shard %d still has the old constraint name", id)
			}
		}
	})

	// drop_column takes the constraint with it, which is the point: the
	// column's dependents have to go on every shard, not just the home one.
	step("08_drop_column", `{
	  "operations": [
	    {"drop_column": {"table": "orders", "column": "remark", "down": "''"}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if s.columnOn(t, id, "orders", "remark") {
				t.Errorf("shard %d still has the column after drop_column completed", id)
			}
		}
	})

	// PGS-882 finding (1), measured rather than assumed: a migration whose
	// FIRST operation pgshard can carry and whose SECOND touches the shard
	// key. The ticket has it running the first operation and refusing the
	// second, after which pgroll's rollback cannot undo the rename the
	// first one made -- data loss reported as a clean rollback. Measured
	// against the pgroll this test pins: the migration is refused at start
	// with nothing changed, so the sequence the ticket feared does not
	// happen here, and this keeps it that way.
	step("09_keepme", `{
	  "operations": [
	    {"add_column": {"table": "orders", "column": {"name": "keepme", "type": "text", "nullable": true}}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if !s.columnOn(t, id, "orders", "keepme") {
				t.Fatalf("shard %d has no keepme, so the experiment below proves nothing", id)
			}
		}
	})

	// A statement over the home-only pgroll schema is cached by the driver.
	// The session then reads a tenant on ANOTHER shard: replaying that
	// statement onto a shard where the schema does not exist used to fail
	// the whole replay and leave the session unable to run anything.

	if err := conn.QueryRow(ctx, `SELECT pgroll.latest_version('public')`).Scan(&version); err != nil {
		t.Fatalf("reading pgroll's own state through the router: %v", err)
	}
	before := map[int]string{}
	tenantOf := map[int]int64{1: t0, 2: t0, 3: t1}
	for id, tenant := range tenantOf {
		keep := fmt.Sprintf("keep-%d", id)
		if _, err := conn.Exec(ctx, "update orders set keepme = $1 where tenant_id = $2 and id = $3", keep, tenant, id); err != nil {
			t.Fatalf("a session that prepared a home-only statement cannot write on another shard: %v", err)
		}
		before[id] = keep
	}

	multi := dir + "/10_multi.json"
	if err := os.WriteFile(multi, []byte(`{
	  "operations": [
	    {"alter_column": {"table": "orders", "column": "keepme", "type": "varchar(64)",
	      "up": "keepme", "down": "keepme"}},
	    {"drop_column": {"table": "orders", "column": "tenant_id", "down": "1"}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("start multi-operation touching the shard key", "start", multi); err == nil {
		t.Error("a migration whose second operation drops the shard key was accepted at start")
	}
	_, _ = run("rollback multi", "rollback")

	// What matters is not the refusal but what it left behind.
	for id, want := range before {
		var got string
		if err := conn.QueryRow(ctx, "select coalesce(keepme, '') from orders where tenant_id = $1 and id = $2", tenantOf[id], id).Scan(&got); err != nil {
			t.Fatalf("reading keepme back for id %d: %v", id, err)
		}
		if got != want {
			t.Errorf("row %d lost its value: %q, want %q -- the first operation ran and the rollback could not undo it", id, got, want)
		}
	}
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "orders", "tenant_id") {
			t.Errorf("shard %d lost the shard key", id)
		}
		if s.columnOn(t, id, "orders", "_pgroll_new_keepme") {
			t.Errorf("shard %d kept _pgroll_new_keepme after the rollback", id)
		}
	}

	// PGS-966 (3): a column operation on the REFERENCE table. The ticket
	// expected pgroll to get as far as installing per-shard down triggers;
	// measured, the first statement it sends is refused by name and pgroll
	// rolls its own start back, so the triggers are never reached.
	refColumn := dir + "/10_reference_add_column.json"
	if err := os.WriteFile(refColumn, []byte(`{
	  "operations": [
	    {"add_column": {"table": "regions", "column": {"name": "note", "type": "text", "nullable": true}}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = run("start a column operation on the reference table", "start", refColumn)
	if err == nil {
		t.Error("a pgroll column operation on a reference table was accepted")
	} else if !strings.Contains(out, "reference table") {
		t.Errorf("the refusal does not say why: %s", out)
	}

	// PGS-966 (2): drop_table of the reference table. The ticket's worry is
	// that pgroll's automatic rollback renames the table back and that the
	// rename back is refused too, leaving pgroll in_progress and the table
	// under a name nothing routes to. Measured: the rename that starts the
	// operation is refused, pgroll rolls the start back itself -- so
	// `pgroll rollback` then reports "no active migration" -- and the table
	// is untouched.
	dropTable := dir + "/11_drop_table.json"
	if err := os.WriteFile(dropTable, []byte(`{
	  "operations": [
	    {"drop_table": {"name": "regions"}}
	  ]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = run("start drop_table of the reference table", "start", dropTable)
	if err == nil {
		t.Error("a pgroll drop_table of a reference table was accepted")
	} else if !strings.Contains(out, "renaming the sharded or reference table") {
		t.Errorf("the refusal does not say why: %s", out)
	}
	if out, rerr := run("rollback drop_table", "rollback"); rerr != nil && !strings.Contains(out, "no active migration") {
		t.Errorf("a refused drop_table left pgroll holding something it cannot roll back: %v\n%s", rerr, out)
	}
	for id := range s.shardDSNs {
		if !s.columnOn(t, id, "regions", "id") {
			t.Errorf("shard %d no longer has the reference table under its own name", id)
		}
		if s.columnOn(t, id, "_pgroll_del_regions", "id") {
			t.Errorf("shard %d has the table under pgroll's deleted name", id)
		}
	}

	// And pgroll is not wedged by either refusal: it starts and completes
	// the next migration. A refusal that wedges the tool is worse than one
	// that does not happen.
	step("12_after_refusals", `{
	  "operations": [
	    {"add_column": {"table": "orders", "column": {"name": "after_refusals", "type": "text", "nullable": true}}}
	  ]
	}`, func() {
		for id := range s.shardDSNs {
			if !s.columnOn(t, id, "orders", "after_refusals") {
				t.Errorf("shard %d did not get the column pgroll added after the refusals", id)
			}
		}
	})

}

// constraintOn reports whether one shard's copy of a table has a named
// constraint.
func (s *shardedStack) constraintOn(tb testing.TB, shard int, table, name string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM pg_constraint k
		JOIN pg_class c ON c.oid = k.conrelid WHERE c.relname = $1 AND k.conname = $2)`, table, name)
}

// columnOn reports whether one shard's copy of a table has a column.
func (s *shardedStack) columnOn(tb testing.TB, shard int, table, column string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_name = $1 AND column_name = $2)`, table, column)
}

// indexOn reports whether one shard has an index by name.
func (s *shardedStack) indexOn(tb testing.TB, shard int, name string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, name)
}

func (s *shardedStack) shardBool(tb testing.TB, shard int, sql string, args ...any) bool {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, strings.Replace(s.shardDSNs[shard], "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		tb.Fatalf("shard %d: %v", shard, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var ok bool
	if err := conn.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
		tb.Fatalf("shard %d: %s: %v", shard, sql, err)
	}
	return ok
}

// viewOn reports whether one shard has a view by name in a schema.
func (s *shardedStack) viewOn(tb testing.TB, shard int, schema, view string) bool {
	tb.Helper()
	return s.shardBool(tb, shard, `SELECT EXISTS (SELECT 1 FROM pg_views WHERE schemaname = $1 AND viewname = $2)`, schema, view)
}

// pgrollArtefacts lists the _pgroll_ columns and tables one shard holds, so
// a refusal can be shown to have left nothing behind.
func (s *shardedStack) pgrollArtefacts(tb testing.TB, shard int) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, strings.Replace(s.shardDSNs[shard], "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		tb.Fatalf("shard %d: %v", shard, err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT table_name || '.' || column_name FROM information_schema.columns
			WHERE table_schema = 'public' AND column_name LIKE '\_pgroll\_%'
		UNION ALL
		SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '\_pgroll\_%'
		ORDER BY 1`)
	if err != nil {
		tb.Fatalf("shard %d: %v", shard, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatalf("shard %d: %v", shard, err)
	}
	return out
}
