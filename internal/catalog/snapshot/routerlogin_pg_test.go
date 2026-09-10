package snapshot

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestTheRouterLoginReadsEverythingAndWritesTwoTables.
//
// The router terminates untrusted client SQL, so it is the most likely
// thing in the system to be compromised. It used to be a member of
// pgshard_admin, which is full DML on every desired-state table -- roles,
// role_members, grants, databases, tables, shard_ranges -- while the
// comment above the grant said it had "only what it needs".
//
// It reads the whole catalog, because that is what planning is, and it
// writes the decision log it coordinates and the migration queue it
// enqueues into. Nothing else.
//
// The load half is what keeps this honest as the catalog grows: a later
// migration that adds a table the planner reads fails here rather than in
// production, where a router would simply stop being able to plan.
func TestTheRouterLoginReadsEverythingAndWritesTwoTables(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `ALTER ROLE pgshard_router WITH PASSWORD 'router-secret'`)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)
	mustExec(t, conn, `INSERT INTO pgshard.roles (rolname, verifier) VALUES ('alice', 'SCRAM-SHA-256$4096:salt$a:b')`)
	mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 0, '[,)')`)

	routerDSN := strings.Replace(dsn, "postgres://postgres@", "postgres://pgshard_router:router-secret@", 1)
	rc, err := pgx.Connect(ctx, routerDSN)
	if err != nil {
		t.Fatalf("the router cannot log in: %v", err)
	}
	defer func() { _ = rc.Close(ctx) }()

	// Everything the router plans against, through the loader the router
	// itself uses.
	snap, err := Load(ctx, rc)
	if err != nil {
		t.Fatalf("the router cannot load the catalog it plans against: %v", err)
	}
	if _, ok := snap.Databases["app"]; !ok {
		t.Fatal("the snapshot the router loaded is missing the database")
	}
	// Including the verifiers, which pgshard_reader is deliberately denied:
	// terminating SCRAM is the router's whole business with them.
	var verifier string
	if err := rc.QueryRow(ctx, `SELECT verifier FROM pgshard.roles WHERE rolname = 'alice'`).Scan(&verifier); err != nil {
		t.Fatalf("the router cannot read the verifier it authenticates against: %v", err)
	}

	// The two it writes.
	if _, err := rc.Exec(ctx, `INSERT INTO pgshard.xact_decisions (gid, state, participants) VALUES ('g1', 'preparing', '{}')`); err != nil {
		t.Fatalf("the router cannot write the decision log it coordinates: %v", err)
	}
	if _, err := rc.Exec(ctx, `INSERT INTO pgshard.migrations (id, database, statement, kind, strategy, scope, home_shard, state) VALUES (gen_random_uuid(), 'app', 'SELECT 1', 'X', 'direct', 'all', 0, 'queued')`); err != nil {
		t.Fatalf("the router cannot enqueue a migration: %v", err)
	}
	if _, err := rc.Exec(ctx, `SELECT pgshard.allocate_sequence_block('app.public.t.id', 100)`); err != nil {
		t.Fatalf("the router cannot allocate a sequence block: %v", err)
	}

	// And the ones a compromised router must not be able to write: each of
	// these is applied to every shard by the controller.
	for _, w := range []struct{ what, sql string }{
		{"a role", `INSERT INTO pgshard.roles (rolname) VALUES ('attacker')`},
		{"a role's verifier", `UPDATE pgshard.roles SET verifier = 'x' WHERE rolname = 'alice'`},
		{"a membership", `INSERT INTO pgshard.role_members (rolname, member) VALUES ('alice', 'alice')`},
		{"a grant", `INSERT INTO pgshard.grants (rolname, database, object_kind, object_name, privileges) VALUES ('alice', 'app', 'database', 'app', ARRAY['ALL'])`},
		{"a table's placement", `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 't', 'unsharded')`},
		{"the shard map", `UPDATE pgshard.shard_ranges SET range = '[,0)' WHERE shard_id = 0`},
	} {
		if _, err := rc.Exec(ctx, w.sql); err == nil {
			t.Errorf("the router wrote %s; a compromised router would then have it applied on every shard", w.what)
		} else if !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("writing %s was refused for the wrong reason: %v", w.what, err)
		}
	}
}
