package operator

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// A standby streams as pgshard_replication rather than as the superuser:
// primary_conninfo is written into every standby's postgresql.auto.conf and
// travels in every clone, so the credential that reaches the most places is
// the one that can do the least.
//
// "The least" still has to be enough. This asserts the whole set against a
// real server -- streaming, the slot the clone creates before it starts, and
// the four functions pg_rewind calls on a source it is not superuser on --
// because each is a privilege PostgreSQL grants separately and a missing one
// does not show up until a failover is already under way.
func TestTheReplicationRoleCanStreamRewindAndNothingElse(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	// Twice: it runs on every pass, and a group rebuilt from elsewhere comes
	// back with whatever password it was cloned with.
	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "rotated"); err != nil {
		t.Fatalf("second pass: %v", err)
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = catalog.ReplicationRole
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("the replication role cannot log in: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	var super, canRep bool
	if err := conn.QueryRow(ctx,
		`SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user`).Scan(&super, &canRep); err != nil {
		t.Fatal(err)
	}
	if super {
		t.Error("the replication role is a superuser, which is the thing this replaces")
	}
	if !canRep {
		t.Error("the replication role cannot replicate")
	}

	// What pg_rewind runs on the source. Each is superuser-only unless
	// granted, and without them a rejoin after a failover falls through to a
	// full re-clone -- the same outcome, hours later.
	for _, sql := range []string{
		`SELECT pg_catalog.pg_ls_dir('.', true, false) LIMIT 1`,
		`SELECT pg_catalog.pg_stat_file('PG_VERSION', true)`,
		`SELECT pg_catalog.pg_read_binary_file('PG_VERSION')`,
		`SELECT pg_catalog.pg_read_binary_file('PG_VERSION', 0, 1, true)`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Errorf("pg_rewind cannot run %s: %v", sql, err)
		}
	}

	// The slot a clone creates on the source before it empties its own
	// PGDATA. Creating one needs the REPLICATION attribute, not a grant.
	if _, err := conn.Exec(ctx, `SELECT pg_create_physical_replication_slot('probe_slot')`); err != nil {
		t.Errorf("the replication role cannot create the slot a clone needs: %v", err)
	} else if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot('probe_slot')`); err != nil {
		t.Errorf("dropping it: %v", err)
	}

	// A walsender connection, which is what primary_conninfo opens and is a
	// different code path from any of the above.
	repCfg := cfg.Copy()
	repCfg.RuntimeParams["replication"] = "true"
	rep, err := pgx.ConnectConfig(ctx, repCfg)
	if err != nil {
		t.Fatalf("the replication role cannot open a walsender connection: %v", err)
	}
	if _, err := rep.PgConn().Exec(ctx, "IDENTIFY_SYSTEM").ReadAll(); err != nil {
		t.Errorf("IDENTIFY_SYSTEM: %v", err)
	}
	_ = rep.Close(ctx)

	// And nothing else: it is not a way back to the superuser's reach.
	if _, err := conn.Exec(ctx, `ALTER SYSTEM SET default_transaction_read_only = on`); !refused(err) {
		t.Errorf("the replication role can ALTER SYSTEM (%v)", err)
	}
	if _, err := conn.Exec(ctx, `CREATE ROLE probe_role LOGIN`); !refused(err) {
		t.Errorf("the replication role can create roles (%v)", err)
	}
}

// refused reports whether PostgreSQL turned the statement down for want of
// a privilege.
func refused(err error) bool {
	var pge *pgconn.PgError
	if errors.As(err, &pge) {
		return pge.Code == "42501"
	}
	return err != nil && strings.Contains(err.Error(), "permission denied")
}

// The conninfo the operator renders has to work as it stands, as the role
// it names, against a real server. An ordinary connection that omits dbname
// asks for a database named after the USER -- so the moment this stopped
// saying user=postgres it started asking for a database called
// pgshard_replication, which does not exist. pg_rewind, the wait for the
// source and the slot a clone creates on it all dial this string directly,
// and each would have failed: a clone that never bootstraps, a rejoin that
// never rewinds.
func TestTheRenderedConninfoConnectsAsTheRoleItNames(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	at, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}

	c := newCluster("wire")
	g := Groups(c)[0]
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false, true)
	var cfg agent.Config
	if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(1))]), &cfg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg.PrimaryConninfo, "user="+catalog.ReplicationRole) {
		t.Fatalf("the fixture no longer renders the role: %q", cfg.PrimaryConninfo)
	}

	// Only the address is swapped for the container's; everything else is
	// what a member is given.
	rendered, err := pgx.ParseConfig(cfg.PrimaryConninfo + " sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	rendered.Host, rendered.Port, rendered.Password = at.Host, at.Port, "s3cr3t"
	conn, err := pgx.ConnectConfig(ctx, rendered)
	if err != nil {
		t.Fatalf("the rendered primary_conninfo does not connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	var who, db string
	if err := conn.QueryRow(ctx, `SELECT current_user, current_database()`).Scan(&who, &db); err != nil {
		t.Fatal(err)
	}
	if who != catalog.ReplicationRole {
		t.Errorf("connected as %q", who)
	}
	if db != "postgres" {
		t.Errorf("connected to database %q, want postgres", db)
	}
}

// The password is reapplied on every pass so a group rebuilt from elsewhere
// comes back in step with the Secret -- but only when it is actually
// different. ALTER ROLE draws a fresh SCRAM salt every time, so doing it
// unconditionally writes pg_authid and its WAL on every pass for every
// group, and under log_statement=ddl puts the password in each primary's
// log that often too.
func TestTheReplicationPasswordIsOnlyWrittenWhenItChanges(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	stored := func() string {
		t.Helper()
		var v *string
		if err := conn.QueryRow(ctx,
			`SELECT rolpassword FROM pg_authid WHERE rolname = $1`, catalog.ReplicationRole).Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v == nil {
			return ""
		}
		return *v
	}

	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "first"); err != nil {
		t.Fatal(err)
	}
	first := stored()
	if first == "" {
		t.Fatal("no verifier was written at all")
	}
	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "first"); err != nil {
		t.Fatal(err)
	}
	if stored() != first {
		t.Error("the same password rewrote the verifier; every pass would churn pg_authid and its WAL")
	}
	// A rotated Secret, or a group restored with an older one, still lands.
	if err := (PgxProber{}).EnsureReplicationRole(ctx, dsn, "second"); err != nil {
		t.Fatal(err)
	}
	if stored() == first {
		t.Error("a changed password did not reach the role, so a restored group stays locked out")
	}
}
