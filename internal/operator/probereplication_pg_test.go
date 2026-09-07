package operator

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
