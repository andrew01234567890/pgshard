package catalog

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// The controller reaches the catalog as pgshard_controller rather than as the
// superuser. Every capability below is one the BARRIER needs on the catalog
// group, which it reaches through that same connection rather than through a
// shard DSN -- so this exercises them as the role, not as the role's owner.
//
// The pg_stat_activity one is why this test exists at all: without
// pg_read_all_stats the columns come back NULL for other backends instead of
// erroring, so the barrier would count zero writers and certify a restore
// point while writers were still running. A missing grant there is a silent
// wrong answer, not a failure.
func TestTheControllerRoleCanDoWhatTheBarrierNeeds(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	adminDSN := startPostgres(t, candidateImages[0])
	admin := connect(t, adminDSN)
	if err := Migrate(ctx, admin); err != nil {
		t.Fatal(err)
	}

	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = ControllerRole
	cfg.Password = ""
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("the controller role cannot log in: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	var superuser bool
	if err := conn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil {
		t.Fatal(err)
	}
	if superuser {
		t.Fatal("the controller role is a superuser, which is the thing this replaces")
	}

	// The schema it drives.
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.databases (name, default_placement, home_shard) VALUES ('app', 'unsharded', 0)`); err != nil {
		t.Fatalf("the controller cannot write the schema it owns: %v", err)
	}
	if _, err := ClearStaleBarrierFence(ctx, conn); err != nil {
		t.Fatalf("recovery's own statement: %v", err)
	}

	// The barrier's restore point and the timeline recorded with it.
	var lsn int64
	if err := conn.QueryRow(ctx, `SELECT (lsn - '0/0'::pg_lsn)::bigint FROM pg_create_restore_point($1) AS lsn`, "pgshard-t").Scan(&lsn); err != nil {
		t.Fatalf("pg_create_restore_point: %v", err)
	}
	var timeline int64
	if err := conn.QueryRow(ctx, `SELECT timeline_id::bigint FROM pg_control_checkpoint()`).Scan(&timeline); err != nil {
		t.Fatalf("pg_control_checkpoint: %v", err)
	}

	// The write pause, and making it take effect.
	if _, err := conn.Exec(ctx, `ALTER SYSTEM SET default_transaction_read_only = on`); err != nil {
		t.Fatalf("the write pause: %v", err)
	}
	var reloaded bool
	if err := conn.QueryRow(ctx, `SELECT pg_reload_conf()`).Scan(&reloaded); err != nil {
		t.Fatalf("pg_reload_conf: %v", err)
	}
	if _, err := conn.Exec(ctx, `ALTER SYSTEM RESET default_transaction_read_only`); err != nil {
		t.Fatalf("lifting the write pause: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		t.Fatal(err)
	}

	// The one that fails silently. Another backend holds a real xid; the
	// controller must be able to SEE it, or the barrier counts no writers
	// and certifies a restore point taken while they were running.
	writer := connect(t, adminDSN)
	tx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE holds_an_xid (x int)`); err != nil {
		t.Fatal(err)
	}
	var writers int
	if err := conn.QueryRow(ctx, `SELECT count(*)::int FROM pg_stat_activity
		WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() AND backend_xid IS NOT NULL`).Scan(&writers); err != nil {
		t.Fatal(err)
	}
	if writers == 0 {
		t.Fatal("the controller cannot see another backend's transaction id: the barrier would certify a restore point while writers are running")
	}
}
