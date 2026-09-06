package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// The barrier reaches the CATALOG group through the controller's own pooled
// connection rather than through a shard DSN, and that connection is no
// longer the superuser's. Every one of its group operations therefore runs
// as pgshard_controller, and each is a statement whose privilege the
// migration has to have granted.
//
// This drives the real methods rather than the statements they contain.
// A test that checked the grants one at a time passed while
// CreateRestorePoint was still refused: it takes the restore point and then
// pg_switch_wal(), which is superuser-only for the same reason
// pg_create_restore_point is, and only the second was granted. Any statement
// added to these methods later is covered without this test being changed.
func TestTheBarriersCatalogWorkRunsAsTheControllerRole(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	adminDSN := startPostgresWith(t)
	if err := catalog.Migrate(ctx, connect(t, adminDSN)); err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = catalog.ControllerRole
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var who string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&who); err != nil {
		t.Fatalf("the controller role cannot log in: %v", err)
	}
	if who != catalog.ControllerRole {
		t.Fatalf("connected as %q, so this proves nothing", who)
	}

	groups := &SQLBarrierGroups{Pool: pool}
	cat := GroupRef{Name: CatalogGroup}
	if !cat.Catalog() {
		t.Fatal("the fixture is not addressing the catalog group")
	}

	if _, err := groups.CreateRestorePoint(ctx, cat, "pgshard-role-test"); err != nil {
		t.Errorf("CreateRestorePoint: %v", err)
	}
	if _, err := groups.PreparedGIDs(ctx, cat); err != nil {
		t.Errorf("PreparedGIDs: %v", err)
	}
	if _, err := groups.SubscriptionCount(ctx, cat); err != nil {
		t.Errorf("SubscriptionCount: %v", err)
	}
	if _, err := groups.WritersSince(ctx, cat, time.Now()); err != nil {
		t.Errorf("WritersSince: %v", err)
	}
	if _, err := groups.PauseEffective(ctx, cat); err != nil {
		t.Errorf("PauseEffective: %v", err)
	}
	for _, pause := range []bool{true, false} {
		if _, err := groups.PauseWrites(ctx, cat, pause); err != nil {
			t.Errorf("PauseWrites(%v): %v", pause, err)
		}
	}
	// archive_mode is off on a bare container, so this one is expected to
	// refuse -- but for that reason and not for want of a privilege.
	if _, err := groups.ArchivedThrough(ctx, cat); denied(err) {
		t.Errorf("ArchivedThrough: %v", err)
	}
}

// denied reports whether err is PostgreSQL refusing the statement for want
// of a privilege, which is the only failure this file is about.
func denied(err error) bool {
	var pge *pgconn.PgError
	return errors.As(err, &pge) && pge.Code == "42501"
}

// Role and DCL work on the catalog group is the superuser's, exactly as it
// is on every shard: CREATE ROLE needs CREATEROLE, a membership grant needs
// ADMIN on the role, and comparing a verifier means reading pg_authid. The
// controller's own login has none of those, so the operator gives it a
// second DSN for this path -- and running it through the pool instead is a
// cluster where no client role can be created at all.
func TestRoleWorkOnTheCatalogGroupNeedsTheSuperuser(t *testing.T) {
	parallelPG(t)
	ctx := context.Background()
	adminDSN := startPostgresWith(t)
	if err := catalog.Migrate(ctx, connect(t, adminDSN)); err != nil {
		t.Fatal(err)
	}
	desired := &catalog.DesiredRoles{Generation: 1, Roles: []catalog.DesiredRole{{Name: "app", Login: true, ConnectionLimit: -1}}}

	super := DSNDialer(adminDSN)
	if err := MaterializeRoles(ctx, func(ctx context.Context, _ string) (ShardConn, error) { return super(ctx) }, desired, false); err != nil {
		t.Fatalf("the superuser dialer cannot materialize a role on the catalog group: %v", err)
	}

	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = catalog.ControllerRole
	asRole := func(ctx context.Context) (ShardConn, error) {
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return pgxShardConn{c}, nil
	}
	err = MaterializeRoles(ctx, func(ctx context.Context, _ string) (ShardConn, error) { return asRole(ctx) },
		&catalog.DesiredRoles{Generation: 2, Roles: []catalog.DesiredRole{{Name: "app2", Login: true, ConnectionLimit: -1}}}, false)
	if !denied(err) {
		t.Fatalf("the controller role created a role on the catalog group (%v); if that is now allowed, --catalog-role-dsn is dead weight", err)
	}
}
