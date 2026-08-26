package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestRoleVerifierWithPostgres runs the verifier against one server that is
// both the catalog and shard 0 (the catalog group is the same server).
func TestRoleVerifierWithPostgres(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch) VALUES ('default', 0, 'g0', 'serving', 1)`)
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('postgres')`)
	mustExec(t, conn, `CREATE TABLE orders (id int)`)
	mustExec(t, conn, `CREATE ROLE stranger LOGIN`)
	verifier, err := pgwire.BuildSCRAMVerifier("pw", nil, pgwire.DefaultSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.roles (rolname, verifier, login, connection_limit, valid_until) VALUES ('app', $1, true, 5, '2031-01-01T00:00:00Z'), ('readers', NULL, false, -1, NULL), ('postgres', NULL, true, -1, NULL)`, verifier.String())
	mustExec(t, conn, `INSERT INTO pgshard.role_members (rolname, member, admin_option) VALUES ('readers', 'app', true)`)
	mustExec(t, conn, `INSERT INTO pgshard.grants (rolname, database, object_kind, object_name, privileges, grant_option) VALUES ('app', 'postgres', 'table', 'orders', '{SELECT,UPDATE}', false)`)
	mustExec(t, conn, `INSERT INTO pgshard.role_settings (rolname, database, name, value) VALUES ('app', '', 'work_mem', '64MB')`)

	v := &RoleVerifier{Store: &PGRoleStore{Pool: pool}, Shards: &PgxShardDialer{Pool: pool, DSNs: map[ShardRef]string{{Set: "default", ID: 0}: dsn}},
		Catalog: CatalogDialer(pool), Logger: slog.New(slog.DiscardHandler)}
	statuses := func() map[string]string {
		t.Helper()
		rows, err := ListRoleStatus(ctx, pool, "")
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, r := range rows {
			var parts []string
			for k, v := range r.Details {
				parts = append(parts, fmt.Sprintf("%s=%v", k, v))
			}
			out[r.Role+"@"+r.Group] = strings.TrimSpace(r.State + " " + strings.Join(parts, " "))
		}
		return out
	}

	t.Run("stale_groups_are_materialized", func(t *testing.T) {
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		got := statuses()
		for _, k := range []string{"app@default/0", "readers@default/0", "app@catalog", "readers@catalog"} {
			if got[k] != "in_sync" {
				t.Fatalf("%s = %q (%v)", k, got[k], got)
			}
		}
		if got["stranger@default/0"] != "unmanaged note=not in pgshard.roles; left alone" {
			t.Fatalf("unmanaged: %v", got)
		}
		if got := got["postgres@default/0"]; !strings.HasPrefix(got, "unmanaged_superuser ") || !strings.Contains(got, "superuser=true") {
			t.Fatalf("a listed superuser is reported, never altered: %q", got)
		}
		if !queryOne[bool](t, conn, `SELECT rolsuper AND rolcreaterole FROM pg_authid WHERE rolname = 'postgres'`) {
			t.Fatal("the listed superuser was demoted")
		}
		if got := queryOne[string](t, conn, `SELECT rolpassword FROM pg_authid WHERE rolname = 'app'`); got != verifier.String() {
			t.Fatalf("verifier %q", got)
		}
		if got := queryOne[string](t, conn, `SELECT rolcanlogin::text || rolconnlimit::text || rolvaliduntil::date::text || (SELECT admin_option::text FROM pg_auth_members WHERE roleid = 'readers'::regrole AND member = 'app'::regrole) FROM pg_authid WHERE rolname = 'app'`); got != "true52031-01-01true" {
			t.Fatalf("attributes/membership %q", got)
		}
		if !queryOne[bool](t, conn, `SELECT has_table_privilege('app', 'orders', 'SELECT') AND has_table_privilege('app', 'orders', 'UPDATE') AND NOT has_table_privilege('app', 'orders', 'INSERT')`) {
			t.Fatal("grants not materialized")
		}
		if got := queryOne[string](t, conn, `SELECT setconfig[1] FROM pg_db_role_setting WHERE setrole = 'app'::regrole`); got != "work_mem=64MB" {
			t.Fatalf("setting %q", got)
		}
		if gen := queryOne[int64](t, conn, `SELECT roles_generation FROM pgshard.role_group_status WHERE group_name = 'catalog'`); gen == 0 {
			t.Fatal("group generation not recorded")
		}
	})

	// Every scenario below starts from materialized roles. The parent body
	// runs even when -run targets a single subtest, so the baseline has to
	// be here rather than a side effect of the subtest above: otherwise
	// running one of them alone fails on roles that were never created,
	// which reports the wrong scenario as broken. RunOnce is a reconcile, so
	// repeating it after the subtest above is a no-op.
	if err := v.RunOnce(ctx); err != nil {
		t.Fatalf("materializing the baseline the scenarios below start from: %v", err)
	}

	t.Run("password_changed_out_of_band_is_repaired", func(t *testing.T) {
		mustExec(t, conn, `ALTER ROLE app PASSWORD 'hijacked' CONNECTION LIMIT 99`)
		mustExec(t, conn, `REVOKE UPDATE ON orders FROM app`)
		mustExec(t, conn, `REVOKE readers FROM app`)
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		got := statuses()["app@default/0"]
		for _, want := range []string{"drifted", "verifier=differs", "attributes=[connection_limit]", "missing_grants=[UPDATE ON table orders]", "missing_memberships=[readers]", "repaired=true"} {
			if !strings.Contains(got, want) {
				t.Fatalf("status %q lacks %q", got, want)
			}
		}
		if got := queryOne[string](t, conn, `SELECT rolpassword FROM pg_authid WHERE rolname = 'app'`); got != verifier.String() {
			t.Fatalf("verifier not repaired: %q", got)
		}
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := statuses(); got["app@default/0"] != "in_sync" || got["app@catalog"] != "in_sync" {
			t.Fatalf("after repair: %v", got)
		}
	})

	t.Run("dropped_role_is_missing_then_recreated", func(t *testing.T) {
		mustExec(t, conn, `DROP ROLE readers`)
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := statuses()["readers@default/0"]; got != "missing repaired=true" {
			t.Fatalf("%q", got)
		}
		if !queryOne[bool](t, conn, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readers')`) {
			t.Fatal("readers not recreated")
		}
	})

	t.Run("waits_while_role_migrations_run", func(t *testing.T) {
		mustExec(t, conn, `ALTER ROLE app CONNECTION LIMIT 42`)
		id, err := catalog.EnqueueMigration(ctx, conn, catalog.DDLMigration{Database: "postgres", Statement: "create role x", Kind: "CREATE ROLE", Strategy: "direct", Scope: "all"})
		if err != nil {
			t.Fatal(err)
		}
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := queryOne[int32](t, conn, `SELECT rolconnlimit FROM pg_authid WHERE rolname = 'app'`); got != 42 {
			t.Fatal("verifier must not touch roles while a role migration is pending")
		}
		mustExec(t, conn, `UPDATE pgshard.migrations SET state = 'failed' WHERE id = $1`, id)
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := queryOne[int32](t, conn, `SELECT rolconnlimit FROM pg_authid WHERE rolname = 'app'`); got != 5 {
			t.Fatal("not repaired once the migration finished")
		}
	})

	t.Run("new_desired_generation_rematerializes", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.roles (rolname, login) VALUES ('newbie', true)`)
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if !queryOne[bool](t, conn, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'newbie')`) {
			t.Fatal("newbie not created")
		}
		if got := statuses()["newbie@catalog"]; got != "in_sync" {
			t.Fatalf("%q", got)
		}
		if behind := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.role_group_status WHERE roles_generation < (SELECT max(desired_generation) FROM pgshard.roles)`); behind != 0 {
			t.Fatalf("%d groups were not materialized at the new generation", behind)
		}
	})

	t.Run("unmanaged_roles_are_never_touched", func(t *testing.T) {
		mustExec(t, conn, `ALTER ROLE stranger PASSWORD 'theirs'`)
		if err := v.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if !queryOne[bool](t, conn, `SELECT rolpassword IS NOT NULL FROM pg_authid WHERE rolname = 'stranger'`) {
			t.Fatal("stranger was modified")
		}
	})

	t.Run("grpc_lists_status", func(t *testing.T) {
		resp, err := (&Server{Pool: pool}).ListRoleStatus(ctx, &pgshardv1.ListRoleStatusRequest{Role: "app"})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Statuses) != 2 || resp.Statuses[0].Group != "catalog" || resp.Statuses[0].State != pgshardv1.RoleStatus_STATE_IN_SYNC || resp.Statuses[1].Group != "default/0" || resp.Statuses[0].DetailsJson != "{}" {
			t.Fatalf("%v", resp.Statuses)
		}
	})

	t.Run("materializer_from_dsn", func(t *testing.T) {
		mustExec(t, conn, `DROP ROLE newbie`)
		d, err := catalog.LoadDesiredRoles(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewRolesMaterializer(dsn)(ctx, d); err != nil {
			t.Fatal(err)
		}
		if !queryOne[bool](t, conn, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'newbie')`) {
			t.Fatal("newbie not created")
		}
	})
}
