//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// everywhere evaluates a scalar SQL expression on every shard and the
// catalog, returning the distinct results.
func (s *ddlStack) everywhere(tb testing.TB, sql string, args ...any) []string {
	tb.Helper()
	seen := map[string]bool{}
	var out []string
	dsns := []string{s.shardDSNs[0], s.shardDSNs[1], s.shardDSNs[2], s.catalogDSN}
	for _, dsn := range dsns {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			tb.Fatal(err)
		}
		var v string
		if err := conn.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
			tb.Fatalf("%s: %s: %v", dsn, sql, err)
		}
		_ = conn.Close(context.Background())
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (s *ddlStack) roleStatus(tb testing.TB, role string) map[string]string {
	tb.Helper()
	cat, err := pgx.Connect(context.Background(), s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(context.Background()) }()
	rows, err := cat.Query(context.Background(), `SELECT group_name, state || ' ' || details::text FROM pgshard.role_status WHERE rolname = $1`, role)
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var g, st string
		if err := rows.Scan(&g, &st); err != nil {
			tb.Fatal(err)
		}
		out[g] = st
	}
	return out
}

// awaitRolesMaterialized waits until every group was materialized at the
// current desired roles generation.
func (s *ddlStack) awaitRolesMaterialized(tb testing.TB) {
	tb.Helper()
	cat, err := pgx.Connect(context.Background(), s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(context.Background()) }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var behind int
		if err := cat.QueryRow(context.Background(), `SELECT 4 - count(*) FROM pgshard.role_group_status WHERE roles_generation >= (SELECT max(desired_generation) FROM pgshard.roles)`).Scan(&behind); err != nil {
			tb.Fatal(err)
		}
		if behind == 0 {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("%d groups never caught up with the desired roles\ncontroller log:\n%s", behind, s.controllerLog.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestRouterRolesAndGrants(t *testing.T) {
	s := startDDLStack(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	if _, err := conn.Exec(ctx, "create table orders (tenant_id int8, id int, primary key (tenant_id, id))"); err != nil {
		t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
	}
	// The client role needs CREATEROLE; declaring it in the catalog lets the
	// controller materialize it on every group.
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Exec(ctx, "update pgshard.roles set createrole = true where rolname = $1", appRole); err != nil {
		t.Fatal(err)
	}
	_ = cat.Close(ctx)
	s.awaitRolesMaterialized(t)
	if st := s.roleStatus(t, appRole); len(st) != 4 || st["default/1"] != "in_sync {}" || st["catalog"] != "in_sync {}" {
		t.Fatalf("app status: %v", st)
	}
	if got := s.onShards(t, "select rolcreaterole from pg_roles where rolname = '"+appRole+"'"); !allTrue(got) {
		t.Fatalf("createrole per shard: %v", got)
	}

	tryLogin := func(user, password string) error {
		c, err := pgx.Connect(ctx, s.dsn(user, password, appDatabase))
		if err != nil {
			return err
		}
		var one int
		err = c.QueryRow(ctx, "select 1").Scan(&one)
		_ = c.Close(ctx)
		return err
	}
	awaitLogin := func(t *testing.T, user, password string, want bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			err := tryLogin(user, password)
			if (err == nil) == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("login %s/%s ok=%v wanted %v (last: %v)\nrouter log:\n%s", user, password, err == nil, want, err, s.routerLog.String())
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	t.Run("create_role_logs_in_with_one_verifier_everywhere", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "create role analyst login password 'first'")
		if err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if tag.String() != "CREATE ROLE" {
			t.Fatalf("tag %q", tag)
		}
		r := s.migration(t, "kind = 'CREATE ROLE'")
		if r.state != "complete" || strings.Count(r.perShard, `"applied"`) != 4 || !strings.Contains(r.perShard, `"catalog"`) {
			t.Fatalf("migration row %+v", r)
		}
		verifiers := s.everywhere(t, "select rolpassword from pg_authid where rolname = 'analyst'")
		if len(verifiers) != 1 || !strings.HasPrefix(verifiers[0], "SCRAM-SHA-256$") {
			t.Fatalf("verifiers per group: %v", verifiers)
		}
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		var desired string
		var login bool
		if err := cat.QueryRow(ctx, "select verifier, login from pgshard.roles where rolname = 'analyst'").Scan(&desired, &login); err != nil {
			t.Fatal(err)
		}
		_ = cat.Close(ctx)
		if desired != verifiers[0] || !login {
			t.Fatalf("catalog desired verifier %q login %v", desired, login)
		}
		awaitLogin(t, "analyst", "first", true)
		if err := tryLogin("analyst", "wrong"); sqlstate(err) != "28P01" {
			t.Fatalf("wrong password: %v", err)
		}
	})

	t.Run("grant_on_sharded_table_reaches_every_shard", func(t *testing.T) {
		for i, tenant := range []int{1, 2, 3, -5} {
			if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, i); err != nil {
				t.Fatal(err)
			}
		}
		ac, err := pgx.Connect(ctx, s.dsn("analyst", "first", appDatabase))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ac.Close(ctx) }()
		var n int
		if err := ac.QueryRow(ctx, "select count(*) from orders").Scan(&n); sqlstate(err) != "42501" {
			t.Fatalf("before the grant: %v", err)
		}
		if _, err := conn.Exec(ctx, "grant select on orders to analyst"); err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if got := s.onShards(t, "select has_table_privilege('analyst', 'orders', 'SELECT')"); !allTrue(got) {
			t.Fatalf("privilege per shard: %v", got)
		}
		if err := ac.QueryRow(ctx, "select count(*) from orders").Scan(&n); err != nil || n != 4 {
			t.Fatalf("scatter as analyst: %d %v", n, err)
		}
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		var privs string
		if err := cat.QueryRow(ctx, "select privileges::text from pgshard.grants where rolname = 'analyst' and object_name = 'orders'").Scan(&privs); err != nil || privs != "{SELECT}" {
			t.Fatalf("desired grant %q %v", privs, err)
		}
		_ = cat.Close(ctx)
	})

	t.Run("password_change_takes_effect_once_applied_everywhere", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "alter role analyst password 'second'"); err != nil {
			t.Fatalf("%v\ncontroller log:\n%s", err, s.controllerLog.String())
		}
		if verifiers := s.everywhere(t, "select rolpassword from pg_authid where rolname = 'analyst'"); len(verifiers) != 1 {
			t.Fatalf("verifiers per group: %v", verifiers)
		}
		awaitLogin(t, "analyst", "second", true)
		awaitLogin(t, "analyst", "first", false)
	})

	t.Run("membership_and_settings_fan_out", func(t *testing.T) {
		for _, sql := range []string{"create role readers", "grant readers to analyst with admin option", "alter role analyst set work_mem = '7MB'"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v\ncontroller log:\n%s", sql, err, s.controllerLog.String())
			}
		}
		if got := s.everywhere(t, "select admin_option::text from pg_auth_members where roleid = 'readers'::regrole and member = 'analyst'::regrole"); fmt.Sprint(got) != "[true]" {
			t.Fatalf("membership per group: %v", got)
		}
		if got := s.everywhere(t, "select setconfig::text from pg_db_role_setting where setrole = 'analyst'::regrole"); fmt.Sprint(got) != "[{work_mem=7MB}]" {
			t.Fatalf("settings per group: %v", got)
		}
		if got := s.everywhere(t, "select rolcanlogin::text from pg_authid where rolname = 'readers'"); fmt.Sprint(got) != "[false]" {
			t.Fatalf("CREATE ROLE defaults to NOLOGIN: %v", got)
		}
	})

	t.Run("drift_on_one_shard_is_repaired", func(t *testing.T) {
		s.awaitRolesMaterialized(t)
		shard1, err := pgx.Connect(ctx, s.shardDSNs[1])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := shard1.Exec(ctx, "alter role analyst password 'hijacked'"); err != nil {
			t.Fatal(err)
		}
		_ = shard1.Close(ctx)
		sawDrift := false
		deadline := time.Now().Add(30 * time.Second)
		for {
			st := s.roleStatus(t, "analyst")
			if strings.HasPrefix(st["default/1"], "drifted") && strings.Contains(st["default/1"], `"verifier": "differs"`) {
				sawDrift = true
			}
			if sawDrift && st["default/1"] == "in_sync {}" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("status never went drifted -> in_sync: %v\ncontroller log:\n%s", st, s.controllerLog.String())
			}
			time.Sleep(200 * time.Millisecond)
		}
		if verifiers := s.everywhere(t, "select rolpassword from pg_authid where rolname = 'analyst'"); len(verifiers) != 1 {
			t.Fatalf("verifiers per group after repair: %v", verifiers)
		}
		st := s.roleStatus(t, "app")
		if len(st) != 4 {
			t.Fatalf("status rows for app: %v", st)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		for _, c := range []struct{ sql, msg string }{
			{"create role boss superuser", "SUPERUSER"},
			{"alter role analyst replication", "REPLICATION"},
			{"drop owned by analyst", "DROP OWNED"},
			{"reassign owned by analyst to app", "REASSIGN OWNED"},
			{"alter default privileges grant select on tables to analyst", "ALTER DEFAULT PRIVILEGES"},
			{"alter role analyst rename to a2", "renaming a role"},
		} {
			_, err := conn.Exec(ctx, c.sql)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), c.msg) {
				t.Fatalf("%s: %v", c.sql, err)
			}
		}
	})

	t.Run("drop_role_everywhere", func(t *testing.T) {
		for _, sql := range []string{"revoke all on orders from analyst", "drop role analyst", "drop role readers"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v\ncontroller log:\n%s", sql, err, s.controllerLog.String())
			}
		}
		if got := s.everywhere(t, "select count(*)::text from pg_roles where rolname in ('analyst', 'readers')"); fmt.Sprint(got) != "[0]" {
			t.Fatalf("roles per group: %v", got)
		}
		awaitLogin(t, "analyst", "second", false)
	})
}
