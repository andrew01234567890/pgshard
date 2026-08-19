package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

func TestSameVerifier(t *testing.T) {
	v, err := pgwire.BuildSCRAMVerifier("pw", nil, pgwire.DefaultSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := pgwire.BuildSCRAMVerifier("pw", nil, pgwire.DefaultSCRAMIterations)
	cases := []struct {
		a, b string
		want bool
	}{
		{v.String(), v.String(), true},
		{v.String(), other.String(), false},
		{v.String(), "", false},
		{"", "", true},
		{"md5abc", v.String(), false},
	}
	for _, c := range cases {
		if got := SameVerifier(c.a, c.b); got != c.want {
			t.Errorf("SameVerifier(%q, %q) = %v", c.a, c.b, got)
		}
	}
}

func TestRoleOptionsAndGrantSQL(t *testing.T) {
	until := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	r := catalog.DesiredRole{Name: "r", Verifier: "SCRAM-SHA-256$x", Login: true, CreateDB: true, Inherit: true, ConnectionLimit: 7, ValidUntil: &until}
	if got := RoleOptionsSQL(r); got != "LOGIN CREATEDB NOCREATEROLE INHERIT CONNECTION LIMIT 7 VALID UNTIL '2030-01-02T03:04:05Z' PASSWORD 'SCRAM-SHA-256$x'" {
		t.Fatalf("options %q", got)
	}
	if got := RoleOptionsSQL(catalog.DesiredRole{Name: "g"}); got != "NOLOGIN NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 0 VALID UNTIL 'infinity'" {
		t.Fatalf("bare options %q", got)
	}
	cases := []struct {
		g    catalog.DesiredGrant
		want string
	}{
		{catalog.DesiredGrant{Kind: "table", Schema: "public", Name: "orders", Grantee: "r", Privileges: []string{"SELECT", "UPDATE"}, GrantOption: true},
			`GRANT SELECT, UPDATE ON TABLE "public"."orders" TO "r" WITH GRANT OPTION`},
		{catalog.DesiredGrant{Kind: "table", Name: "items", Column: "id", Grantee: "r", Privileges: []string{"SELECT", "UPDATE"}},
			`GRANT SELECT ("id"), UPDATE ("id") ON TABLE "items" TO "r"`},
		{catalog.DesiredGrant{Kind: "schema", Name: "audit", Grantee: "r", Privileges: []string{"USAGE"}}, `GRANT USAGE ON SCHEMA "audit" TO "r"`},
	}
	for _, c := range cases {
		if got := GrantSQL(c.g); got != c.want {
			t.Errorf("GrantSQL = %q, want %q", got, c.want)
		}
	}
}

func TestDiffGroup(t *testing.T) {
	until := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	desired := &catalog.DesiredRoles{
		Roles: []catalog.DesiredRole{
			{Name: "app", Verifier: "SCRAM-SHA-256$4096:c2FsdA==$a2V5:a2V5", Login: true, Inherit: true, ConnectionLimit: -1},
			{Name: "readers", Inherit: true, ConnectionLimit: -1},
			{Name: "gone", Login: true, Inherit: true, ConnectionLimit: -1, ValidUntil: &until},
		},
		Members: []catalog.RoleMembership{{Role: "readers", Member: "app", Admin: true}},
		Grants: []catalog.DesiredGrant{
			{Grantee: "app", Database: "app", Kind: "table", Name: "orders", Privileges: []string{"SELECT", "UPDATE"}, GrantOption: true},
			{Grantee: "app", Database: "app", Kind: "schema", Name: "audit", Privileges: []string{"USAGE"}},
		},
	}
	obs := &observedGroup{
		Roles: map[string]observedRole{
			"app":      {Name: "app", Login: false, Inherit: true, ConnectionLimit: 5, Verifier: "SCRAM-SHA-256$4096:c2FsdA==$b3RoZXI=:a2V5", VerifierKnown: true},
			"readers":  {Name: "readers", Inherit: true, ConnectionLimit: -1, VerifierKnown: true},
			"other":    {Name: "other", Login: true, VerifierKnown: true},
			"postgres": {Name: "postgres", Super: true, VerifierKnown: true},
		},
		Members: map[membershipKey]bool{{"readers", "app"}: false, {"readers", "other"}: false},
		Grants: map[grantKey]map[string]bool{
			{"app", "table", "", "orders", "", "app"}: {"SELECT": true, "SELECT*": true, "UPDATE": true},
		},
	}
	rows := diffGroup("default/0", desired, obs, true)
	got := map[string]string{}
	for _, r := range rows {
		d, _ := json.Marshal(r.Details)
		got[r.Role] = r.State + " " + string(d)
	}
	want := map[string]string{
		"app":     `drifted {"attributes":["login","connection_limit"],"missing_grants":["UPDATE* ON table orders"],"missing_memberships":["readers"],"verifier":"differs"}`,
		"readers": `in_sync {}`,
		"gone":    `missing {}`,
		"other":   `unmanaged {"note":"not in pgshard.roles; left alone"}`,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows:\n%v\nwant:\n%v", got, want)
	}
	if len(rows) != 4 || rows[0].Role != "app" || rows[3].Role != "other" || rows[3].Group != "default/0" {
		t.Fatalf("order/group: %+v", rows)
	}

	obs.Roles["app"] = observedRole{Name: "app", Login: true, Inherit: true, ConnectionLimit: -1, Verifier: desired.Roles[0].Verifier, VerifierKnown: true}
	obs.Members[membershipKey{"readers", "app"}] = true
	obs.Grants[grantKey{"app", "table", "", "orders", "", "app"}]["UPDATE*"] = true
	rows = diffGroup("catalog", desired, obs, false)
	if rows[0].State != RoleInSync {
		t.Fatalf("repaired app: %+v", rows[0])
	}
	obs.Members[membershipKey{"app", "readers"}] = false
	rows = diffGroup("catalog", desired, obs, false)
	for _, r := range rows {
		if r.Role == "readers" && r.State != RoleInSync {
			t.Fatalf("memberships are additive; an extra one is not drift: %+v", r)
		}
	}
}

func TestSubsetRoles(t *testing.T) {
	d := &catalog.DesiredRoles{Generation: 9,
		Roles:    []catalog.DesiredRole{{Name: "a"}, {Name: "b"}},
		Members:  []catalog.RoleMembership{{Role: "a", Member: "b"}, {Role: "c", Member: "d"}},
		Grants:   []catalog.DesiredGrant{{Grantee: "a"}, {Grantee: "b"}},
		Settings: []catalog.RoleSetting{{Role: "b", Name: "x"}}}
	s := subsetRoles(d, []string{"a"})
	if s.Generation != 9 || len(s.Roles) != 1 || len(s.Members) != 1 || len(s.Grants) != 1 || len(s.Settings) != 0 {
		t.Fatalf("subset %+v", s)
	}
}

func TestApplierFansRoleStatementsToTheCatalogLast(t *testing.T) {
	f := newApplierFixture(t)
	cat := &fakeShards{ran: map[int32][]string{}, dbs: map[int32][]string{}}
	f.app.Catalog = func(context.Context) (ShardConn, error) { return &fakeConn{f: cat, id: 99}, nil }
	id := f.queue(catalog.DDLMigration{Statement: "create role r", Kind: "CREATE ROLE", Scope: "all",
		Meta: catalog.MigrationMeta{RunAs: "app", Role: "r", RoleOp: "create", Object: catalog.MigrationObject{Kind: "role", Name: "r", Expect: "present"}}})
	grant := f.queue(catalog.DDLMigration{Statement: "grant select on t to r", Kind: "GRANT", Scope: "all", Meta: catalog.MigrationMeta{RunAs: "app"}})
	f.run(t)
	m := f.store.get(t, id)
	if got := states(m); got != "0=applied/1 1=applied/1 2=applied/1 catalog=applied/1" || m.State != catalog.MigrationComplete {
		t.Fatalf("%s %s", m.State, got)
	}
	if got := strings.Join(cat.statements(99), ";"); strings.Contains(got, "SET ROLE") || !strings.Contains(got, "create role r") {
		t.Fatalf("catalog ran %q: no SET ROLE, the statement itself", got)
	}
	if g := f.store.get(t, grant); states(g) != "0=applied/1 1=applied/1 2=applied/1" {
		t.Fatalf("object grants stay on the shards: %s", states(g))
	}

	f.shards.exec = func(shard int32, _ string) error {
		if shard == 1 {
			return pgErr("42710", "role exists")
		}
		return nil
	}
	id = f.queue(catalog.DDLMigration{Statement: "create role r2", Kind: "CREATE ROLE", Scope: "all",
		Meta: catalog.MigrationMeta{Role: "r2", RoleOp: "create", Verifier: "v", Object: catalog.MigrationObject{Kind: "role", Name: "r2", Expect: "present"}}})
	before := len(f.store.execs)
	f.run(t)
	m = f.store.get(t, id)
	if got := states(m); got != "0=applied/1 1=failed/1 2=applied/1 catalog=failed/0" || m.State != catalog.MigrationFailed {
		t.Fatalf("%s %s", m.State, got)
	}
	if strings.Contains(strings.Join(cat.statements(99), ";"), "r2") {
		t.Fatal("the catalog group must not run a statement a shard failed")
	}
	if len(f.store.execs) != before {
		t.Fatalf("a failed migration must not change the desired verifier: %v", f.store.execs[before:])
	}
}

// roleGroupConn fakes a group for MaterializeRoles: roles maps an existing
// role to whether it is a superuser.
type roleGroupConn struct {
	roles map[string]bool
	ran   []string
}

func (c *roleGroupConn) Exec(_ context.Context, sql string, _ ...any) (pgconnTag, error) {
	c.ran = append(c.ran, sql)
	return pgconn.CommandTag{}, nil
}

func (c *roleGroupConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !strings.Contains(sql, "rolsuper") {
		return nil, fmt.Errorf("unexpected query %q", sql)
	}
	super, ok := c.roles[args[0].(string)]
	if !ok {
		return &boolRows{}, nil
	}
	return &boolRows{vals: []bool{super}}, nil
}

func (c *roleGroupConn) Close(context.Context) error { return nil }

func TestMaterializeRolesNeverAltersASuperuser(t *testing.T) {
	conn := &roleGroupConn{roles: map[string]bool{"postgres": true, "app": false}}
	d := &catalog.DesiredRoles{
		Roles:    []catalog.DesiredRole{{Name: "postgres", Login: true}, {Name: "app", Login: true}, {Name: "fresh"}},
		Members:  []catalog.RoleMembership{{Role: "app", Member: "postgres"}, {Role: "app", Member: "fresh"}},
		Settings: []catalog.RoleSetting{{Role: "postgres", Name: "work_mem", Value: "1MB"}, {Role: "app", Name: "work_mem", Value: "2MB"}},
	}
	err := MaterializeRoles(context.Background(), func(context.Context, string) (ShardConn, error) { return conn, nil }, d, false)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(conn.ran, ";")
	if strings.Contains(got, `"postgres"`) {
		t.Fatalf("a superuser was touched: %q", got)
	}
	want := `ALTER ROLE "app" LOGIN NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 0 VALID UNTIL 'infinity';` +
		`CREATE ROLE "fresh" NOLOGIN NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 0 VALID UNTIL 'infinity' NOSUPERUSER NOREPLICATION NOBYPASSRLS;` +
		`GRANT "app" TO "fresh";ALTER ROLE "app" SET "work_mem" TO '2MB'`
	if got != want {
		t.Fatalf("ran %q\nwant %q", got, want)
	}
	st := diffGroup("g", d, &observedGroup{Roles: map[string]observedRole{"postgres": {Name: "postgres", Super: true, Login: true}}, Members: map[membershipKey]bool{}}, false)
	if st[0].State != RoleUnmanagedSuperuser || st[0].Details["superuser"] != true {
		t.Fatalf("superuser status %+v", st[0])
	}
}
