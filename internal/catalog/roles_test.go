package catalog

import (
	"fmt"
	"strings"
	"testing"
)

func TestNormalizePrivileges(t *testing.T) {
	cases := []struct {
		kind, column string
		in           []string
		want         string
	}{
		{"table", "", []string{"select", "SELECT", "insert"}, "INSERT SELECT"},
		{"table", "", []string{"all"}, "DELETE INSERT MAINTAIN REFERENCES SELECT TRIGGER TRUNCATE UPDATE"},
		{"table", "id", []string{"ALL"}, "INSERT REFERENCES SELECT UPDATE"},
		{"database", "", []string{"temp", "connect"}, "CONNECT TEMPORARY"},
		{"foreign server", "", []string{"ALL"}, "ALL"},
		{"schema", "", nil, ""},
	}
	for _, c := range cases {
		if got := strings.Join(NormalizePrivileges(c.kind, c.column, c.in), " "); got != c.want {
			t.Errorf("%s %q %v = %q, want %q", c.kind, c.column, c.in, got, c.want)
		}
	}
}

func render(stmts []Statement) string {
	var out []string
	for _, s := range stmts {
		first := strings.TrimSpace(strings.SplitN(s.SQL, "\n", 2)[0])
		out = append(out, fmt.Sprintf("%s %v", first, s.Args))
	}
	return strings.Join(out, "\n")
}

func TestRoleMirrorStatements(t *testing.T) {
	yes, no := true, false
	limit := int32(3)
	until := ""
	cases := []struct {
		name string
		meta MigrationMeta
		want []string
	}{
		{"create with attributes", MigrationMeta{Role: "r", RoleOp: "create", Verifier: "v", Roles: &RoleChanges{Attributes: &RoleAttributes{Login: &yes, ConnectionLimit: &limit, ValidUntil: &until}}},
			[]string{"INSERT INTO pgshard.roles (rolname, verifier, login, createdb, createrole, inherit, connection_limit, valid_until) [r v 0x"}},
		{"alter without password or attributes touches nothing", MigrationMeta{Role: "r", RoleOp: "alter"}, nil},
		{"alter attributes only", MigrationMeta{Role: "r", RoleOp: "alter", Roles: &RoleChanges{Attributes: &RoleAttributes{CreateDB: &no}}},
			[]string{"INSERT INTO pgshard.roles (rolname, verifier, login, createdb"}},
		{"drop several", MigrationMeta{RoleOp: "drop", Roles: &RoleChanges{DropRoles: []string{"a", "b"}}},
			[]string{"DELETE FROM pgshard.roles WHERE rolname = $1 [a]", "DELETE FROM pgshard.roles WHERE rolname = $1 [b]"}},
		{"drop one names it once", MigrationMeta{Role: "a", RoleOp: "drop", Roles: &RoleChanges{DropRoles: []string{"a"}}},
			[]string{"DELETE FROM pgshard.roles WHERE rolname = $1 [a]"}},
		{"memberships", MigrationMeta{Roles: &RoleChanges{GrantMembers: []RoleMembership{{Role: "g", Member: "m", Admin: true}},
			RevokeMembers: []RoleMembership{{Role: "g", Member: "x"}, {Role: "g", Member: "y", AdminOnly: true}}}},
			[]string{"INSERT INTO pgshard.role_members (rolname, member, admin_option) [g m true]",
				"DELETE FROM pgshard.role_members WHERE rolname = $1 AND member = $2 [g x]",
				"UPDATE pgshard.role_members SET admin_option = false WHERE rolname = $1 AND member = $2 [g y]"}},
		{"grant expands all", MigrationMeta{Roles: &RoleChanges{Grants: []GrantChange{{Kind: "sequence", Name: "s", Grantee: "r", Privileges: []string{"ALL"}, GrantOption: true}}}},
			[]string{"INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, column_name, privileges, grant_option) [r app sequence  s  [SELECT UPDATE USAGE] true]"}},
		{"revoke subtracts then drops empty rows", MigrationMeta{Roles: &RoleChanges{Revokes: []GrantChange{{Kind: "table", Schema: "public", Name: "t", Grantee: "r", Privileges: []string{"select"}}}}},
			[]string{"UPDATE pgshard.grants SET privileges = (SELECT coalesce(array_agg(p ORDER BY p), '{}') FROM unnest(privileges) p WHERE p <> ALL ($7::text[])) WHERE rolname = $1 AND database = $2 AND object_kind = $3 AND object_schema = $4 AND object_name = $5 AND ($6 = '' OR column_name = $6) [r app table public t  [SELECT]]",
				"DELETE FROM pgshard.grants WHERE rolname = $1 AND database = $2 AND object_kind = $3 AND object_schema = $4 AND object_name = $5 AND ($6 = '' OR column_name = $6) AND privileges = '{}' [r app table public t ]"}},
		{"revoke grant option keeps privileges", MigrationMeta{Roles: &RoleChanges{Revokes: []GrantChange{{Kind: "table", Name: "t", Grantee: "r", Privileges: []string{"SELECT"}, GrantOption: true}}}},
			[]string{"UPDATE pgshard.grants SET grant_option = false WHERE rolname = $1"}},
		{"settings", MigrationMeta{Roles: &RoleChanges{Settings: []RoleSetting{{Role: "r", Name: "work_mem", Value: "64MB"}, {Role: "r", Database: "app", Name: "work_mem", Reset: true}, {Role: "r", ResetAll: true}}}},
			[]string{"INSERT INTO pgshard.role_settings (rolname, database, name, value) [r  work_mem 64MB]",
				"DELETE FROM pgshard.role_settings WHERE rolname = $1 AND database = $2 AND name = $3 [r app work_mem]",
				"DELETE FROM pgshard.role_settings WHERE rolname = $1 AND database = $2 [r ]"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := RoleMirrorStatements("app", c.meta)
			if len(got) != len(c.want) {
				t.Fatalf("%d statements, want %d:\n%s", len(got), len(c.want), render(got))
			}
			for i, w := range c.want {
				if !strings.Contains(render(got[i:i+1]), w) {
					t.Errorf("statement %d:\n%s\nwant %s", i, render(got[i:i+1]), w)
				}
			}
		})
	}
}
