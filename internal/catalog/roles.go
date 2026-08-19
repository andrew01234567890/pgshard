package catalog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// RoleAttributes are the role attributes a CREATE/ALTER ROLE sets; nil
// fields are left as they are.
type RoleAttributes struct {
	Login           *bool  `json:"login,omitempty"`
	CreateDB        *bool  `json:"createdb,omitempty"`
	CreateRole      *bool  `json:"createrole,omitempty"`
	Inherit         *bool  `json:"inherit,omitempty"`
	ConnectionLimit *int32 `json:"connection_limit,omitempty"`
	// ValidUntil is a timestamp literal; "" clears the expiry.
	ValidUntil *string `json:"valid_until,omitempty"`
}

// RoleMembership is one row of pgshard.role_members.
type RoleMembership struct {
	Role   string `json:"role"`
	Member string `json:"member"`
	Admin  bool   `json:"admin,omitempty"`
	// AdminOnly marks REVOKE ADMIN OPTION FOR: the membership stays.
	AdminOnly bool `json:"admin_only,omitempty"`
}

// GrantChange is one normalized GRANT or REVOKE on an object.
type GrantChange struct {
	Kind       string   `json:"kind"`
	Schema     string   `json:"schema,omitempty"`
	Name       string   `json:"name"`
	Column     string   `json:"column,omitempty"`
	Grantee    string   `json:"grantee"`
	Privileges []string `json:"privileges"`
	// GrantOption is WITH GRANT OPTION on a grant and GRANT OPTION FOR on
	// a revoke (the privileges themselves stay).
	GrantOption bool `json:"grant_option,omitempty"`
}

// RoleSetting is one row of pgshard.role_settings; Reset removes it.
type RoleSetting struct {
	Role     string `json:"role"`
	Database string `json:"database,omitempty"`
	Name     string `json:"name,omitempty"`
	Value    string `json:"value,omitempty"`
	Reset    bool   `json:"reset,omitempty"`
	ResetAll bool   `json:"reset_all,omitempty"`
}

// RoleChanges is the desired-state delta a role, grant or membership
// statement implies; the applier writes it into the catalog once the
// statement is applied everywhere.
type RoleChanges struct {
	Attributes    *RoleAttributes  `json:"attributes,omitempty"`
	DropRoles     []string         `json:"drop_roles,omitempty"`
	GrantMembers  []RoleMembership `json:"grant_members,omitempty"`
	RevokeMembers []RoleMembership `json:"revoke_members,omitempty"`
	Grants        []GrantChange    `json:"grants,omitempty"`
	Revokes       []GrantChange    `json:"revokes,omitempty"`
	Settings      []RoleSetting    `json:"settings,omitempty"`
}

// Statement is one parameterized catalog statement.
type Statement struct {
	SQL  string
	Args []any
}

// AllPrivileges expands ALL PRIVILEGES per object kind so stored grants are
// explicit and a later REVOKE of one privilege can be subtracted.
var AllPrivileges = map[string][]string{
	"table":    {"DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"},
	"column":   {"INSERT", "REFERENCES", "SELECT", "UPDATE"},
	"sequence": {"SELECT", "UPDATE", "USAGE"},
	"schema":   {"CREATE", "USAGE"},
	"database": {"CONNECT", "CREATE", "TEMPORARY"},
	"function": {"EXECUTE"},
	"type":     {"USAGE"},
	"domain":   {"USAGE"},
	"language": {"USAGE"},
}

// NormalizePrivileges upper-cases, expands ALL for the kind (column grants
// use the column set) and sorts without duplicates.
func NormalizePrivileges(kind, column string, privs []string) []string {
	set := map[string]bool{}
	for _, p := range privs {
		p = strings.ToUpper(strings.TrimSpace(p))
		if p == "TEMP" {
			p = "TEMPORARY"
		}
		if p == "ALL" || p == "ALL PRIVILEGES" || p == "" {
			k := kind
			if column != "" {
				k = "column"
			}
			all, ok := AllPrivileges[k]
			if !ok {
				set["ALL"] = true
				continue
			}
			for _, a := range all {
				set[a] = true
			}
			continue
		}
		set[p] = true
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// RoleMirrorStatements returns the catalog statements that record meta of a
// completed migration in the desired-state tables.
func RoleMirrorStatements(database string, meta MigrationMeta) []Statement {
	var out []Statement
	rc := meta.Roles
	switch meta.RoleOp {
	case "create", "alter":
		if meta.Role != "" && (meta.RoleOp == "create" || meta.Verifier != "" || rc != nil && rc.Attributes != nil) {
			out = append(out, upsertRole(meta.Role, meta.Verifier, attrsOf(rc)))
		}
	case "drop":
		names := []string{}
		if meta.Role != "" {
			names = append(names, meta.Role)
		}
		if rc != nil {
			names = append(names, rc.DropRoles...)
		}
		for _, n := range dedupe(names) {
			out = append(out, Statement{`DELETE FROM pgshard.roles WHERE rolname = $1`, []any{n}})
		}
	}
	if rc == nil {
		return out
	}
	for _, m := range rc.GrantMembers {
		out = append(out, Statement{`INSERT INTO pgshard.role_members (rolname, member, admin_option)
			SELECT $1, $2, $3 WHERE EXISTS (SELECT 1 FROM pgshard.roles WHERE rolname = $1) AND EXISTS (SELECT 1 FROM pgshard.roles WHERE rolname = $2)
			ON CONFLICT (rolname, member) DO UPDATE SET admin_option = pgshard.role_members.admin_option OR EXCLUDED.admin_option`,
			[]any{m.Role, m.Member, m.Admin}})
	}
	for _, m := range rc.RevokeMembers {
		if m.AdminOnly {
			out = append(out, Statement{`UPDATE pgshard.role_members SET admin_option = false WHERE rolname = $1 AND member = $2`, []any{m.Role, m.Member}})
		} else {
			out = append(out, Statement{`DELETE FROM pgshard.role_members WHERE rolname = $1 AND member = $2`, []any{m.Role, m.Member}})
		}
	}
	for _, g := range rc.Grants {
		privs := NormalizePrivileges(g.Kind, g.Column, g.Privileges)
		out = append(out, Statement{`INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, column_name, privileges, grant_option)
			SELECT $1, $2, $3, $4, $5, $6, $7::text[], $8 WHERE EXISTS (SELECT 1 FROM pgshard.roles WHERE rolname = $1) AND EXISTS (SELECT 1 FROM pgshard.databases WHERE name = $2)
			ON CONFLICT (rolname, database, object_kind, object_schema, object_name, column_name) DO UPDATE SET
			privileges = (SELECT array_agg(DISTINCT p ORDER BY p) FROM unnest(pgshard.grants.privileges || EXCLUDED.privileges) p),
			grant_option = pgshard.grants.grant_option OR EXCLUDED.grant_option`,
			[]any{g.Grantee, database, g.Kind, g.Schema, g.Name, g.Column, privs, g.GrantOption}})
	}
	for _, g := range rc.Revokes {
		privs := NormalizePrivileges(g.Kind, g.Column, g.Privileges)
		where := ` WHERE rolname = $1 AND database = $2 AND object_kind = $3 AND object_schema = $4 AND object_name = $5 AND ($6 = '' OR column_name = $6)`
		args := []any{g.Grantee, database, g.Kind, g.Schema, g.Name, g.Column}
		if g.GrantOption {
			out = append(out, Statement{`UPDATE pgshard.grants SET grant_option = false` + where, args})
			continue
		}
		out = append(out, Statement{`UPDATE pgshard.grants SET privileges = (SELECT coalesce(array_agg(p ORDER BY p), '{}') FROM unnest(privileges) p WHERE p <> ALL ($7::text[]))` + where, append(args, privs)})
		out = append(out, Statement{`DELETE FROM pgshard.grants` + where + ` AND privileges = '{}'`, args})
	}
	for _, s := range rc.Settings {
		switch {
		case s.ResetAll:
			out = append(out, Statement{`DELETE FROM pgshard.role_settings WHERE rolname = $1 AND database = $2`, []any{s.Role, s.Database}})
		case s.Reset:
			out = append(out, Statement{`DELETE FROM pgshard.role_settings WHERE rolname = $1 AND database = $2 AND name = $3`, []any{s.Role, s.Database, s.Name}})
		default:
			out = append(out, Statement{`INSERT INTO pgshard.role_settings (rolname, database, name, value)
				SELECT $1, $2, $3, $4 WHERE EXISTS (SELECT 1 FROM pgshard.roles WHERE rolname = $1)
				ON CONFLICT (rolname, database, name) DO UPDATE SET value = EXCLUDED.value`, []any{s.Role, s.Database, s.Name, s.Value}})
		}
	}
	return out
}

func attrsOf(rc *RoleChanges) RoleAttributes {
	if rc == nil || rc.Attributes == nil {
		return RoleAttributes{}
	}
	return *rc.Attributes
}

// upsertRole writes verifier and the attributes that are set; unset ones
// keep their current (or default) value.
func upsertRole(name, verifier string, a RoleAttributes) Statement {
	var validUntil any
	if a.ValidUntil != nil {
		if *a.ValidUntil == "" {
			validUntil = ""
		} else {
			validUntil = *a.ValidUntil
		}
	}
	return Statement{`INSERT INTO pgshard.roles (rolname, verifier, login, createdb, createrole, inherit, connection_limit, valid_until)
		VALUES ($1, nullif($2, ''), coalesce($3, true), coalesce($4, false), coalesce($5, false), coalesce($6, true), coalesce($7, -1),
		        CASE WHEN $8::text IS NULL OR $8 = '' THEN NULL ELSE $8::timestamptz END)
		ON CONFLICT (rolname) DO UPDATE SET
		verifier = coalesce(nullif(EXCLUDED.verifier, ''), pgshard.roles.verifier),
		login = coalesce($3, pgshard.roles.login), createdb = coalesce($4, pgshard.roles.createdb),
		createrole = coalesce($5, pgshard.roles.createrole), inherit = coalesce($6, pgshard.roles.inherit),
		connection_limit = coalesce($7, pgshard.roles.connection_limit),
		valid_until = CASE WHEN $8::text IS NULL THEN pgshard.roles.valid_until WHEN $8 = '' THEN NULL ELSE $8::timestamptz END,
		updated_at = now()`,
		[]any{name, verifier, a.Login, a.CreateDB, a.CreateRole, a.Inherit, a.ConnectionLimit, validUntil}}
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// DesiredRole is a row of pgshard.roles.
type DesiredRole struct {
	Name            string
	Verifier        string
	Login           bool
	CreateDB        bool
	CreateRole      bool
	Inherit         bool
	ConnectionLimit int32
	ValidUntil      *time.Time
}

// DesiredGrant is a row of pgshard.grants.
type DesiredGrant struct {
	Grantee     string
	Database    string
	Kind        string
	Schema      string
	Name        string
	Column      string
	Privileges  []string
	GrantOption bool
}

// DesiredRoles is the whole role desired state at one generation.
type DesiredRoles struct {
	Roles      []DesiredRole
	Members    []RoleMembership
	Grants     []DesiredGrant
	Settings   []RoleSetting
	Generation int64
}

// Role finds a desired role by name.
func (d *DesiredRoles) Role(name string) (DesiredRole, bool) {
	for _, r := range d.Roles {
		if r.Name == name {
			return r, true
		}
	}
	return DesiredRole{}, false
}

// LoadDesiredRoles reads roles, memberships, grants and settings with the
// generation the newest of them carries.
func LoadDesiredRoles(ctx context.Context, q Querier) (*DesiredRoles, error) {
	d := &DesiredRoles{}
	rows, err := q.Query(ctx, `SELECT rolname, coalesce(verifier, ''), login, createdb, createrole, inherit, connection_limit, valid_until FROM pgshard.roles ORDER BY rolname`)
	if err != nil {
		return nil, fmt.Errorf("catalog: roles: %w", err)
	}
	d.Roles, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (DesiredRole, error) {
		var r DesiredRole
		err := row.Scan(&r.Name, &r.Verifier, &r.Login, &r.CreateDB, &r.CreateRole, &r.Inherit, &r.ConnectionLimit, &r.ValidUntil)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: roles: %w", err)
	}
	rows, err = q.Query(ctx, `SELECT rolname, member, admin_option FROM pgshard.role_members ORDER BY rolname, member`)
	if err != nil {
		return nil, fmt.Errorf("catalog: role_members: %w", err)
	}
	d.Members, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (RoleMembership, error) {
		var m RoleMembership
		err := row.Scan(&m.Role, &m.Member, &m.Admin)
		return m, err
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: role_members: %w", err)
	}
	rows, err = q.Query(ctx, `SELECT rolname, database, object_kind, object_schema, object_name, column_name, privileges, grant_option FROM pgshard.grants ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: grants: %w", err)
	}
	d.Grants, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (DesiredGrant, error) {
		var g DesiredGrant
		err := row.Scan(&g.Grantee, &g.Database, &g.Kind, &g.Schema, &g.Name, &g.Column, &g.Privileges, &g.GrantOption)
		return g, err
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: grants: %w", err)
	}
	rows, err = q.Query(ctx, `SELECT rolname, database, name, value FROM pgshard.role_settings ORDER BY rolname, database, name`)
	if err != nil {
		return nil, fmt.Errorf("catalog: role_settings: %w", err)
	}
	d.Settings, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (RoleSetting, error) {
		var s RoleSetting
		err := row.Scan(&s.Role, &s.Database, &s.Name, &s.Value)
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: role_settings: %w", err)
	}
	rows, err = q.Query(ctx, `SELECT coalesce(max(g), 0) FROM (
		SELECT max(desired_generation) g FROM pgshard.roles
		UNION ALL SELECT max(desired_generation) FROM pgshard.role_members
		UNION ALL SELECT max(desired_generation) FROM pgshard.grants
		UNION ALL SELECT max(desired_generation) FROM pgshard.role_settings) m`)
	if err != nil {
		return nil, fmt.Errorf("catalog: roles generation: %w", err)
	}
	d.Generation, err = pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("catalog: roles generation: %w", err)
	}
	return d, nil
}
