package plan

import (
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// roleAttributes reads the attribute options of CREATE/ALTER ROLE and
// refuses the ones the cluster does not hand out (superuser, replication,
// bypassrls): they would give a client role rights on every shard's server.
func roleAttributes(options []*pgquerypb.Node) (*catalog.RoleAttributes, error) {
	a := &catalog.RoleAttributes{}
	set := false
	for _, o := range options {
		d := o.GetDefElem()
		b := func() *bool { v := d.GetArg().GetBoolean().GetBoolval(); return &v }
		switch strings.ToLower(d.GetDefname()) {
		case "superuser", "isreplication", "bypassrls":
			if d.GetArg().GetBoolean().GetBoolval() {
				word := map[string]string{"superuser": "SUPERUSER", "isreplication": "REPLICATION", "bypassrls": "BYPASSRLS"}[strings.ToLower(d.GetDefname())]
				return nil, notYet("roles with the "+word+" attribute are not available through the router",
					"the attribute would apply on every shard's server; manage such roles on the shards directly")
			}
		case "canlogin":
			a.Login, set = b(), true
		case "createdb":
			a.CreateDB, set = b(), true
		case "createrole":
			a.CreateRole, set = b(), true
		case "inherit":
			a.Inherit, set = b(), true
		case "connectionlimit":
			v := d.GetArg().GetInteger().GetIval()
			a.ConnectionLimit, set = &v, true
		case "validuntil":
			v := d.GetArg().GetString_().GetSval()
			if strings.EqualFold(v, "infinity") {
				v = ""
			}
			a.ValidUntil, set = &v, true
		}
	}
	if !set {
		return nil, nil
	}
	return a, nil
}

// createRoleMembers reads IN ROLE / ROLE / ADMIN of CREATE ROLE.
func createRoleMembers(role string, options []*pgquerypb.Node) []catalog.RoleMembership {
	var out []catalog.RoleMembership
	for _, o := range options {
		d := o.GetDefElem()
		var items []*pgquerypb.Node
		if l := d.GetArg().GetList(); l != nil {
			items = l.GetItems()
		}
		switch strings.ToLower(d.GetDefname()) {
		case "addroleto":
			for _, it := range items {
				out = append(out, catalog.RoleMembership{Role: it.GetRoleSpec().GetRolename(), Member: role})
			}
		case "rolemembers":
			for _, it := range items {
				out = append(out, catalog.RoleMembership{Role: role, Member: it.GetRoleSpec().GetRolename()})
			}
		case "adminmembers":
			for _, it := range items {
				out = append(out, catalog.RoleMembership{Role: role, Member: it.GetRoleSpec().GetRolename(), Admin: true})
			}
		}
	}
	return out
}

func (w *walker) grantRole(g *pgquerypb.GrantRoleStmt) error {
	admin := false
	for _, o := range g.GetOpt() {
		d := o.GetDefElem()
		if strings.EqualFold(d.GetDefname(), "admin") {
			// REVOKE ADMIN OPTION FOR carries the option with a false value.
			admin = !g.GetIsGrant() || d.GetArg().GetBoolean().GetBoolval()
		}
	}
	rc := &catalog.RoleChanges{}
	kind := "GRANT ROLE"
	if !g.GetIsGrant() {
		kind = "REVOKE ROLE"
	}
	for _, r := range g.GetGrantedRoles() {
		for _, m := range g.GetGranteeRoles() {
			ms := catalog.RoleMembership{Role: r.GetAccessPriv().GetPrivName(), Member: m.GetRoleSpec().GetRolename()}
			if g.GetIsGrant() {
				ms.Admin = admin
				rc.GrantMembers = append(rc.GrantMembers, ms)
			} else {
				ms.AdminOnly = admin
				rc.RevokeMembers = append(rc.RevokeMembers, ms)
			}
		}
	}
	return w.migration(Migration{Kind: kind, Scope: ScopeAll, Roles: rc})
}

// grantKind maps the object type of GRANT/REVOKE to the desired-state
// kind; "" means the grant is fanned out but not recorded.
func grantKind(t pgquerypb.ObjectType) string {
	switch t {
	case pgquerypb.ObjectType_OBJECT_TABLE:
		return "table"
	case pgquerypb.ObjectType_OBJECT_SEQUENCE:
		return "sequence"
	case pgquerypb.ObjectType_OBJECT_SCHEMA:
		return "schema"
	case pgquerypb.ObjectType_OBJECT_DATABASE:
		return "database"
	case pgquerypb.ObjectType_OBJECT_FUNCTION, pgquerypb.ObjectType_OBJECT_PROCEDURE, pgquerypb.ObjectType_OBJECT_ROUTINE:
		return "function"
	case pgquerypb.ObjectType_OBJECT_TYPE:
		return "type"
	case pgquerypb.ObjectType_OBJECT_DOMAIN:
		return "domain"
	case pgquerypb.ObjectType_OBJECT_LANGUAGE:
		return "language"
	}
	return ""
}

// grantChanges normalizes one GRANT/REVOKE into one entry per (object,
// column, grantee); PUBLIC and unknown object kinds are not recorded.
func grantChanges(g *pgquerypb.GrantStmt) []catalog.GrantChange {
	kind := grantKind(g.GetObjtype())
	if kind == "" || g.GetTargtype() != pgquerypb.GrantTargetType_ACL_TARGET_OBJECT {
		return nil
	}
	type obj struct{ schema, name string }
	var objs []obj
	for _, o := range g.GetObjects() {
		switch n := o.GetNode().(type) {
		case *pgquerypb.Node_RangeVar:
			objs = append(objs, obj{n.RangeVar.GetSchemaname(), n.RangeVar.GetRelname()})
		case *pgquerypb.Node_String_:
			objs = append(objs, obj{"", n.String_.GetSval()})
		case *pgquerypb.Node_ObjectWithArgs:
			var parts []string
			for _, p := range n.ObjectWithArgs.GetObjname() {
				parts = append(parts, p.GetString_().GetSval())
			}
			schema := ""
			if len(parts) > 1 {
				schema, parts = parts[0], parts[1:]
			}
			objs = append(objs, obj{schema, strings.Join(parts, ".")})
		case *pgquerypb.Node_List:
			var parts []string
			for _, p := range n.List.GetItems() {
				parts = append(parts, p.GetString_().GetSval())
			}
			schema := ""
			if len(parts) > 1 {
				schema, parts = parts[0], parts[1:]
			}
			objs = append(objs, obj{schema, strings.Join(parts, ".")})
		}
	}
	type priv struct {
		name string
		cols []string
	}
	privs := []priv{{name: "ALL"}}
	if len(g.GetPrivileges()) > 0 {
		privs = nil
		for _, p := range g.GetPrivileges() {
			ap := p.GetAccessPriv()
			pr := priv{name: ap.GetPrivName()}
			if pr.name == "" {
				pr.name = "ALL"
			}
			for _, c := range ap.GetCols() {
				pr.cols = append(pr.cols, c.GetString_().GetSval())
			}
			privs = append(privs, pr)
		}
	}
	var grantees []string
	for _, r := range g.GetGrantees() {
		rs := r.GetRoleSpec()
		if rs.GetRoletype() == pgquerypb.RoleSpecType_ROLESPEC_CSTRING {
			grantees = append(grantees, rs.GetRolename())
		}
	}
	index := map[string]int{}
	var out []catalog.GrantChange
	add := func(o obj, col, grantee, p string) {
		key := o.schema + "\x00" + o.name + "\x00" + col + "\x00" + grantee
		i, ok := index[key]
		if !ok {
			i = len(out)
			index[key] = i
			out = append(out, catalog.GrantChange{Kind: kind, Schema: o.schema, Name: o.name, Column: col, Grantee: grantee, GrantOption: g.GetGrantOption()})
		}
		out[i].Privileges = append(out[i].Privileges, p)
	}
	for _, o := range objs {
		for _, gr := range grantees {
			for _, p := range privs {
				if len(p.cols) == 0 {
					add(o, "", gr, p.name)
					continue
				}
				for _, c := range p.cols {
					add(o, c, gr, p.name)
				}
			}
		}
	}
	for i := range out {
		out[i].Privileges = catalog.NormalizePrivileges(kind, out[i].Column, out[i].Privileges)
	}
	return out
}

// alterRoleSet fans out ALTER ROLE ... SET/RESET and records the setting.
func (w *walker) alterRoleSet(s *pgquerypb.AlterRoleSetStmt) error {
	m := Migration{Kind: "ALTER ROLE", Scope: ScopeAll}
	set := s.GetSetstmt()
	// Validate the setting before any role-shape early return: CURRENT_USER,
	// SESSION_USER and ALL are not ROLESPEC_CSTRING but still persist the
	// value for a concrete role at apply time.
	if setsProtectedValue(set.GetKind()) {
		if err := refuseProtectedGUC(set.GetName()); err != nil {
			return err
		}
	}
	role := s.GetRole()
	if role == nil || role.GetRoletype() != pgquerypb.RoleSpecType_ROLESPEC_CSTRING {
		return w.migration(m)
	}
	rs := catalog.RoleSetting{Role: role.GetRolename(), Database: s.GetDatabase(), Name: set.GetName()}
	switch set.GetKind() {
	case pgquerypb.VariableSetKind_VAR_SET_VALUE:
		rs.Value = settingValue(set.GetArgs())
	case pgquerypb.VariableSetKind_VAR_SET_DEFAULT, pgquerypb.VariableSetKind_VAR_RESET:
		rs.Reset = true
	case pgquerypb.VariableSetKind_VAR_RESET_ALL:
		rs.ResetAll = true
	default:
		return notYet("ALTER ROLE ... SET ... FROM CURRENT is not available through the router",
			"give the value explicitly: ALTER ROLE ... SET name TO value")
	}
	m.Role, m.RoleOp = rs.Role, "set"
	m.Roles = &catalog.RoleChanges{Settings: []catalog.RoleSetting{rs}}
	return w.migration(m)
}

// settingValue renders SET arguments the way SHOW reports them.
func settingValue(args []*pgquerypb.Node) string {
	var parts []string
	for _, a := range args {
		switch n := a.GetNode().(type) {
		case *pgquerypb.Node_AConst:
			switch v := n.AConst.GetVal().(type) {
			case *pgquerypb.A_Const_Sval:
				parts = append(parts, v.Sval.GetSval())
			case *pgquerypb.A_Const_Ival:
				parts = append(parts, strconv.FormatInt(int64(v.Ival.GetIval()), 10))
			case *pgquerypb.A_Const_Fval:
				parts = append(parts, v.Fval.GetFval())
			case *pgquerypb.A_Const_Boolval:
				parts = append(parts, strconv.FormatBool(v.Boolval.GetBoolval()))
			default:
				parts = append(parts, "")
			}
		case *pgquerypb.Node_TypeCast:
			parts = append(parts, settingValue([]*pgquerypb.Node{n.TypeCast.GetArg()}))
		default:
			parts = append(parts, "")
		}
	}
	return strings.Join(parts, ", ")
}

func refuseOwned(what string) error {
	err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s is not available through the router: it acts on every object of a role on each shard and cannot be recorded as desired state", what)
	err.Hint = "GRANT/REVOKE and ALTER ... OWNER TO the objects explicitly"
	return err
}
