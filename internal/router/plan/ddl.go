package plan

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Migration scopes: which shards of the database a DDL statement targets.
const (
	// ScopeAll targets every shard of the set.
	ScopeAll = "all"
	// ScopeHome targets the database's home shard only.
	ScopeHome = "home"
	// ScopeExisting targets every shard but tolerates shards where the
	// object does not exist (indexes and views, whose owning table the
	// router cannot resolve).
	ScopeExisting = "existing"
)

// Migration strategies.
const (
	StrategyDirect     = "direct"
	StrategyConcurrent = "concurrent"
	// StrategyMultistep runs Migration.Steps per shard, each under its
	// own lock_timeout retry, so no step takes a long strong lock.
	StrategyMultistep = "multistep"
)

// Migration is a DDL/DCL statement the router hands to the migration queue
// instead of running it on a shard.
type Migration struct {
	// Kind names the statement ("CREATE TABLE", "GRANT", ...); it is also
	// the command tag reported to the client.
	Kind string
	// Statement is the SQL every target shard runs; it differs from the
	// client's text when a role password was replaced by its verifier.
	Statement string
	Strategy  string
	Scope     string
	// Object is the created or dropped object the applier checks for when
	// it resumes a shard step after a crash; empty when there is none.
	Object ObjectRef
	// Role and Verifier are set for CREATE/ALTER/DROP ROLE so the applier
	// mirrors the desired row in pgshard.roles.
	Role     string
	Verifier string
	// RoleOp is "create", "alter", "set" or "drop" when Role is set.
	RoleOp string
	// Roles is the desired-state delta of a role, membership, grant or
	// setting statement.
	Roles *catalog.RoleChanges
	// Database and DatabaseOp mirror CREATE/DROP DATABASE into
	// pgshard.databases.
	Database   string
	DatabaseOp string
	// Steps is the plan of a StrategyMultistep migration.
	Steps []Step
	// Rewrite is the plan of a StrategyRewrite migration.
	Rewrite *catalog.RewriteChange
}

// Step is one statement of a multistep migration.
type Step struct {
	SQL        string
	Concurrent bool
	Skip       Check
	Index      string
	OnFail     string
}

// Check is the shard-side predicate under which a Step is already done.
type Check struct {
	Kind   string
	Schema string
	Table  string
	Name   string
	// NameSchema qualifies Name when it is a partition with an explicit
	// schema; empty resolves Name through the search_path, as the
	// statement itself does.
	NameSchema string
}

// ObjectRef names the object a migration creates or drops.
type ObjectRef struct {
	// Kind is "relation", "schema", "type", "role" or "database".
	Kind   string
	Schema string
	Name   string
	// Expect is "present" after a CREATE and "absent" after a DROP.
	Expect string
}

const (
	objectPresent = "present"
	objectAbsent  = "absent"
)

// migration finishes the plan as a Migration.
func (w *walker) migration(m Migration) error {
	if m.Strategy == "" {
		m.Strategy = StrategyDirect
	}
	if m.Statement == "" {
		m.Statement = w.sql
	}
	w.plan.Kind, w.plan.Shards, w.plan.Migration = MigrationKind, nil, &m
	return nil
}

// relScope maps the placement of the relations a statement names to a
// scope, refusing a statement that mixes sharded and unsharded tables.
func (w *walker) relScope(rels []*rel) (string, error) {
	sharded, unsharded := false, false
	for _, r := range rels {
		if r == nil {
			continue
		}
		if r.kind == placeUnsharded {
			unsharded = true
		} else {
			sharded = true
		}
	}
	switch {
	case sharded && unsharded:
		return "", notYet("one DDL statement cannot touch both sharded and unsharded tables", "split it into one statement per table")
	case sharded:
		return ScopeAll, nil
	}
	return ScopeHome, nil
}

func (w *walker) lookupList(rvs []*pgquerypb.RangeVar) ([]*rel, error) {
	rels := make([]*rel, 0, len(rvs))
	for _, rv := range rvs {
		r, err := w.lookup(rv)
		if err != nil {
			return nil, err
		}
		if r != nil {
			rels = append(rels, r)
		}
	}
	return rels, nil
}

func relationRef(rv *pgquerypb.RangeVar, expect string) ObjectRef {
	return ObjectRef{Kind: "relation", Schema: rv.GetSchemaname(), Name: rv.GetRelname(), Expect: expect}
}

// createTable enforces the sharded-table constraints and queues the
// migration.
func (w *walker) createTable(c *pgquerypb.CreateStmt) error {
	r, err := w.lookup(c.GetRelation())
	if err != nil {
		return err
	}
	if r != nil && r.kind == placeSharded {
		if err := checkCreateSharded(r, c); err != nil {
			return err
		}
	}
	scope, err := w.relScope([]*rel{r})
	if err != nil {
		return err
	}
	return w.migration(Migration{Kind: "CREATE TABLE", Scope: scope, Object: relationRef(c.GetRelation(), objectPresent)})
}

// blankPaddedType reports whether a type name is a blank-padded character
// type, whose equality ignores trailing spaces and so cannot key routing.
func blankPaddedType(name string) bool {
	switch strings.ToLower(name) {
	case "bpchar", "char", "character":
		return true
	}
	return false
}

func checkCreateSharded(r *rel, c *pgquerypb.CreateStmt) error {
	haveKey := false
	for _, elt := range c.GetTableElts() {
		switch e := elt.GetNode().(type) {
		case *pgquerypb.Node_ColumnDef:
			if e.ColumnDef.GetColname() == r.shardKey {
				haveKey = true
				if names := stringList(e.ColumnDef.GetTypeName().GetNames()); len(names) > 0 && blankPaddedType(names[len(names)-1]) {
					return notYet("shard key column \""+r.shardKey+"\" of sharded table \""+r.name+"\" cannot be a blank-padded character type",
						"character(n)/bpchar equality ignores trailing spaces, which does not match byte-wise routing; use text or varchar")
				}
			}
			for _, con := range e.ColumnDef.GetConstraints() {
				if isUniqueConstraint(con.GetConstraint()) && e.ColumnDef.GetColname() != r.shardKey {
					return keyConstraintError(r, e.ColumnDef.GetColname())
				}
			}
		case *pgquerypb.Node_Constraint:
			if isUniqueConstraint(e.Constraint) && !contains(stringList(e.Constraint.GetKeys()), r.shardKey) {
				return keyConstraintError(r, strings.Join(stringList(e.Constraint.GetKeys()), ", "))
			}
		}
	}
	if !haveKey {
		return notYet("sharded table \""+r.name+"\" must define its shard key column \""+r.shardKey+"\"",
			"add the column, or change the shard key in pgshard.tables")
	}
	return nil
}

func (w *walker) createIndex(s *pgquerypb.IndexStmt) error {
	r, err := w.lookup(s.GetRelation())
	if err != nil {
		return err
	}
	if r != nil && r.kind == placeSharded && s.GetUnique() {
		var cols []string
		for _, p := range s.GetIndexParams() {
			cols = append(cols, p.GetIndexElem().GetName())
		}
		if !contains(cols, r.shardKey) {
			return keyConstraintError(r, strings.Join(cols, ", "))
		}
	}
	scope, err := w.relScope([]*rel{r})
	if err != nil {
		return err
	}
	m := Migration{Kind: "CREATE INDEX", Scope: scope}
	if s.GetConcurrent() {
		m.Strategy = StrategyConcurrent
	}
	if s.GetIdxname() != "" {
		m.Object = ObjectRef{Kind: "relation", Schema: s.GetRelation().GetSchemaname(), Name: s.GetIdxname(), Expect: objectPresent}
	}
	return w.migration(m)
}

func (w *walker) alterTable(a *pgquerypb.AlterTableStmt) error {
	switch a.GetObjtype() {
	case pgquerypb.ObjectType_OBJECT_TABLE:
	case pgquerypb.ObjectType_OBJECT_INDEX:
		return w.migration(Migration{Kind: "ALTER INDEX", Scope: ScopeExisting})
	case pgquerypb.ObjectType_OBJECT_VIEW:
		return w.migration(Migration{Kind: "ALTER VIEW", Scope: ScopeExisting})
	case pgquerypb.ObjectType_OBJECT_SEQUENCE:
		return w.migration(Migration{Kind: "ALTER SEQUENCE", Scope: ScopeAll})
	case pgquerypb.ObjectType_OBJECT_TYPE:
		return w.migration(Migration{Kind: "ALTER TYPE", Scope: ScopeAll})
	default:
		return w.unshardedOnly()
	}
	r, err := w.lookup(a.GetRelation())
	if err != nil {
		return err
	}
	for _, n := range a.GetCmds() {
		if err := checkAlterCmd(r, n.GetAlterTableCmd()); err != nil {
			return err
		}
	}
	scope, err := w.relScope([]*rel{r})
	if err != nil {
		return err
	}
	rw, err := w.rewriteChange(a, r)
	if err != nil {
		return err
	}
	if rw != nil {
		return w.migration(Migration{Kind: "ALTER TABLE", Scope: scope, Strategy: StrategyRewrite, Rewrite: rw})
	}
	steps, err := w.alterSteps(a, r)
	if err != nil {
		return err
	}
	if steps != nil {
		return w.migration(Migration{Kind: "ALTER TABLE", Scope: scope, Strategy: StrategyMultistep, Steps: steps})
	}
	return w.migration(Migration{Kind: "ALTER TABLE", Scope: scope})
}

// checkAlterCmd refuses the rewrite class of ALTER TABLE and any change to
// the shard key column of a sharded table.
func checkAlterCmd(r *rel, c *pgquerypb.AlterTableCmd) error {
	sharded := r != nil && r.kind == placeSharded
	switch c.GetSubtype() {
	case pgquerypb.AlterTableType_AT_AlterColumnType:
		if sharded && c.GetName() == r.shardKey {
			return shardKeyChangeError(r)
		}
	case pgquerypb.AlterTableType_AT_SetUnLogged:
		return notDurable("ALTER TABLE ... SET UNLOGGED")
	case pgquerypb.AlterTableType_AT_SetLogged:
		return rewriteClass("SET LOGGED")
	case pgquerypb.AlterTableType_AT_SetTableSpace:
		return rewriteClass("SET TABLESPACE")
	case pgquerypb.AlterTableType_AT_AddColumn:
		col := c.GetDef().GetColumnDef()
		if names := stringList(col.GetTypeName().GetNames()); len(names) > 0 && serialType(names[len(names)-1]) {
			return rewriteClass("ADD COLUMN of a serial type")
		}
		for _, con := range col.GetConstraints() {
			cs := con.GetConstraint()
			switch cs.GetContype() {
			case pgquerypb.ConstrType_CONSTR_IDENTITY:
				return rewriteClass("ADD COLUMN ... GENERATED AS IDENTITY")
			case pgquerypb.ConstrType_CONSTR_GENERATED:
				if cs.GetGeneratedKind() != "v" {
					return rewriteClass("ADD COLUMN ... GENERATED ... STORED")
				}
			}
			if sharded && isUniqueConstraint(cs) && col.GetColname() != r.shardKey {
				return keyConstraintError(r, col.GetColname())
			}
		}
	case pgquerypb.AlterTableType_AT_DropColumn:
		if sharded && c.GetName() == r.shardKey {
			return shardKeyChangeError(r)
		}
	case pgquerypb.AlterTableType_AT_AddConstraint, pgquerypb.AlterTableType_AT_AddIndex:
		cs := c.GetDef().GetConstraint()
		if sharded && isUniqueConstraint(cs) && cs.GetIndexname() == "" && !contains(stringList(cs.GetKeys()), r.shardKey) {
			return keyConstraintError(r, strings.Join(stringList(cs.GetKeys()), ", "))
		}
	}
	return nil
}

func serialType(name string) bool {
	switch name {
	case "serial", "serial2", "serial4", "serial8", "smallserial", "bigserial":
		return true
	}
	return false
}

// stableDefaults are the functions whose DEFAULT PostgreSQL evaluates once
// at ALTER time and stores as a metadata-only default (no rewrite).
var stableDefaults = map[string]bool{"now": true, "transaction_timestamp": true, "statement_timestamp": true,
	"current_timestamp": true, "localtimestamp": true, "current_date": true, "current_time": true, "localtime": true,
	"current_user": true, "session_user": true, "current_schema": true, "current_database": true}

// stableDefault reports whether a DEFAULT expression is a literal (possibly
// cast) or a stable timestamp/session function, which PostgreSQL stores as
// a metadata-only default; volatile defaults rewrite the table.
func stableDefault(n *pgquerypb.Node) bool {
	switch e := n.GetNode().(type) {
	case nil:
		return true
	case *pgquerypb.Node_AConst:
		return true
	case *pgquerypb.Node_TypeCast:
		return stableDefault(e.TypeCast.GetArg())
	case *pgquerypb.Node_SqlvalueFunction:
		return true
	case *pgquerypb.Node_FuncCall:
		names := stringList(e.FuncCall.GetFuncname())
		if len(names) == 0 || len(e.FuncCall.GetArgs()) != 0 {
			return false
		}
		return stableDefaults[strings.ToLower(names[len(names)-1])]
	}
	return false
}

func rewriteClass(form string) error {
	return notYet("rewrite-class DDL is not available yet: "+form+" rewrites the table",
		"column type changes and volatile-default ADD COLUMN run online; this form still needs a new table and a copy of the rows")
}

func shardKeyChangeError(r *rel) error {
	return notYet("the shard key column \""+r.shardKey+"\" of sharded table \""+r.name+"\" cannot be dropped, renamed or retyped",
		"change the shard key with a rekey workflow")
}

func (w *walker) rename(s *pgquerypb.RenameStmt) error {
	switch s.GetRenameType() {
	case pgquerypb.ObjectType_OBJECT_TABLE:
		r, err := w.lookup(s.GetRelation())
		if err != nil {
			return err
		}
		if r != nil && r.kind != placeUnsharded {
			return notYet("renaming the sharded or reference table \""+r.name+"\" is not available yet",
				"the catalog declares the table by name; declare the new name and copy the rows")
		}
		return w.migration(Migration{Kind: "ALTER TABLE", Scope: ScopeHome})
	case pgquerypb.ObjectType_OBJECT_COLUMN:
		if s.GetRelationType() != pgquerypb.ObjectType_OBJECT_TABLE {
			return w.migration(Migration{Kind: "ALTER " + objectWord(s.GetRelationType()), Scope: ScopeExisting})
		}
		r, err := w.lookup(s.GetRelation())
		if err != nil {
			return err
		}
		if r != nil && r.kind == placeSharded && s.GetSubname() == r.shardKey {
			return shardKeyChangeError(r)
		}
		scope, err := w.relScope([]*rel{r})
		if err != nil {
			return err
		}
		return w.migration(Migration{Kind: "ALTER TABLE", Scope: scope})
	case pgquerypb.ObjectType_OBJECT_INDEX, pgquerypb.ObjectType_OBJECT_VIEW:
		return w.migration(Migration{Kind: "ALTER " + objectWord(s.GetRenameType()), Scope: ScopeExisting})
	case pgquerypb.ObjectType_OBJECT_ROLE:
		return notYet("renaming a role is not available through the router",
			"the catalog records roles, memberships and grants by name; create the new role and drop the old one")
	case pgquerypb.ObjectType_OBJECT_SCHEMA, pgquerypb.ObjectType_OBJECT_SEQUENCE, pgquerypb.ObjectType_OBJECT_TYPE,
		pgquerypb.ObjectType_OBJECT_DATABASE:
		return w.migration(Migration{Kind: "ALTER " + objectWord(s.GetRenameType()), Scope: ScopeAll})
	}
	return w.unshardedOnly()
}

func objectWord(t pgquerypb.ObjectType) string {
	return strings.TrimPrefix(t.String(), "OBJECT_")
}

func (w *walker) drop(d *pgquerypb.DropStmt) error {
	kind := "DROP " + objectWord(d.GetRemoveType())
	switch d.GetRemoveType() {
	case pgquerypb.ObjectType_OBJECT_TABLE:
		var rvs []*pgquerypb.RangeVar
		for _, obj := range d.GetObjects() {
			if rv := qualifiedName(obj); rv != nil {
				rvs = append(rvs, rv)
			}
		}
		rels, err := w.lookupList(rvs)
		if err != nil {
			return err
		}
		scope, err := w.relScope(rels)
		if err != nil {
			return err
		}
		m := Migration{Kind: kind, Scope: scope}
		if len(rvs) == 1 {
			m.Object = relationRef(rvs[0], objectAbsent)
		}
		return w.migration(m)
	case pgquerypb.ObjectType_OBJECT_INDEX, pgquerypb.ObjectType_OBJECT_VIEW:
		m := Migration{Kind: kind, Scope: ScopeExisting}
		if d.GetConcurrent() {
			m.Strategy = StrategyConcurrent
		}
		if objs := d.GetObjects(); len(objs) == 1 {
			if rv := qualifiedName(objs[0]); rv != nil {
				m.Object = relationRef(rv, objectAbsent)
			}
		}
		if d.GetRemoveType() == pgquerypb.ObjectType_OBJECT_INDEX && !d.GetConcurrent() && len(d.GetObjects()) == 1 && d.GetBehavior() != pgquerypb.DropBehavior_DROP_CASCADE {
			sql, err := w.deparse(func(n *pgquerypb.Node) { n.GetDropStmt().Concurrent = true })
			if err != nil {
				return err
			}
			m.Strategy, m.Statement = StrategyConcurrent, sql
		}
		return w.migration(m)
	case pgquerypb.ObjectType_OBJECT_SCHEMA:
		m := Migration{Kind: kind, Scope: ScopeAll}
		if objs := d.GetObjects(); len(objs) == 1 {
			m.Object = ObjectRef{Kind: "schema", Name: objs[0].GetString_().GetSval(), Expect: objectAbsent}
		}
		return w.migration(m)
	case pgquerypb.ObjectType_OBJECT_SEQUENCE, pgquerypb.ObjectType_OBJECT_TYPE:
		return w.migration(Migration{Kind: kind, Scope: ScopeAll})
	}
	return w.unshardedOnly()
}

// qualifiedName turns a List of name parts into a RangeVar.
func qualifiedName(obj *pgquerypb.Node) *pgquerypb.RangeVar {
	names := stringList(obj.GetList().GetItems())
	if len(names) == 0 {
		return nil
	}
	rv := &pgquerypb.RangeVar{Relname: names[len(names)-1]}
	if len(names) >= 2 {
		rv.Schemaname = names[len(names)-2]
	}
	return rv
}

// createView places a view on every shard when its query reads sharded or
// reference tables and on the home shard otherwise.
func (w *walker) createView(v *pgquerypb.ViewStmt) error {
	inner := &walker{sess: w.sess, plan: &Plan{home: w.plan.home, set: w.plan.set, snap: w.plan.snap}, tree: w.tree, root: v.GetQuery()}
	if err := inner.statement(v.GetQuery()); err != nil {
		return err
	}
	scope := ScopeHome
	for _, r := range inner.rels {
		if r.kind != placeUnsharded {
			scope = ScopeAll
		}
	}
	return w.migration(Migration{Kind: "CREATE VIEW", Scope: scope, Object: relationRef(v.GetView(), objectPresent)})
}

func (w *walker) reindex(s *pgquerypb.ReindexStmt) error {
	m := Migration{Kind: "REINDEX"}
	for _, p := range s.GetParams() {
		if strings.EqualFold(p.GetDefElem().GetDefname(), "concurrently") {
			m.Strategy = StrategyConcurrent
		}
	}
	if m.Strategy == "" && s.GetKind() != pgquerypb.ReindexObjectType_REINDEX_OBJECT_SYSTEM {
		sql, err := w.deparse(func(n *pgquerypb.Node) {
			r := n.GetReindexStmt()
			r.Params = append(r.Params, &pgquerypb.Node{Node: &pgquerypb.Node_DefElem{DefElem: &pgquerypb.DefElem{Defname: "concurrently"}}})
		})
		if err != nil {
			return err
		}
		m.Strategy, m.Statement = StrategyConcurrent, sql
	}
	switch s.GetKind() {
	case pgquerypb.ReindexObjectType_REINDEX_OBJECT_TABLE:
		r, err := w.lookup(s.GetRelation())
		if err != nil {
			return err
		}
		if m.Scope, err = w.relScope([]*rel{r}); err != nil {
			return err
		}
	case pgquerypb.ReindexObjectType_REINDEX_OBJECT_INDEX:
		m.Scope = ScopeExisting
	default:
		m.Scope = ScopeAll
	}
	return w.migration(m)
}

func (w *walker) grant(g *pgquerypb.GrantStmt) error {
	kind := "GRANT"
	if !g.GetIsGrant() {
		kind = "REVOKE"
	}
	if g.GetTargtype() == pgquerypb.GrantTargetType_ACL_TARGET_DEFAULTS {
		return notYet("ALTER DEFAULT PRIVILEGES is not available through the router",
			"default ACLs are recorded per creating role and schema; GRANT on the objects after creating them")
	}
	// A grant to one of the cluster's own roles changes what the control
	// plane itself may do on every shard.
	for _, r := range g.GetGrantees() {
		if err := checkRoleName(r.GetRoleSpec().GetRolename()); err != nil {
			return err
		}
	}
	rc := &catalog.RoleChanges{}
	if g.GetIsGrant() {
		rc.Grants = grantChanges(g)
	} else {
		rc.Revokes = grantChanges(g)
	}
	if len(rc.Grants) == 0 && len(rc.Revokes) == 0 {
		rc = nil
	}
	if g.GetTargtype() == pgquerypb.GrantTargetType_ACL_TARGET_OBJECT && g.GetObjtype() == pgquerypb.ObjectType_OBJECT_TABLE {
		var rvs []*pgquerypb.RangeVar
		for _, obj := range g.GetObjects() {
			if rv := obj.GetRangeVar(); rv != nil {
				rvs = append(rvs, rv)
			}
		}
		rels, err := w.lookupList(rvs)
		if err != nil {
			return err
		}
		scope, err := w.relScope(rels)
		if err != nil {
			return err
		}
		return w.migration(Migration{Kind: kind, Scope: scope, Roles: rc})
	}
	return w.migration(Migration{Kind: kind, Scope: ScopeAll, Roles: rc})
}

// createRole replaces a plaintext PASSWORD by its SCRAM verifier so every
// shard stores the same verifier and the catalog can mirror it.
func (w *walker) createRole(raw *pgquerypb.RawStmt, s *pgquerypb.CreateRoleStmt) error {
	if err := checkRoleName(s.GetRole()); err != nil {
		return err
	}
	m := Migration{Kind: "CREATE ROLE", Scope: ScopeAll, Role: s.GetRole(), RoleOp: "create",
		Object: ObjectRef{Kind: "role", Name: s.GetRole(), Expect: objectPresent}}
	attrs, err := roleAttributes(s.GetOptions())
	if err != nil {
		return err
	}
	verifier, stmt, err := w.hashPassword(raw, s.GetOptions())
	if err != nil {
		return err
	}
	m.Verifier, m.Statement = verifier, stmt
	m.Roles = &catalog.RoleChanges{Attributes: attrs, GrantMembers: createRoleMembers(s.GetRole(), s.GetOptions())}
	if s.GetStmtType() == pgquerypb.RoleStmtType_ROLESTMT_ROLE || s.GetStmtType() == pgquerypb.RoleStmtType_ROLESTMT_GROUP {
		if attrs == nil || attrs.Login == nil {
			if m.Roles.Attributes == nil {
				m.Roles.Attributes = &catalog.RoleAttributes{}
			}
			login := false
			m.Roles.Attributes.Login = &login
		}
	}
	return w.migration(m)
}

func (w *walker) alterRole(raw *pgquerypb.RawStmt, s *pgquerypb.AlterRoleStmt) error {
	if err := checkRoleName(s.GetRole().GetRolename()); err != nil {
		return err
	}
	m := Migration{Kind: "ALTER ROLE", Scope: ScopeAll, Role: s.GetRole().GetRolename(), RoleOp: "alter"}
	attrs, err := roleAttributes(s.GetOptions())
	if err != nil {
		return err
	}
	verifier, stmt, err := w.hashPassword(raw, s.GetOptions())
	if err != nil {
		return err
	}
	m.Verifier, m.Statement = verifier, stmt
	if attrs != nil {
		m.Roles = &catalog.RoleChanges{Attributes: attrs}
	}
	return w.migration(m)
}

func (w *walker) dropRole(s *pgquerypb.DropRoleStmt) error {
	m := Migration{Kind: "DROP ROLE", Scope: ScopeAll, RoleOp: "drop", Roles: &catalog.RoleChanges{}}
	for _, r := range s.GetRoles() {
		name := r.GetRoleSpec().GetRolename()
		if err := checkRoleName(name); err != nil {
			return err
		}
		m.Roles.DropRoles = append(m.Roles.DropRoles, name)
	}
	if len(m.Roles.DropRoles) == 1 {
		m.Role = m.Roles.DropRoles[0]
		m.Object = ObjectRef{Kind: "role", Name: m.Role, Expect: objectAbsent}
	}
	return w.migration(m)
}

// hashPassword rewrites a PASSWORD 'plaintext' option into the SCRAM
// verifier and returns it with the deparsed statement; a statement without
// a plaintext password is left alone.
func (w *walker) hashPassword(raw *pgquerypb.RawStmt, options []*pgquerypb.Node) (verifier, stmt string, err error) {
	var pw *pgquerypb.DefElem
	for _, o := range options {
		d := o.GetDefElem()
		if strings.EqualFold(d.GetDefname(), "password") {
			pw = d
		}
	}
	if pw == nil {
		return "", "", nil
	}
	plain := pw.GetArg().GetString_().GetSval()
	if plain == "" {
		return "", "", nil
	}
	if v, perr := pgwire.ParseSCRAMVerifier(plain); perr == nil {
		return v.String(), "", nil
	}
	v, err := pgwire.BuildSCRAMVerifier(plain, nil, pgwire.DefaultSCRAMIterations)
	if err != nil {
		return "", "", pgwire.Errorf(pgwire.CodeInternalError, "hashing the role password: %v", err)
	}
	clone := proto.Clone(raw).(*pgquerypb.RawStmt)
	rewritePassword(clone.GetStmt(), v.String())
	tree := &pgquerypb.ParseResult{Version: w.parseVersion(), Stmts: []*pgquerypb.RawStmt{clone}}
	stmt, err = pgparser.Deparse(tree)
	if err != nil {
		return "", "", pgwire.Errorf(pgwire.CodeInternalError, "rewriting the role statement: %v", err)
	}
	return v.String(), stmt, nil
}

func (w *walker) parseVersion() int32 {
	if pr, ok := w.tree.(*pgquerypb.ParseResult); ok {
		return pr.GetVersion()
	}
	return 0
}

func rewritePassword(node *pgquerypb.Node, verifier string) {
	var options []*pgquerypb.Node
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_CreateRoleStmt:
		options = n.CreateRoleStmt.GetOptions()
	case *pgquerypb.Node_AlterRoleStmt:
		options = n.AlterRoleStmt.GetOptions()
	}
	for _, o := range options {
		d := o.GetDefElem()
		if strings.EqualFold(d.GetDefname(), "password") {
			d.Arg = &pgquerypb.Node{Node: &pgquerypb.Node_String_{String_: &pgquerypb.String{Sval: verifier}}}
		}
	}
}

func (w *walker) createDatabase(s *pgquerypb.CreatedbStmt) error {
	if err := catalog.CheckDatabaseName(s.GetDbname()); err != nil {
		return pgwire.Errorf("42602", "%v", err)
	}
	return w.migration(Migration{Kind: "CREATE DATABASE", Scope: ScopeAll, Database: s.GetDbname(), DatabaseOp: "create",
		Object: ObjectRef{Kind: "database", Name: s.GetDbname(), Expect: objectPresent}})
}

func (w *walker) dropDatabase(s *pgquerypb.DropdbStmt) error {
	if s.GetDbname() == w.sess.Database {
		return pgwire.Errorf("55006", "cannot drop the currently open database")
	}
	return w.migration(Migration{Kind: "DROP DATABASE", Scope: ScopeAll, Database: s.GetDbname(), DatabaseOp: "drop",
		Object: ObjectRef{Kind: "database", Name: s.GetDbname(), Expect: objectAbsent}})
}

// alterObjectScope is the scope of ALTER ... SET SCHEMA / OWNER TO.
func (w *walker) alterObject(kind string, objType pgquerypb.ObjectType, rv *pgquerypb.RangeVar) error {
	switch objType {
	case pgquerypb.ObjectType_OBJECT_TABLE:
		r, err := w.lookup(rv)
		if err != nil {
			return err
		}
		if r != nil && r.kind != placeUnsharded && kind == "ALTER TABLE SET SCHEMA" {
			return notYet("moving the sharded or reference table \""+r.name+"\" to another schema is not available yet",
				"the catalog declares the table by schema and name")
		}
		scope, err := w.relScope([]*rel{r})
		if err != nil {
			return err
		}
		return w.migration(Migration{Kind: "ALTER TABLE", Scope: scope})
	case pgquerypb.ObjectType_OBJECT_INDEX, pgquerypb.ObjectType_OBJECT_VIEW:
		return w.migration(Migration{Kind: "ALTER " + objectWord(objType), Scope: ScopeExisting})
	case pgquerypb.ObjectType_OBJECT_SCHEMA, pgquerypb.ObjectType_OBJECT_SEQUENCE, pgquerypb.ObjectType_OBJECT_TYPE,
		pgquerypb.ObjectType_OBJECT_DATABASE:
		return w.migration(Migration{Kind: "ALTER " + objectWord(objType), Scope: ScopeAll})
	}
	return w.unshardedOnly()
}

// vacuumFull reports whether a VACUUM statement carries the FULL option.
func vacuumFull(v *pgquerypb.VacuumStmt) bool {
	if !v.GetIsVacuumcmd() {
		return false
	}
	for _, o := range v.GetOptions() {
		d := o.GetDefElem()
		if strings.EqualFold(d.GetDefname(), "full") {
			arg := d.GetArg()
			if arg == nil {
				return true
			}
			switch strings.ToLower(arg.GetString_().GetSval()) {
			case "", "on", "true", "yes", "1":
				return true
			}
		}
	}
	return false
}

// vacuum routes VACUUM: a VACUUM (FULL) of one sharded or reference table
// becomes a repack migration the applier runs as REPACK (CONCURRENTLY) on
// PostgreSQL 19+; everything else keeps the maintenance rules.
func (w *walker) vacuum(v *pgquerypb.VacuumStmt) error {
	rels := v.GetRels()
	if vacuumFull(v) && len(rels) == 1 {
		rv := rels[0].GetVacuumRelation().GetRelation()
		r, err := w.lookup(rv)
		if err != nil {
			return err
		}
		if r != nil && r.kind != placeUnsharded {
			return w.migration(Migration{Kind: "VACUUM", Scope: ScopeAll, Strategy: StrategyRepack,
				Object: relationRef(rv, objectPresent)})
		}
	}
	for _, item := range rels {
		if err := w.maintenance("VACUUM and ANALYZE", item.GetVacuumRelation().GetRelation()); err != nil {
			return err
		}
	}
	return nil
}
