package plan

import (
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// createFunction fans a function or procedure out to every shard. A function
// exists per database, and a database exists in every group, so the one
// place a function belongs is everywhere: a trigger on a sharded table calls
// it on whichever shard the row is on. Migration tools create their trigger
// functions this way (PGS-867), and a function created on the home shard
// alone would leave every other shard's trigger calling nothing.
func (w *walker) createFunction(f *pgquerypb.CreateFunctionStmt) error {
	kind := "CREATE FUNCTION"
	if f.GetIsProcedure() {
		kind = "CREATE PROCEDURE"
	}
	m := Migration{Kind: kind, Scope: ScopeAll}
	// CREATE OR REPLACE is already safe to run again after a crash. A plain
	// CREATE is not, so the applier needs to recognise the function it made.
	if !f.GetReplace() {
		var args []*pgquerypb.TypeName
		for _, p := range f.GetParameters() {
			fp := p.GetFunctionParameter()
			switch fp.GetMode() {
			case pgquerypb.FunctionParameterMode_FUNC_PARAM_OUT, pgquerypb.FunctionParameterMode_FUNC_PARAM_TABLE:
				continue
			}
			args = append(args, fp.GetArgType())
		}
		if schema, sig, ok := functionSignature(f.GetFuncname(), args); ok {
			m.Object = ObjectRef{Kind: "function", Schema: schema, Name: sig, Expect: objectPresent}
		}
	}
	return w.migration(m)
}

// dropFunction removes a function or procedure from every shard that has it.
func (w *walker) dropFunction(kind string, d *pgquerypb.DropStmt) error {
	m := Migration{Kind: kind, Scope: ScopeExisting}
	if d.GetMissingOk() {
		m.Scope = ScopeAll
	}
	if objs := d.GetObjects(); len(objs) == 1 {
		o := objs[0].GetObjectWithArgs()
		if o != nil && !o.GetArgsUnspecified() {
			var args []*pgquerypb.TypeName
			for _, a := range o.GetObjargs() {
				args = append(args, a.GetTypeName())
			}
			if schema, sig, ok := functionSignature(o.GetObjname(), args); ok {
				m.Object = ObjectRef{Kind: "function", Schema: schema, Name: sig, Expect: objectAbsent}
			}
		}
	}
	return w.migration(m)
}

// createTrigger places a trigger where its table is.
//
// Not on a reference table: a trigger there is something the router cannot
// prove fires identically on every copy, so once one exists every write to
// the table is refused. Creating it would turn a working table read-only
// with no error at the CREATE, so the CREATE is what is refused.
func (w *walker) createTrigger(tr *pgquerypb.CreateTrigStmt) error {
	r, err := w.lookup(tr.GetRelation())
	if err != nil {
		return err
	}
	if r != nil && r.kind == placeReference {
		return notYet("CREATE TRIGGER on a reference table is not available: the router refuses writes to a reference table with a trigger, since it cannot prove the trigger writes the same row on every shard",
			"keep the logic in the application, or make the table unsharded")
	}
	scope, err := w.relScope([]*rel{r})
	if err != nil {
		return err
	}
	return w.migration(Migration{Kind: "CREATE TRIGGER", Scope: scope})
}

// comment sets a table's, view's or column's comment where the relation is.
// Comments on other objects stay refused: nothing says where they live.
func (w *walker) comment(c *pgquerypb.CommentStmt) error {
	var rv *pgquerypb.RangeVar
	switch c.GetObjtype() {
	case pgquerypb.ObjectType_OBJECT_TABLE, pgquerypb.ObjectType_OBJECT_VIEW:
		rv = qualifiedName(c.GetObject())
	case pgquerypb.ObjectType_OBJECT_COLUMN:
		names := stringList(c.GetObject().GetList().GetItems())
		if len(names) >= 2 {
			rv = &pgquerypb.RangeVar{Relname: names[len(names)-2]}
			if len(names) >= 3 {
				rv.Schemaname = names[len(names)-3]
			}
		}
	}
	if rv == nil {
		return w.unfannable("COMMENT ON " + objectWord(c.GetObjtype()))
	}
	r, err := w.lookup(rv)
	if err != nil {
		return err
	}
	scope, err := w.relScope([]*rel{r})
	if err != nil {
		return err
	}
	return w.migration(Migration{Kind: "COMMENT", Scope: scope})
}

// functionSignature renders a function's schema, if the statement names
// one, and its unqualified name and argument types the way to_regprocedure
// reads them; or reports that it cannot: a %TYPE argument names a column
// rather than a type.
func functionSignature(name []*pgquerypb.Node, args []*pgquerypb.TypeName) (schema, sig string, ok bool) {
	parts := stringList(name)
	if len(parts) == 0 {
		return "", "", false
	}
	if len(parts) >= 2 {
		schema = parts[len(parts)-2]
	}
	types := make([]string, 0, len(args))
	for _, a := range args {
		if a == nil || a.GetPctType() {
			return "", "", false
		}
		names := stringList(a.GetNames())
		if len(names) == 0 {
			return "", "", false
		}
		t := pgx.Identifier(names).Sanitize() + strings.Repeat("[]", len(a.GetArrayBounds()))
		types = append(types, t)
	}
	return schema, pgx.Identifier{parts[len(parts)-1]}.Sanitize() + "(" + strings.Join(types, ", ") + ")", true
}
