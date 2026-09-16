package plan

import (
	"slices"

	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// localSchema reports whether schema is listed in the database's
// local_schemas, so every object in it lives on the home shard.
func (s Session) localSchema(schema string) bool {
	if s.Snapshot == nil || schema == "" {
		return false
	}
	return slices.Contains(s.Snapshot.Databases[s.Database].LocalSchemas, schema)
}

// inLocalSchemas reports whether a DDL statement is about objects that all
// live in the database's local schemas, so it runs on the home shard as it
// would in a local database. Only objects the statement names with their
// schema count: the planner cannot know which schema of a search path an
// unqualified CREATE lands in, and guessing would send a fan-out to one
// shard.
func (s Session) inLocalSchemas(node *pgquerypb.Node) bool {
	if s.Snapshot == nil || len(s.Snapshot.Databases[s.Database].LocalSchemas) == 0 {
		return false
	}
	schemas, eventTrigger := statementSchemas(node)
	if eventTrigger {
		// Event triggers belong to no schema, and one can reach a
		// multi-shard database through the router only by calling a
		// function in a local schema, so dropping or altering one is about
		// that schema's tool.
		return true
	}
	if len(schemas) == 0 {
		return false
	}
	for _, schema := range schemas {
		if !s.localSchema(schema) {
			return false
		}
	}
	return true
}

// statementSchemas lists the schema of every object a DDL statement names,
// with "" for an object named without one, or reports an event trigger
// statement naming no function.
func statementSchemas(node *pgquerypb.Node) (schemas []string, eventTrigger bool) {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_CreateSchemaStmt:
		return []string{n.CreateSchemaStmt.GetSchemaname()}, false
	case *pgquerypb.Node_CreateStmt:
		return []string{n.CreateStmt.GetRelation().GetSchemaname()}, false
	case *pgquerypb.Node_IndexStmt:
		return []string{n.IndexStmt.GetRelation().GetSchemaname()}, false
	case *pgquerypb.Node_AlterTableStmt:
		return []string{n.AlterTableStmt.GetRelation().GetSchemaname()}, false
	case *pgquerypb.Node_ViewStmt:
		return []string{n.ViewStmt.GetView().GetSchemaname()}, false
	case *pgquerypb.Node_CreateTableAsStmt:
		return []string{n.CreateTableAsStmt.GetInto().GetRel().GetSchemaname()}, false
	case *pgquerypb.Node_CreateSeqStmt:
		return []string{n.CreateSeqStmt.GetSequence().GetSchemaname()}, false
	case *pgquerypb.Node_AlterSeqStmt:
		return []string{n.AlterSeqStmt.GetSequence().GetSchemaname()}, false
	case *pgquerypb.Node_CompositeTypeStmt:
		return []string{n.CompositeTypeStmt.GetTypevar().GetSchemaname()}, false
	case *pgquerypb.Node_CreateEnumStmt:
		return []string{nameSchema(n.CreateEnumStmt.GetTypeName(), 2)}, false
	case *pgquerypb.Node_CreateRangeStmt:
		return []string{nameSchema(n.CreateRangeStmt.GetTypeName(), 2)}, false
	case *pgquerypb.Node_CreateFunctionStmt:
		return []string{nameSchema(n.CreateFunctionStmt.GetFuncname(), 2)}, false
	case *pgquerypb.Node_AlterFunctionStmt:
		return []string{nameSchema(n.AlterFunctionStmt.GetFunc().GetObjname(), 2)}, false
	case *pgquerypb.Node_CreateTrigStmt:
		return []string{n.CreateTrigStmt.GetRelation().GetSchemaname()}, false
	case *pgquerypb.Node_CreateEventTrigStmt:
		return []string{nameSchema(n.CreateEventTrigStmt.GetFuncname(), 2)}, false
	case *pgquerypb.Node_AlterEventTrigStmt:
		return nil, true
	case *pgquerypb.Node_CommentStmt:
		return []string{objectSchema(n.CommentStmt.GetObjtype(), n.CommentStmt.GetObject())}, false
	case *pgquerypb.Node_RenameStmt:
		if rv := n.RenameStmt.GetRelation(); rv != nil {
			return []string{rv.GetSchemaname()}, false
		}
		if n.RenameStmt.GetRenameType() == pgquerypb.ObjectType_OBJECT_SCHEMA {
			return []string{n.RenameStmt.GetSubname()}, false
		}
		return []string{objectSchema(n.RenameStmt.GetRenameType(), n.RenameStmt.GetObject())}, false
	case *pgquerypb.Node_AlterOwnerStmt:
		if rv := n.AlterOwnerStmt.GetRelation(); rv != nil {
			return []string{rv.GetSchemaname()}, false
		}
		return []string{objectSchema(n.AlterOwnerStmt.GetObjectType(), n.AlterOwnerStmt.GetObject())}, false
	case *pgquerypb.Node_GrantStmt:
		g := n.GrantStmt
		for _, obj := range g.GetObjects() {
			switch {
			case obj.GetRangeVar() != nil:
				schemas = append(schemas, obj.GetRangeVar().GetSchemaname())
			case g.GetTargtype() == pgquerypb.GrantTargetType_ACL_TARGET_ALL_IN_SCHEMA:
				schemas = append(schemas, obj.GetString_().GetSval())
			default:
				schemas = append(schemas, objectSchema(g.GetObjtype(), obj))
			}
		}
		return schemas, false
	case *pgquerypb.Node_DropStmt:
		d := n.DropStmt
		if d.GetRemoveType() == pgquerypb.ObjectType_OBJECT_EVENT_TRIGGER {
			return nil, true
		}
		for _, obj := range d.GetObjects() {
			schemas = append(schemas, objectSchema(d.GetRemoveType(), obj))
		}
		return schemas, false
	}
	return nil, false
}

// objectSchema is the schema an object node of type t names, or "".
func objectSchema(t pgquerypb.ObjectType, obj *pgquerypb.Node) string {
	switch t {
	case pgquerypb.ObjectType_OBJECT_SCHEMA:
		return obj.GetString_().GetSval()
	case pgquerypb.ObjectType_OBJECT_FUNCTION, pgquerypb.ObjectType_OBJECT_PROCEDURE, pgquerypb.ObjectType_OBJECT_ROUTINE:
		return nameSchema(obj.GetObjectWithArgs().GetObjname(), 2)
	case pgquerypb.ObjectType_OBJECT_COLUMN, pgquerypb.ObjectType_OBJECT_TRIGGER, pgquerypb.ObjectType_OBJECT_POLICY,
		pgquerypb.ObjectType_OBJECT_RULE, pgquerypb.ObjectType_OBJECT_TABCONSTRAINT:
		return nameSchema(obj.GetList().GetItems(), 3)
	case pgquerypb.ObjectType_OBJECT_TYPE, pgquerypb.ObjectType_OBJECT_DOMAIN:
		if tn := obj.GetTypeName(); tn != nil {
			return nameSchema(tn.GetNames(), 2)
		}
	}
	return nameSchema(obj.GetList().GetItems(), 2)
}

// nameSchema is the schema of a dotted name whose unqualified form has
// parts-1 components before the object's own name: 2 for schema.object,
// 3 for schema.table.column.
func nameSchema(name []*pgquerypb.Node, parts int) string {
	names := stringList(name)
	if len(names) < parts {
		return ""
	}
	return names[len(names)-parts]
}
