package plan

import (
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// StrategyRewrite marks a migration the applier runs as an online
// OID-preserving column duplication (see catalog.RewriteChange).
const StrategyRewrite = catalog.StrategyRewrite

// StrategyRepack marks a full-table repack (see catalog.StrategyRepack).
const StrategyRepack = catalog.StrategyRepack

// rewriteCmd reports whether an ALTER TABLE action needs the rewrite
// strategy: a column type change, or ADD COLUMN with a volatile DEFAULT.
func rewriteCmd(c *pgquerypb.AlterTableCmd) bool {
	switch c.GetSubtype() {
	case pgquerypb.AlterTableType_AT_AlterColumnType:
		return true
	case pgquerypb.AlterTableType_AT_AddColumn:
		for _, con := range c.GetDef().GetColumnDef().GetConstraints() {
			cs := con.GetConstraint()
			if cs.GetContype() == pgquerypb.ConstrType_CONSTR_DEFAULT && !stableDefault(cs.GetRawExpr()) {
				return true
			}
		}
	}
	return false
}

// rewriteChange builds the RewriteChange of a single rewrite-class ALTER
// TABLE action; nil means the statement is not rewrite class.
func (w *walker) rewriteChange(a *pgquerypb.AlterTableStmt, r *rel) (*catalog.RewriteChange, error) {
	hasRewrite := false
	for _, n := range a.GetCmds() {
		if rewriteCmd(n.GetAlterTableCmd()) {
			hasRewrite = true
		}
	}
	if !hasRewrite {
		return nil, nil
	}
	if len(a.GetCmds()) > 1 {
		return nil, notYet("ALTER TABLE with several actions of which one needs an online rewrite is not available yet",
			"run the column type change or volatile-default ADD COLUMN as its own ALTER TABLE statement")
	}
	c := a.GetCmds()[0].GetAlterTableCmd()
	rv := a.GetRelation()
	rw := &catalog.RewriteChange{Schema: rv.GetSchemaname(), Table: rv.GetRelname()}
	if rw.Schema == "" && r != nil {
		rw.Schema = r.schema
	}
	def := c.GetDef().GetColumnDef()
	var err error
	if rw.NewType, err = w.typeSQL(def.GetTypeName()); err != nil {
		return nil, err
	}
	switch c.GetSubtype() {
	case pgquerypb.AlterTableType_AT_AlterColumnType:
		rw.Column = c.GetName()
		if c.GetBehavior() == pgquerypb.DropBehavior_DROP_CASCADE {
			return nil, notYet("ALTER COLUMN ... TYPE ... CASCADE is not available yet", "")
		}
		if def.GetCollClause() != nil {
			return nil, notYet("ALTER COLUMN ... TYPE with a COLLATE clause is not available with the online rewrite yet",
				"change the type first, then the collation")
		}
		if u := def.GetRawDefault(); u != nil {
			if rw.Using, err = w.exprSQL(u); err != nil {
				return nil, err
			}
		} else {
			rw.Using = "CAST(" + quoteIdent(rw.Column) + " AS " + rw.NewType + ")"
		}
	case pgquerypb.AlterTableType_AT_AddColumn:
		rw.Add = true
		rw.Column = def.GetColname()
		for _, con := range def.GetConstraints() {
			cs := con.GetConstraint()
			switch cs.GetContype() {
			case pgquerypb.ConstrType_CONSTR_DEFAULT:
				if rw.Default, err = w.exprSQL(cs.GetRawExpr()); err != nil {
					return nil, err
				}
			case pgquerypb.ConstrType_CONSTR_NULL:
			default:
				return nil, notYet("ADD COLUMN with a volatile DEFAULT cannot carry other constraints yet",
					"add the column first, then add the constraint in its own statement")
			}
		}
		if def.GetIsNotNull() {
			return nil, notYet("ADD COLUMN with a volatile DEFAULT cannot carry other constraints yet",
				"add the column first, then SET NOT NULL in its own statement")
		}
	}
	return rw, nil
}

// exprSQL deparses one expression by wrapping it in a SELECT.
func (w *walker) exprSQL(expr *pgquerypb.Node) (string, error) {
	sel := &pgquerypb.Node{Node: &pgquerypb.Node_SelectStmt{SelectStmt: &pgquerypb.SelectStmt{
		TargetList: []*pgquerypb.Node{{Node: &pgquerypb.Node_ResTarget{ResTarget: &pgquerypb.ResTarget{Val: expr}}}}}}}
	sql, err := pgparser.Deparse(&pgquerypb.ParseResult{Version: w.parseVersion(), Stmts: []*pgquerypb.RawStmt{{Stmt: sel}}})
	if err != nil {
		return "", pgwire.Errorf(pgwire.CodeInternalError, "rewriting the expression: %v", err)
	}
	const prefix = "SELECT "
	if len(sql) <= len(prefix) {
		return "", pgwire.Errorf(pgwire.CodeInternalError, "rewriting the expression: empty deparse")
	}
	return sql[len(prefix):], nil
}

// typeSQL deparses a TypeName by casting NULL to it.
func (w *walker) typeSQL(t *pgquerypb.TypeName) (string, error) {
	cast := &pgquerypb.Node{Node: &pgquerypb.Node_TypeCast{TypeCast: &pgquerypb.TypeCast{
		Arg:      &pgquerypb.Node{Node: &pgquerypb.Node_AConst{AConst: &pgquerypb.A_Const{Isnull: true}}},
		TypeName: t}}}
	sql, err := w.exprSQL(cast)
	if err != nil {
		return "", err
	}
	const prefix = "CAST(NULL AS "
	if len(sql) > len(prefix)+1 && sql[:len(prefix)] == prefix && sql[len(sql)-1] == ')' {
		return sql[len(prefix) : len(sql)-1], nil
	}
	// pg_query deparses simple casts as NULL::type.
	const alt = "NULL::"
	if len(sql) > len(alt) && sql[:len(alt)] == alt {
		return sql[len(alt):], nil
	}
	return "", pgwire.Errorf(pgwire.CodeInternalError, "rewriting the type name: unexpected deparse %q", sql)
}
