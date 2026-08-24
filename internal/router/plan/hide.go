package plan

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// hideRewriteColumns keeps the working columns of an in-flight rewrite
// migration invisible: a statement naming one is refused, SELECT * over a
// table under rewrite is expanded to the visible columns and an INSERT
// without a column list gets one, so the shards' extra column never
// reaches a client. The rewritten text is left in plan.Rewritten.
func (w *walker) hideRewriteColumns() error {
	switch w.plan.Kind {
	case Refuse, SessionLocal, MigrationKind:
		return nil
	}
	var under []*rel
	for _, r := range w.rels {
		if len(r.hidden) > 0 {
			under = append(under, r)
		}
	}
	if err := w.refuseHiddenNames(); err != nil {
		return err
	}
	if len(under) == 0 {
		return nil
	}
	needStar, err := w.starPlan(under)
	if err != nil {
		return err
	}
	ins := w.root.GetInsertStmt()
	needCols := ins != nil && len(ins.GetCols()) == 0 && w.target != nil && len(w.target.hidden) > 0
	valueWidth := 0
	if needCols {
		rows := ins.GetSelectStmt().GetSelectStmt().GetValuesLists()
		if len(rows) == 0 {
			return notYet("INSERT without a column list into table \""+w.target.name+"\" while it is under an online schema migration is not available",
				"list the columns explicitly")
		}
		valueWidth = len(rows[0].GetList().GetItems())
		for _, row := range rows {
			if len(row.GetList().GetItems()) != valueWidth {
				return notYet("INSERT without a column list into table \""+w.target.name+"\" has VALUES rows of different lengths",
					"list the columns explicitly")
			}
		}
	}
	if !needStar && !needCols {
		return nil
	}
	target := under[0]
	if needCols {
		target = w.target
	}
	if len(target.visible) == 0 {
		e := pgwire.Errorf("55000", "table \"%s\" is under an online schema migration and its column list is not published yet", target.name)
		e.Hint = "retry the statement, or list the columns explicitly"
		return e
	}
	tree := proto.Clone(w.tree.(*pgquerypb.ParseResult)).(*pgquerypb.ParseResult)
	root := tree.GetStmts()[0].GetStmt()
	if needStar {
		expandStars(root, target)
	}
	if needCols {
		if valueWidth > len(target.visible) {
			return notYet("INSERT without a column list into table \""+target.name+"\" has more values than visible columns", "list the columns explicitly")
		}
		clone := root.GetInsertStmt()
		for _, col := range target.visible[:valueWidth] {
			clone.Cols = append(clone.Cols, &pgquerypb.Node{Node: &pgquerypb.Node_ResTarget{ResTarget: &pgquerypb.ResTarget{Name: col}}})
		}
	}
	sql, err := pgparser.Deparse(tree)
	if err != nil {
		return pgwire.Errorf(pgwire.CodeInternalError, "rewriting the statement for the online migration: %v", err)
	}
	w.plan.Rewritten = sql
	return nil
}

// refuseHiddenNames refuses any statement that names a migration working
// column, whether it belongs to a known rewrite or not.
func (w *walker) refuseHiddenNames() error {
	var bad string
	visit(w.root, func(n *pgquerypb.Node) bool {
		if bad != "" {
			return false
		}
		if cr := n.GetColumnRef(); cr != nil {
			for _, f := range stringList(cr.GetFields()) {
				if strings.HasPrefix(f, catalog.HiddenPrefix) {
					bad = f
				}
			}
		}
		if rt := n.GetResTarget(); rt != nil && strings.HasPrefix(rt.GetName(), catalog.HiddenPrefix) {
			bad = rt.GetName()
		}
		return true
	})
	if bad == "" {
		return nil
	}
	e := pgwire.Errorf("42703", "column \"%s\" does not exist", bad)
	e.Hint = "columns starting with " + catalog.HiddenPrefix + " belong to an online schema migration"
	return e
}

// starPlan decides whether stars must be expanded: true when the statement
// has a star whose scope includes a table under rewrite. A star spanning a
// rewrite table next to other tables is refused: the router cannot expand
// the other tables' columns.
func (w *walker) starPlan(under []*rel) (bool, error) {
	hasStar, qualified := false, map[string]bool{}
	visit(w.root, func(n *pgquerypb.Node) bool {
		cr := n.GetColumnRef()
		if cr == nil {
			return true
		}
		fields := cr.GetFields()
		if len(fields) == 0 || fields[len(fields)-1].GetAStar() == nil {
			return true
		}
		hasStar = true
		if len(fields) > 1 {
			qualified[fields[len(fields)-2].GetString_().GetSval()] = true
		} else {
			qualified[""] = true
		}
		return true
	})
	if !hasStar {
		return false, nil
	}
	starHitsRewrite := false
	for _, r := range under {
		if qualified[r.alias] || qualified[""] {
			starHitsRewrite = true
		}
	}
	if !starHitsRewrite {
		return false, nil
	}
	if len(w.rels) > 1 || len(under) > 1 {
		return false, notYet("SELECT * over table \""+under[0].name+"\" while it is under an online schema migration cannot span other tables",
			"list the columns explicitly")
	}
	return true, nil
}

// expandStars replaces every star that resolves to target with target's
// visible columns, qualified the way the star was.
func expandStars(root *pgquerypb.Node, target *rel) {
	visit(root, func(n *pgquerypb.Node) bool {
		s := n.GetSelectStmt()
		if s == nil {
			return true
		}
		var out []*pgquerypb.Node
		for _, t := range s.GetTargetList() {
			rt := t.GetResTarget()
			fields := rt.GetVal().GetColumnRef().GetFields()
			if len(fields) == 0 || fields[len(fields)-1].GetAStar() == nil || rt.GetName() != "" {
				out = append(out, t)
				continue
			}
			var prefix []string
			for _, f := range fields[:len(fields)-1] {
				prefix = append(prefix, f.GetString_().GetSval())
			}
			if len(prefix) > 0 && prefix[len(prefix)-1] != target.alias {
				out = append(out, t)
				continue
			}
			for _, col := range target.visible {
				var fs []*pgquerypb.Node
				for _, p := range prefix {
					fs = append(fs, &pgquerypb.Node{Node: &pgquerypb.Node_String_{String_: &pgquerypb.String{Sval: p}}})
				}
				fs = append(fs, &pgquerypb.Node{Node: &pgquerypb.Node_String_{String_: &pgquerypb.String{Sval: col}}})
				out = append(out, &pgquerypb.Node{Node: &pgquerypb.Node_ResTarget{ResTarget: &pgquerypb.ResTarget{
					Val: &pgquerypb.Node{Node: &pgquerypb.Node_ColumnRef{ColumnRef: &pgquerypb.ColumnRef{Fields: fs}}}}}})
			}
		}
		s.TargetList = out
		return true
	})
}
