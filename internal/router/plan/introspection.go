package plan

import (
	"strings"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"google.golang.org/protobuf/proto"
)

// reservedColumnFilters are the catalogs that list a table's columns by
// name, and the column of each that holds the name.
//
// Only these two: a working column is a column, and these are where a
// client asks what columns a table has. The rest of the catalog describes
// it in ways nothing generates SQL from.
// likeReservedPrefix matches the reserved prefix in a LIKE pattern: the
// underscores are wildcards unless escaped, and LIKE's default escape is
// the backslash.
var likeReservedPrefix = strings.ReplaceAll(catalog.HiddenPrefix, "_", `\_`) + "%"

var reservedColumnFilters = map[string]struct{ schema, column string }{
	"columns":      {"information_schema", "column_name"},
	"pg_attribute": {"pg_catalog", "attname"},
}

// editable is the parse tree a rewrite may change: a clone, made once and
// shared by every pass that rewrites this statement.
//
// The parse result itself is the cache's, handed to every session that
// sends the same SQL, and mutating it means the next execution starts from
// the last one's output. For this pass that is unbounded: the walk finds
// the catalog reference inside the subquery it built last time and wraps it
// again, so the text grows on every execution until it is refused for
// length. One clone rather than one each also lets the passes compose --
// two clones would each deparse their own half and the last would win.
func (w *walker) editable() *pgquerypb.ParseResult {
	if w.edit == nil {
		w.edit = proto.Clone(w.tree.(*pgquerypb.ParseResult)).(*pgquerypb.ParseResult)
	}
	return w.edit
}

// hideReservedFromIntrospection filters pgshard's own working columns out
// of the catalogs a client reads to learn what columns a table has.
//
// The router refuses a statement that names one and keeps it out of
// SELECT *, but introspection listed it: verified against a real
// PostgreSQL, information_schema.columns and pg_attribute both report the
// working column of an in-flight rewrite. Anything that reads the schema
// and then writes SQL from it -- an ORM's model check, a migration tool's
// diff, a schema dump -- therefore proposes a column the router will not
// let it name.
//
// The filter is on the reserved prefix rather than on the migration in
// flight, because the prefix is reserved whatever the catalog says: a
// column left behind by a rewrite that failed is not part of anyone's
// schema either.
//
// It is not the whole of PGS-590. Introspection still answers for the home
// shard rather than the cluster, so a mid-migration divergence between
// shards is still invisible and a reference table still looks like a
// sharded one. Those need catalog-backed logical views; this is the one
// case where the router already knows the truthful answer and was hiding
// it everywhere else.
func (w *walker) hideReservedFromIntrospection() error {
	switch w.plan.Kind {
	case Refuse, SessionLocal, MigrationKind:
		// DDL is applied from the recorded statement, not from the
		// rewritten text, so a rewrite here would be computed, ignored,
		// and still cost the re-plan that a non-empty Rewritten forces.
		return nil
	}
	// Cheap first: the walker met every relation of this statement while
	// planning it, so it already knows whether either filtered catalog is
	// among them. Without this an ordinary statement -- which is nearly all
	// of them -- paid two full walks of its parse tree to discover it names
	// neither, and the walk is the dominant cost of planning.
	if !w.mentionsFilteredCatalog() {
		return nil
	}
	if w.referencesCatalogDirectly() {
		// Two ways a statement can depend on the catalog being a table
		// rather than a subquery over one: naming it by schema
		// (information_schema.columns.column_name) and reading a system
		// column off it. Both are legal SQL that a subquery does not
		// answer, and neither is what a client reflecting on a schema
		// sends, so the filter stands aside rather than break them.
		return nil
	}
	tree := w.editable()
	if !w.filterIntrospection(tree.GetStmts()[0].GetStmt()) {
		return nil
	}
	sql, err := pgparser.Deparse(tree)
	if err != nil {
		return pgwire.Errorf(pgwire.CodeInternalError, "filtering pgshard's own columns out of introspection: %v", err)
	}
	w.plan.Rewritten = sql
	return nil
}

// mentionsFilteredCatalog reports whether the statement named a catalog
// this filters, from the relations the planner already resolved.
func (w *walker) mentionsFilteredCatalog() bool {
	for _, r := range w.rels {
		if _, ok := reservedColumnFilters[strings.ToLower(r.name)]; ok {
			return true
		}
	}
	return false
}

// filterIntrospection replaces every range var naming one of those
// catalogs with a subquery over it that drops the reserved names, and
// reports whether it changed anything.
//
// A subquery stands in for a table wherever a table can appear, including
// the nullable side of an outer join, so this needs no case analysis of
// where the reference sits. What it does need is to find every SELECT, not
// only the statement's own: the reference is as likely to be inside an
// EXISTS in the target list as in the top-level FROM, which is how the
// first version of this missed the query that found the bug.
//
// The replacements are collected first and applied afterwards. Swapping a
// node in while the walk is still descending would send the walk into the
// subquery this just built, whose own FROM names the same catalog, and it
// would wrap it again for as long as it had stack.
func (w *walker) filterIntrospection(root *pgquerypb.Node) bool {
	var targets []*pgquerypb.Node
	visit(root, func(n *pgquerypb.Node) bool {
		switch s := n.GetNode().(type) {
		case *pgquerypb.Node_SelectStmt:
			collectSelect(s.SelectStmt, &targets)
		case *pgquerypb.Node_UpdateStmt:
			collectFromItems(s.UpdateStmt.GetFromClause(), &targets)
		case *pgquerypb.Node_DeleteStmt:
			collectFromItems(s.DeleteStmt.GetUsingClause(), &targets)
		}
		return true
	})
	changed := false
	for _, item := range targets {
		rv := item.GetRangeVar()
		if rv == nil {
			// Already replaced: a from item can be reached twice when a
			// statement nests the same select.
			continue
		}
		item.Node = &pgquerypb.Node_RangeSubselect{RangeSubselect: reservedFilterFor(rv)}
		changed = true
	}
	return changed
}

// collectSelect gathers from one SELECT and from the arms of any set
// operation under it. The arms are SelectStmt messages rather than Nodes,
// so the reflective walk never offers them to the callback: a UNION of two
// catalog reads went through unfiltered.
func collectSelect(s *pgquerypb.SelectStmt, out *[]*pgquerypb.Node) {
	if s == nil {
		return
	}
	collectFromItems(s.GetFromClause(), out)
	collectSelect(s.GetLarg(), out)
	collectSelect(s.GetRarg(), out)
}

// collectFromItems gathers the from-clause range vars that name a filtered
// catalog, descending through joins.
func collectFromItems(items []*pgquerypb.Node, out *[]*pgquerypb.Node) {
	for _, item := range items {
		switch n := item.GetNode().(type) {
		case *pgquerypb.Node_RangeVar:
			if reservedFilterFor(n.RangeVar) != nil {
				*out = append(*out, item)
			}
		case *pgquerypb.Node_JoinExpr:
			collectFromItems([]*pgquerypb.Node{n.JoinExpr.GetLarg(), n.JoinExpr.GetRarg()}, out)
		}
	}
}

// reservedFilterFor builds the subquery that stands in for rv, or nil when
// rv is not one of the catalogs this filters.
func reservedFilterFor(rv *pgquerypb.RangeVar) *pgquerypb.RangeSubselect {
	if rv == nil {
		return nil
	}
	want, ok := reservedColumnFilters[strings.ToLower(rv.GetRelname())]
	if !ok {
		return nil
	}
	// An unqualified name resolves through the search path, and both of
	// these live in a schema PostgreSQL searches ahead of the user's. A
	// name qualified with anything else is a different relation.
	if sc := rv.GetSchemaname(); sc != "" && !strings.EqualFold(sc, want.schema) {
		return nil
	}
	name := rv.GetRelname()
	if a := rv.GetAlias().GetAliasname(); a != "" {
		name = a
	}
	sub, err := pgparser.Parse("select * from " + want.schema + "." + rv.GetRelname() +
		" where " + want.column + " not like '" + likeReservedPrefix + "'")
	if err != nil || len(sub.Stmts) != 1 {
		return nil
	}
	raw, ok := sub.Stmts[0].RawStmt.(*pgquerypb.RawStmt)
	if !ok {
		return nil
	}
	return &pgquerypb.RangeSubselect{
		Subquery: raw.GetStmt(),
		Alias:    &pgquerypb.Alias{Aliasname: name},
	}
}

// referencesCatalogDirectly reports whether the statement reads one of the
// filtered catalogs in a way that only the table itself answers.
func (w *walker) referencesCatalogDirectly() bool {
	found := false
	visit(w.root, func(n *pgquerypb.Node) bool {
		cr := n.GetColumnRef()
		if cr == nil || found {
			return !found
		}
		fields := stringList(cr.GetFields())
		if len(fields) > 0 && systemColumns[strings.ToLower(fields[len(fields)-1])] {
			found = true
			return false
		}
		if len(fields) >= 3 {
			switch strings.ToLower(fields[0]) {
			case "information_schema", "pg_catalog":
				found = true
				return false
			}
		}
		return true
	})
	return found
}
