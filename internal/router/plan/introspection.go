package plan

import (
	"strings"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
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
	case Refuse, SessionLocal:
		return nil
	}
	if !w.filterIntrospection(w.root) {
		return nil
	}
	sql, err := pgparser.Deparse(w.tree)
	if err != nil {
		return pgwire.Errorf(pgwire.CodeInternalError, "filtering pgshard's own columns out of introspection: %v", err)
	}
	w.plan.Rewritten = sql
	return nil
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
		if s := n.GetSelectStmt(); s != nil {
			collectFromItems(s.GetFromClause(), &targets)
		}
		return true
	})
	for _, item := range targets {
		item.Node = &pgquerypb.Node_RangeSubselect{RangeSubselect: reservedFilterFor(item.GetRangeVar())}
	}
	return len(targets) > 0
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
