package plan

import (
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// scatterUnionAll plans a UNION ALL whose arms each read one sharded table.
//
// Every shard runs the whole statement over its own rows, and the router
// concatenates what comes back, or merges it under the statement's ORDER BY
// and LIMIT. That is the answer a single server gives only because UNION
// ALL keeps every row: each row of each arm lives on exactly one shard, so
// each is returned exactly once. UNION, INTERSECT and EXCEPT compare rows,
// and a shard can compare only its own, so they stay refused -- and so does
// anything inside an arm that combines rows (an aggregate, DISTINCT, GROUP
// BY, a window, an arm's own LIMIT), for the same reason a single-table
// scatter refuses it.
func (w *walker) scatterUnionAll() error {
	for _, r := range w.rels {
		if r.kind != placeSharded {
			return notYet("multi-shard UNION ALL is available only when every arm reads a sharded table",
				"read reference and unsharded tables in a query of their own")
		}
	}
	if blockers := without(w.scatterBlockers, "set operations"); len(blockers) > 0 {
		return notYet("multi-shard UNION ALL with "+strings.Join(blockers, ", ")+" is not available yet",
			"filter on one shard key value")
	}
	top := w.setOp
	if top.GetWithClause() != nil {
		return notYet("multi-shard UNION ALL with a WITH clause is not available yet", "filter on one shard key value")
	}
	if len(top.GetLockingClause()) > 0 {
		return notYet("multi-shard UNION ALL with FOR UPDATE/SHARE is not available yet", "")
	}
	arms, err := unionArms(top, true, nil)
	if err != nil {
		return err
	}
	scalar := w.scalarFunctions()
	for _, arm := range arms {
		if err := plainArm(arm, scalar); err != nil {
			return err
		}
	}
	// The result columns are the leftmost arm's, and a set operation's
	// ORDER BY may name only those (PostgreSQL refuses an expression there
	// itself), so the merge is built over them with the statement's own
	// ORDER BY and LIMIT.
	sel := &pgquerypb.SelectStmt{
		TargetList:  arms[0].GetTargetList(),
		SortClause:  top.GetSortClause(),
		LimitCount:  top.GetLimitCount(),
		LimitOffset: top.GetLimitOffset(),
		LimitOption: top.GetLimitOption(),
	}
	b := &mergeBuilder{tree: w.tree, sel: sel, scalar: scalar, setOp: true, spec: Merge{Limit: -1, Offset: -1}}
	if err := b.runUnionAll(); err != nil {
		return err
	}
	if b.changed {
		out, err := pgparser.Deparse(b.cloneT)
		if err != nil {
			return pgwire.Errorf(pgwire.CodeInternalError, "router: deparse of the shard query failed: %v", err)
		}
		b.spec.ShardSQL = out
	}
	p := w.plan
	p.merge = &b.spec
	p.Kind = Scatter
	p.Shards = w.allShards()
	return nil
}

// unionArms lists the SELECTs a tree of set operations combines, leftmost
// first, and refuses an inner set operation that carries its own ORDER BY,
// LIMIT, locking or WITH: "(a union all b order by 1 limit 3) union all c"
// would be limited by every shard on its own and concatenated, returning up
// to three rows per shard where one server returns three.
func unionArms(s *pgquerypb.SelectStmt, top bool, out []*pgquerypb.SelectStmt) ([]*pgquerypb.SelectStmt, error) {
	if s.GetOp() == pgquerypb.SetOperation_SETOP_NONE || s.GetOp() == pgquerypb.SetOperation_SET_OPERATION_UNDEFINED {
		return append(out, s), nil
	}
	if !top && (len(s.GetSortClause()) > 0 || s.GetLimitCount() != nil || s.GetLimitOffset() != nil ||
		len(s.GetLockingClause()) > 0 || s.GetWithClause() != nil) {
		return nil, notYet("multi-shard UNION ALL with ORDER BY, LIMIT or WITH on a parenthesised set operation inside it is not available yet",
			"move the ORDER BY and LIMIT to the outermost level")
	}
	out, err := unionArms(s.GetLarg(), false, out)
	if err != nil {
		return nil, err
	}
	return unionArms(s.GetRarg(), false, out)
}

// plainArm refuses an arm whose rows a shard cannot produce on its own.
func plainArm(s *pgquerypb.SelectStmt, scalar declared) error {
	refuse := func(what string) error {
		return notYet("multi-shard UNION ALL with "+what+" in an arm is not available yet",
			"each arm of a multi-shard UNION ALL has to be a plain SELECT over one sharded table")
	}
	switch {
	case s.GetWithClause() != nil:
		return refuse("a WITH clause")
	case len(s.GetDistinctClause()) > 0:
		return refuse("DISTINCT")
	case len(s.GetGroupClause()) > 0, s.GetHavingClause() != nil:
		return refuse("GROUP BY")
	case len(s.GetWindowClause()) > 0:
		return refuse("window functions")
	case len(s.GetSortClause()) > 0, s.GetLimitCount() != nil, s.GetLimitOffset() != nil:
		return refuse("ORDER BY or LIMIT")
	case len(s.GetLockingClause()) > 0:
		return refuse("FOR UPDATE/SHARE")
	case len(s.GetValuesLists()) > 0:
		return refuse("VALUES")
	case len(s.GetFromClause()) != 1 || s.GetFromClause()[0].GetRangeVar() == nil:
		return refuse("anything but one table in FROM")
	case hasSubLink(s.GetWhereClause()):
		return refuse("a subquery")
	}
	for _, t := range s.GetTargetList() {
		switch {
		case hasWindow(t):
			return refuse("window functions")
		case hasAggregate(t):
			return refuse("an aggregate")
		case hasSubLink(t):
			return refuse("a subquery")
		}
		if name := unknownFunction(t, scalar); name != "" {
			return notYet("multi-shard "+name+"() is not available yet: it is not a PostgreSQL built-in, and a user-defined aggregate cannot be told from a scalar function by name",
				"declare it in pgshard.functions, or filter on one shard key value")
		}
	}
	return nil
}

// runUnionAll is run for the merge of a UNION ALL: its ORDER BY and LIMIT,
// with the LIMIT pushed down to every shard as the single-table merge does.
func (b *mergeBuilder) runUnionAll() error {
	if b.sel.GetLimitOption() == pgquerypb.LimitOption_LIMIT_OPTION_WITH_TIES {
		return notYet("multi-shard SELECT with FETCH ... WITH TIES is not available yet", "use LIMIT")
	}
	limit, offset, err := b.limits()
	if err != nil {
		return err
	}
	if err := b.orderBy(false); err != nil {
		return err
	}
	b.spec.Limit, b.spec.Offset = limit, offset
	b.pushLimit(limit, offset)
	return nil
}
