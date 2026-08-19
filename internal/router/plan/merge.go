package plan

import (
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Merge tells the executor how to combine the per-shard result streams of a
// read that runs on more than one shard.
type Merge struct {
	// ShardSQL is the statement text every shard runs; "" means the client's
	// text is sent unchanged.
	ShardSQL string
	// Hidden is the number of trailing columns the shard query returns for
	// ORDER BY expressions that are not in the select list; they are
	// stripped before rows reach the client.
	Hidden int
	// OrderBy is the merge order; empty means the streams are concatenated.
	OrderBy []SortKey
	// Limit and Offset are applied at the router; -1 means absent.
	Limit  int64
	Offset int64
	// Aggregates, when set, has one entry per result column: every shard
	// returns exactly one row and the router combines them into one.
	Aggregates []AggFunc
}

// SortKey is one ORDER BY key over the shard rows.
type SortKey struct {
	// Column indexes the shard row (hidden columns included).
	Column     int
	Desc       bool
	NullsFirst bool
	// CCollation is set when the key carries an explicit COLLATE "C" or
	// "POSIX"; text keys without it are refused at execution.
	CCollation bool
}

// AggFunc is a distributive aggregate the router combines across shards.
type AggFunc int

const (
	// AggCount sums per-shard counts.
	AggCount AggFunc = iota + 1
	// AggSum sums per-shard sums.
	AggSum
	// AggMin keeps the smallest per-shard minimum.
	AggMin
	// AggMax keeps the largest per-shard maximum.
	AggMax
)

var aggNames = map[string]AggFunc{"count": AggCount, "sum": AggSum, "min": AggMin, "max": AggMax}

// MultiShard reports how the executor may run the plan on several shards:
// the merge specification, or the refusal explaining why it cannot.
func (p Plan) MultiShard() (*Merge, error) {
	if p.mergeErr != nil {
		return nil, p.mergeErr
	}
	if p.merge == nil {
		return nil, notYet("multi-shard execution of this statement is not available yet", "filter on one shard key value")
	}
	return p.merge, nil
}

const hiddenPrefix = "__pgshard_sort_"

// mergeBuilder analyses the outermost SELECT of a single-table read.
type mergeBuilder struct {
	tree     proto.Message
	sel      *pgquerypb.SelectStmt
	shardKey string
	spec     Merge
	changed  bool
	// clone is the mutable copy of the tree, made on first change.
	clone  *pgquerypb.SelectStmt
	cloneT *pgquerypb.ParseResult
}

// buildMerge computes the Merge for sel; err is the refusal when the shape
// cannot be merged.
func buildMerge(tree proto.Message, sel *pgquerypb.SelectStmt, shardKey string, blockers []string) (*Merge, error) {
	if len(blockers) > 0 {
		return nil, notYet("multi-shard SELECT with "+strings.Join(blockers, ", ")+" is not available yet",
			"filter on one shard key value")
	}
	b := &mergeBuilder{tree: tree, sel: sel, shardKey: shardKey, spec: Merge{Limit: -1, Offset: -1}}
	if err := b.run(); err != nil {
		return nil, err
	}
	if b.changed {
		out, err := pgparser.Deparse(b.cloneT)
		if err != nil {
			return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: deparse of the shard query failed: %v", err)
		}
		b.spec.ShardSQL = out
	}
	return &b.spec, nil
}

func (b *mergeBuilder) mutable() *pgquerypb.SelectStmt {
	if b.clone == nil {
		b.cloneT = proto.Clone(b.tree).(*pgquerypb.ParseResult)
		b.clone = b.cloneT.GetStmts()[0].GetStmt().GetSelectStmt()
		b.changed = true
	}
	return b.clone
}

func (b *mergeBuilder) run() error {
	s := b.sel
	if s.GetLimitOption() == pgquerypb.LimitOption_LIMIT_OPTION_WITH_TIES {
		return notYet("multi-shard SELECT with FETCH ... WITH TIES is not available yet", "use LIMIT")
	}
	shardLocal := false
	if len(s.GetGroupClause()) > 0 {
		if !b.namesShardKey(s.GetGroupClause()) {
			return notYet("multi-shard GROUP BY without the shard key is not available yet",
				"group by the shard key \""+b.shardKey+"\" (every group then lives on one shard), or filter on one key value")
		}
		shardLocal = true
	}
	if len(s.GetDistinctClause()) > 0 {
		on := s.GetDistinctClause()
		if len(on) == 1 && on[0].GetNode() == nil {
			on = targetExprs(s.GetTargetList())
		}
		if !b.namesShardKey(on) {
			return notYet("multi-shard DISTINCT without the shard key is not available yet",
				"include the shard key \""+b.shardKey+"\" in the DISTINCT columns, or filter on one key value")
		}
		shardLocal = true
	}
	if s.GetHavingClause() != nil && !shardLocal {
		return notYet("multi-shard HAVING without GROUP BY on the shard key is not available yet", "")
	}
	aggregated := false
	for _, t := range s.GetTargetList() {
		if hasAggregate(t) {
			aggregated = true
			break
		}
	}
	if aggregated && !shardLocal {
		if err := b.aggregates(); err != nil {
			return err
		}
	}
	limit, offset, err := b.limits()
	if err != nil {
		return err
	}
	if len(b.spec.Aggregates) > 0 {
		b.spec.Limit, b.spec.Offset = limit, offset
		if s.GetLimitCount() != nil || s.GetLimitOffset() != nil {
			m := b.mutable()
			m.LimitCount, m.LimitOffset, m.LimitOption = nil, nil, pgquerypb.LimitOption_LIMIT_OPTION_DEFAULT
		}
		return nil
	}
	if err := b.orderBy(len(s.GetDistinctClause()) > 0); err != nil {
		return err
	}
	b.spec.Limit, b.spec.Offset = limit, offset
	switch {
	case limit >= 0:
		m := b.mutable()
		pushed := limit
		if offset > 0 {
			if pushed > math.MaxInt64-offset {
				pushed = math.MaxInt64
			} else {
				pushed += offset
			}
		}
		m.LimitCount = intConst(pushed)
		m.LimitOffset = nil
		m.LimitOption = pgquerypb.LimitOption_LIMIT_OPTION_COUNT
	case offset >= 0:
		m := b.mutable()
		m.LimitOffset = nil
	}
	return nil
}

func intConst(v int64) *pgquerypb.Node {
	if v > math.MaxInt32 {
		return &pgquerypb.Node{Node: &pgquerypb.Node_AConst{AConst: &pgquerypb.A_Const{Val: &pgquerypb.A_Const_Fval{Fval: &pgquerypb.Float{Fval: fmt.Sprint(v)}}}}}
	}
	return &pgquerypb.Node{Node: &pgquerypb.Node_AConst{AConst: &pgquerypb.A_Const{Val: &pgquerypb.A_Const_Ival{Ival: &pgquerypb.Integer{Ival: int32(v)}}}}}
}

func targetExprs(targets []*pgquerypb.Node) []*pgquerypb.Node {
	out := make([]*pgquerypb.Node, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.GetResTarget().GetVal())
	}
	return out
}

// namesShardKey reports whether one of exprs is a plain reference to the
// shard key column (or an integer position of one in the select list).
func (b *mergeBuilder) namesShardKey(exprs []*pgquerypb.Node) bool {
	for _, e := range exprs {
		if e.GetGroupingSet() != nil {
			return false
		}
		if b.isShardKeyRef(e) {
			return true
		}
		if c := e.GetAConst(); c != nil {
			if iv, ok := c.GetVal().(*pgquerypb.A_Const_Ival); ok {
				n := int(iv.Ival.GetIval())
				targets := b.sel.GetTargetList()
				if n >= 1 && n <= len(targets) && b.isShardKeyRef(targets[n-1].GetResTarget().GetVal()) {
					return true
				}
			}
		}
	}
	return false
}

func (b *mergeBuilder) isShardKeyRef(e *pgquerypb.Node) bool {
	cr := e.GetColumnRef()
	if cr == nil {
		return false
	}
	fields := stringList(cr.GetFields())
	return len(fields) > 0 && len(fields) == len(cr.GetFields()) && fields[len(fields)-1] == b.shardKey
}

// aggregates validates an aggregate-only select list without GROUP BY.
func (b *mergeBuilder) aggregates() error {
	for _, t := range b.sel.GetTargetList() {
		fc := t.GetResTarget().GetVal().GetFuncCall()
		if fc == nil {
			return notYet("multi-shard aggregates must be top-level: expressions around or beside an aggregate are not available yet",
				"select only count(*), count(x), sum(x), min(x) or max(x), or filter on one shard key value")
		}
		names := stringList(fc.GetFuncname())
		if len(names) == 2 && names[0] == "pg_catalog" {
			names = names[1:]
		}
		fn := aggNames[strings.ToLower(names[len(names)-1])]
		if len(names) != 1 || fn == 0 {
			return notYet("multi-shard "+strings.ToLower(names[len(names)-1])+"() is not available yet",
				"only count, sum, min and max combine across shards; avg(x) can be computed from sum(x) and count(x)")
		}
		if fc.GetAggDistinct() || fc.GetAggFilter() != nil || len(fc.GetAggOrder()) > 0 || fc.GetOver() != nil || fc.GetAggWithinGroup() {
			return notYet("multi-shard aggregates with DISTINCT, FILTER, ORDER BY or OVER are not available yet", "")
		}
		if fn == AggCount && !fc.GetAggStar() && len(fc.GetArgs()) != 1 || fn != AggCount && len(fc.GetArgs()) != 1 {
			return notYet("multi-shard aggregate with an unexpected argument list is not available yet", "")
		}
		for _, a := range fc.GetArgs() {
			if hasAggregate(a) {
				return notYet("nested aggregates cannot run on multiple shards", "")
			}
		}
		b.spec.Aggregates = append(b.spec.Aggregates, fn)
	}
	return nil
}

func (b *mergeBuilder) limits() (limit, offset int64, err error) {
	limit, err = b.limitValue(b.sel.GetLimitCount(), "LIMIT")
	if err != nil {
		return 0, 0, err
	}
	offset, err = b.limitValue(b.sel.GetLimitOffset(), "OFFSET")
	if err != nil {
		return 0, 0, err
	}
	return limit, offset, nil
}

func (b *mergeBuilder) limitValue(node *pgquerypb.Node, what string) (int64, error) {
	if node == nil {
		return -1, nil
	}
	c := node.GetAConst()
	if c == nil {
		return 0, notYet("multi-shard "+what+" must be an integer constant", "parameters and expressions in "+what+" are not available across shards yet")
	}
	if c.GetIsnull() {
		return -1, nil
	}
	var v int64
	switch x := c.GetVal().(type) {
	case *pgquerypb.A_Const_Ival:
		v = int64(x.Ival.GetIval())
	case *pgquerypb.A_Const_Fval:
		i, err := parseInt(x.Fval.GetFval())
		if err != nil {
			return 0, notYet("multi-shard "+what+" must be an integer constant", "")
		}
		v = i
	default:
		return 0, notYet("multi-shard "+what+" must be an integer constant", "")
	}
	if v < 0 {
		return 0, pgwire.Errorf("2201W", "%s must not be negative", what)
	}
	return v, nil
}

// orderBy resolves every sort key to a column of the shard row, adding
// hidden columns for expressions the select list does not carry.
func (b *mergeBuilder) orderBy(distinct bool) error {
	targets := b.sel.GetTargetList()
	for i, sb := range b.sel.GetSortClause() {
		sort := sb.GetSortBy()
		if sort.GetSortbyDir() == pgquerypb.SortByDir_SORTBY_USING {
			return notYet("multi-shard ORDER BY ... USING is not available yet", "order by ASC or DESC")
		}
		key := SortKey{Desc: sort.GetSortbyDir() == pgquerypb.SortByDir_SORTBY_DESC}
		switch sort.GetSortbyNulls() {
		case pgquerypb.SortByNulls_SORTBY_NULLS_FIRST:
			key.NullsFirst = true
		case pgquerypb.SortByNulls_SORTBY_NULLS_LAST:
			key.NullsFirst = false
		default:
			key.NullsFirst = key.Desc
		}
		expr := sort.GetNode()
		match := expr
		if cc := expr.GetCollateClause(); cc != nil {
			key.CCollation = isCCollation(cc)
			// The column is looked up without the collation so ORDER BY
			// name COLLATE "C" orders the existing name column.
			match = cc.GetArg()
		}
		col, err := b.sortColumn(match, targets)
		if err != nil {
			return err
		}
		if col < 0 {
			if distinct {
				return notYet("multi-shard SELECT DISTINCT with ORDER BY expressions outside the select list is not available yet",
					"add the ORDER BY expression to the select list")
			}
			m := b.mutable()
			m.TargetList = append(m.TargetList, &pgquerypb.Node{Node: &pgquerypb.Node_ResTarget{ResTarget: &pgquerypb.ResTarget{
				Name: fmt.Sprintf("%s%d", hiddenPrefix, i), Val: proto.Clone(expr).(*pgquerypb.Node)}}})
			col = len(targets) + b.spec.Hidden
			b.spec.Hidden++
		}
		if col < len(targets) && isCCollation(targets[col].GetResTarget().GetVal().GetCollateClause()) {
			key.CCollation = true
		}
		key.Column = col
		b.spec.OrderBy = append(b.spec.OrderBy, key)
	}
	return nil
}

// sortColumn finds the select-list column expr refers to, PostgreSQL style:
// an integer position, an output column name, or an identical expression.
// -1 means the expression is not in the select list.
func (b *mergeBuilder) sortColumn(expr *pgquerypb.Node, targets []*pgquerypb.Node) (int, error) {
	if c := expr.GetAConst(); c != nil {
		iv, ok := c.GetVal().(*pgquerypb.A_Const_Ival)
		if !ok {
			return 0, pgwire.Errorf("42601", "non-integer constant in ORDER BY")
		}
		n := int(iv.Ival.GetIval())
		if n < 1 || n > len(targets) {
			return 0, pgwire.Errorf("42P10", "ORDER BY position %d is not in select list", n)
		}
		return n - 1, nil
	}
	if cr := expr.GetColumnRef(); cr != nil && len(cr.GetFields()) == 1 {
		if name := cr.GetFields()[0].GetString_().GetSval(); name != "" {
			for i, t := range targets {
				rt := t.GetResTarget()
				if rt.GetName() == name {
					return i, nil
				}
				if rt.GetName() == "" {
					if f := stringList(rt.GetVal().GetColumnRef().GetFields()); len(f) > 0 && f[len(f)-1] == name && len(f) == len(rt.GetVal().GetColumnRef().GetFields()) {
						return i, nil
					}
				}
			}
		}
	}
	for i, t := range targets {
		if proto.Equal(stripLocations(t.GetResTarget().GetVal()), stripLocations(expr)) {
			return i, nil
		}
	}
	return -1, nil
}

// stripLocations clears the token positions so structurally equal
// expressions compare equal.
func stripLocations(n *pgquerypb.Node) *pgquerypb.Node {
	c := proto.Clone(n).(*pgquerypb.Node)
	clearLocations(c.ProtoReflect())
	return c
}

func isCCollation(cc *pgquerypb.CollateClause) bool {
	if cc == nil {
		return false
	}
	names := stringList(cc.GetCollname())
	n := len(names)
	return n > 0 && (names[n-1] == "C" || names[n-1] == "POSIX")
}
