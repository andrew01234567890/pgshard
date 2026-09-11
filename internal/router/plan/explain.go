package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	pgquerypb "github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// ExplainOption is the EXPLAIN option that asks for the ROUTER's plan
// rather than a shard's.
//
// A plain EXPLAIN is planned and routed like any other statement, so it
// reaches a shard and comes back with that shard's PostgreSQL plan. That
// answers "what did one shard do with the SQL it was given" and never
// "which shards was it given to, and why" -- which is the question a user
// of a sharded database actually has when a query is slow.
const ExplainOption = "pgshard"

// explainsRouting reports whether EXPLAIN carries the pgshard option, which
// PostgreSQL spells like every other boolean EXPLAIN option: bare means on,
// and an explicit false means off.
func explainsRouting(e *pgquerypb.ExplainStmt) bool {
	for _, o := range e.GetOptions() {
		d := o.GetDefElem()
		if d == nil || !strings.EqualFold(d.GetDefname(), ExplainOption) {
			continue
		}
		return optionIsOn(d)
	}
	return false
}

// optionIsOn reads a boolean EXPLAIN option the way defGetBoolean does: no
// argument is true, and TRUE/ON/1 in any of the spellings the grammar puts
// them in is true.
//
// The grammar hands the argument over as a bare String or Integer node
// rather than an A_Const, so `(pgshard on)` and `(pgshard 1)` arrive in two
// different shapes and neither is the shape a value expression has.
func optionIsOn(d *pgquerypb.DefElem) bool {
	arg := d.GetArg()
	switch {
	case arg == nil:
		return true
	case arg.GetString_() != nil:
		return isTrueWord(arg.GetString_().GetSval())
	case arg.GetInteger() != nil:
		return arg.GetInteger().GetIval() != 0
	case arg.GetBoolean() != nil:
		return arg.GetBoolean().GetBoolval()
	}
	return false
}

func isTrueWord(s string) bool {
	switch strings.ToLower(s) {
	case "true", "on", "yes", "t", "y", "1":
		return true
	}
	return false
}

// refuseOtherExplainOptions refuses pgshard combined with any other EXPLAIN
// option.
//
// ANALYZE is the one that matters: it promises the statement ran, and this
// EXPLAIN never runs it, so accepting the pair would report timings for
// something that did not happen. The rest (COSTS, BUFFERS, FORMAT, ...)
// describe a PostgreSQL plan, and this output is not one.
func refuseOtherExplainOptions(e *pgquerypb.ExplainStmt) error {
	for _, o := range e.GetOptions() {
		d := o.GetDefElem()
		if d == nil || strings.EqualFold(d.GetDefname(), ExplainOption) {
			continue
		}
		err := notYet(fmt.Sprintf("EXPLAIN (%s) does not accept the %s option: it reports the router's routing decision, not a PostgreSQL plan, and it does not run the statement",
			ExplainOption, strings.ToUpper(d.GetDefname())),
			fmt.Sprintf("run EXPLAIN (%s) on its own to see the routing, and a plain EXPLAIN to see what one shard would do", ExplainOption))
		return err
	}
	return nil
}

// explainRouting plans the inner statement and renders the routing decision
// instead of running it. The statement is never executed: EXPLAIN without
// ANALYZE does not run its argument, and the router has nothing to measure
// without doing so.
//
// It runs the whole planning pipeline over the inner statement rather than
// walking it in place, because a refusal is the routing decision a user most
// needs to see, and several of the refusals are raised before the walker is
// ever reached. Rendering only the ones the walker raises would leave
// EXPLAIN aborting on exactly the statements it was asked about.
func (p *Planner) explainRouting(ctx context.Context, sess Session, version int32, e *pgquerypb.ExplainStmt) (Plan, error) {
	if err := refuseOtherExplainOptions(e); err != nil {
		return refusalErr(err)
	}
	out := sess.session()
	sql, err := pgparser.Deparse(&pgquerypb.ParseResult{Version: version, Stmts: []*pgquerypb.RawStmt{{Stmt: e.GetQuery()}}})
	if err != nil {
		return out, err
	}
	sub, subErr := p.plan(ctx, sess, sql, false)
	out.Explain = renderPlan(&sub, subErr)
	return out, nil
}

// renderPlan describes a plan in the terms the router decided it in.
func renderPlan(p *Plan, err error) []string {
	if err != nil {
		return []string{"Refused", "  " + err.Error(), "",
			"The statement was not run. A refusal is the routing decision:",
			"pgshard would rather answer nothing than answer from some of the shards."}
	}
	out := []string{
		"Route [" + p.Kind.String() + "]",
		"  Shards: " + shardsLabel(p),
		"  Fanout: " + fanoutLabel(p.Fanout()),
	}
	for _, t := range p.Tables {
		out = append(out, fmt.Sprintf("  Table:  %s.%s", t.SchemaName, t.TableName))
	}
	// Only where the merge is reached: every read carries a merge spec,
	// and describing one on a plan that runs on a single shard says the
	// router combines results it never receives.
	if len(p.Shards) > 1 || p.Kind == Scatter {
		m, merr := p.MultiShard()
		switch {
		case merr != nil:
			out = append(out, "  Merge:  refused at execution -- "+merr.Error())
		case m != nil:
			out = append(out, "  Merge:  "+mergeLabel(m))
		}
	}
	if p.Rewritten != "" {
		out = append(out, "  Rewritten: "+p.Rewritten)
	}
	return out
}

func shardsLabel(p *Plan) string {
	switch {
	case p.Deferred:
		return "decided at Bind, from the parameter that carries the shard key"
	case p.Kind == Reference && len(p.Shards) == 0:
		return "one serving shard, chosen at execution: the table is the same on every shard"
	case len(p.Shards) == 0:
		return "none: the router answers this itself"
	}
	ids := make([]string, len(p.Shards))
	for i, s := range p.Shards {
		ids[i] = fmt.Sprint(s)
	}
	return strings.Join(ids, ", ")
}

func mergeLabel(m *Merge) string {
	var parts []string
	switch {
	case len(m.Aggregates) > 0:
		parts = append(parts, "each shard returns one row and the router combines them")
	case len(m.OrderBy) > 0:
		parts = append(parts, "the shards' rows are merged in ORDER BY order")
	default:
		parts = append(parts, "the shards' rows are concatenated, in no particular order")
	}
	if m.Limit >= 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d applied at the router", m.Limit))
	}
	if m.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d applied at the router", m.Offset))
	}
	return strings.Join(parts, "; ")
}

func fanoutLabel(f string) string {
	if f == FanoutExempt {
		return "not applicable"
	}
	return f
}

func parseVersion(tree any) int32 {
	if pr, ok := tree.(*pgquerypb.ParseResult); ok {
		return pr.GetVersion()
	}
	return 0
}
