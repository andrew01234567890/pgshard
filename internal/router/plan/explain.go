package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgwire"

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

// explainRequest is what an EXPLAIN says about the pgshard option.
type explainRequest int

const (
	// explainPlain does not mention it, and is routed to a shard as it is.
	explainPlain explainRequest = iota
	// explainRouting asks the router for its own plan.
	explainRouting
	// explainDeclined names the option and turns it off, which asks for
	// what PostgreSQL means by EXPLAIN. The option is ours, so it has to
	// come off the statement before a shard sees it.
	explainDeclined
)

// explainsRouting reads the pgshard option, which PostgreSQL spells like
// every other boolean EXPLAIN option: bare means on, and an explicit false
// means off.
//
// The LAST spelling wins, because that is what defGetBoolean does with a
// repeated option and the user writing (pgshard true, pgshard false) is
// entitled to the answer PostgreSQL would have given.
func explainsRouting(e *pgquerypb.ExplainStmt) (explainRequest, error) {
	out := explainPlain
	for _, o := range e.GetOptions() {
		d := o.GetDefElem()
		if d == nil || !strings.EqualFold(d.GetDefname(), ExplainOption) {
			continue
		}
		on, err := optionIsOn(d)
		if err != nil {
			return out, err
		}
		if on {
			out = explainRouting
		} else {
			out = explainDeclined
		}
	}
	return out, nil
}

// withoutExplainOption is the statement with every pgshard option removed,
// so a shard is asked something it recognises.
//
// EXPLAIN (PGSHARD FALSE) means "do what PostgreSQL means by EXPLAIN", and
// forwarding it verbatim got the user `unrecognized EXPLAIN option
// "pgshard"` -- an error about a form we define, from a server that has
// never heard of it.
func withoutExplainOption(e *pgquerypb.ExplainStmt) *pgquerypb.Node {
	kept := make([]*pgquerypb.Node, 0, len(e.GetOptions()))
	for _, o := range e.GetOptions() {
		if d := o.GetDefElem(); d != nil && strings.EqualFold(d.GetDefname(), ExplainOption) {
			continue
		}
		kept = append(kept, o)
	}
	return &pgquerypb.Node{Node: &pgquerypb.Node_ExplainStmt{
		ExplainStmt: &pgquerypb.ExplainStmt{Query: e.GetQuery(), Options: kept},
	}}
}

// optionIsOn reads a boolean EXPLAIN option exactly as defGetBoolean does:
// no argument is true, the integers 0 and 1, and the words true, false, on
// and off in any case. Anything else is the error defGetBoolean raises.
//
// The error has to be raised here rather than left to a shard. Every other
// EXPLAIN option is PostgreSQL's, so a shard rejects what it cannot read;
// this one is ours, and the statement carrying it either never leaves the
// router or leaves with the option stripped off. Treating an unreadable
// value as false would run the statement the user did not ask for.
//
// The grammar hands the argument over as a bare String or Integer node
// rather than an A_Const, so `(pgshard on)` and `(pgshard 1)` arrive in two
// different shapes and neither is the shape a value expression has.
func optionIsOn(d *pgquerypb.DefElem) (bool, error) {
	arg := d.GetArg()
	switch {
	case arg == nil:
		return true, nil
	case arg.GetString_() != nil:
		switch strings.ToLower(arg.GetString_().GetSval()) {
		case "true", "on":
			return true, nil
		case "false", "off":
			return false, nil
		}
	case arg.GetInteger() != nil:
		switch arg.GetInteger().GetIval() {
		case 1:
			return true, nil
		case 0:
			return false, nil
		}
	case arg.GetBoolean() != nil:
		return arg.GetBoolean().GetBoolval(), nil
	}
	return false, notBoolean()
}

// notBoolean is defGetBoolean's own refusal, message and SQLSTATE.
func notBoolean() *pgwire.Error {
	err := pgwire.Errorf(pgwire.CodeSyntaxError, "%s requires a Boolean value", ExplainOption)
	err.Hint = "write " + ExplainOption + ", " + ExplainOption + " false, or leave it out"
	return err
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

// planExplain plans the inner statement and renders the routing decision
// instead of running it. The statement is never executed: EXPLAIN without
// ANALYZE does not run its argument, and the router has nothing to measure
// without doing so.
//
// It runs the whole planning pipeline over the inner statement rather than
// walking it in place, because a refusal is the routing decision a user most
// needs to see, and several of the refusals are raised before the walker is
// ever reached. Rendering only the ones the walker raises would leave
// EXPLAIN aborting on exactly the statements it was asked about.
func (p *Planner) planExplain(ctx context.Context, sess Session, version int32, e *pgquerypb.ExplainStmt) (Plan, error) {
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
	out.ExplainParams = explainParamOIDs(e.GetQuery(), &sub)
	return out, nil
}

// explainParamOIDs types the parameters of the statement being explained.
//
// The count is what matters: a driver sends no values at all unless the
// server says how many it expects. The types are 0 -- the protocol's
// "unspecified", which every driver answers by inferring from the value it
// holds -- except where the sub-plan routes on a parameter and the catalog
// has recorded the shard key column's type, which is the type PostgreSQL
// would have inferred there.
//
// Claiming a type we have not determined would be worse than saying
// nothing: the values are never read here, but a driver that trusts a wrong
// one fails to encode a perfectly good argument.
func explainParamOIDs(q *pgquerypb.Node, sub *Plan) []uint32 {
	n := maxParam(q)
	if n == 0 {
		return nil
	}
	oids := make([]uint32, n)
	for _, t := range sub.terms {
		oid := inferredOID(t.keyType)
		if oid == 0 {
			continue
		}
		for _, ref := range t.params {
			if i := int(ref.Number) - 1; i >= 0 && i < len(oids) {
				oids[i] = oid
			}
		}
	}
	return oids
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
	switch {
	// A reference write never reaches the merge: it runs on every shard
	// inside one two-phase commit, and the lowest shard's rows and tag are
	// the ones the client sees. Its merge spec would report a refusal the
	// executor does not consult.
	case p.Kind == Reference && p.Class.Write && len(p.Shards) > 1:
		out = append(out, "  Write:  applied on every shard in one two-phase commit")
	// Only where the merge is reached: every read carries a merge spec,
	// and describing one on a plan that runs on a single shard says the
	// router combines results it never receives.
	case len(p.Shards) > 1 || p.Kind == Scatter:
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
	case len(p.Shards) == 0 && (p.NextVal != "" || p.Explain != nil):
		return "none: the router answers this itself"
	case len(p.Shards) == 0:
		// SessionLocal is not router-local: SET, EXECUTE and DECLARE are
		// forwarded to whichever shard the session is already on.
		return "the shard this session is on; the statement is forwarded there"
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
