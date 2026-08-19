package router

import (
	"context"
	"errors"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgparser"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Shard names one shard of one shard set.
type Shard struct {
	Set string
	ID  int32
}

// CatalogShardSet is the pseudo shard set that fronts the catalog database.
const CatalogShardSet = "catalog"

// DefaultShardSet is the shard set unsharded databases live in.
const DefaultShardSet = "default"

// StmtClass is what the planner learned about a statement that the session
// needs to track beyond routing.
type StmtClass struct {
	// SetGUC is set for a session-level SET/RESET; Name is the GUC ("" for
	// RESET ALL).
	SetGUC  bool
	GUCName string
}

// Plan is the routing decision for one statement.
type Plan struct {
	Shards []Shard
	Class  StmtClass
}

// Planner decides which shards a statement touches. Today every statement of
// a session goes to its home shard; the seam exists so sharded planning can
// replace it without touching the executor.
type Planner struct {
	parser *pgparser.Parser
}

// NewPlanner builds a Planner with a bounded parse cache.
func NewPlanner() *Planner {
	return &Planner{parser: pgparser.New(pgparser.Options{CacheEntries: 4096, CacheBytes: 32 << 20})}
}

const cursorOptHold = 0x0020

// Plan classifies sql and resolves its shards. Statements that cannot work
// on a single shard through the router are refused with 0A000. Text the
// bound grammar cannot parse is forwarded unclassified so the backend
// reports the error itself.
func (p *Planner) Plan(ctx context.Context, home Shard, sql string) (Plan, error) {
	plan := Plan{Shards: []Shard{home}}
	res, err := p.parser.Parse(ctx, sql)
	if err != nil {
		var perr *pgparser.Error
		if errors.As(err, &perr) && perr.SQLState != pgparser.SyntaxErrorSQLState {
			return Plan{}, pgwire.Errorf(perr.SQLState, "%s", perr.Message)
		}
		return plan, nil
	}
	for _, st := range res.Stmts {
		raw, ok := st.RawStmt.(*pgquerypb.RawStmt)
		if !ok {
			continue
		}
		if err := classify(raw.GetStmt(), &plan.Class); err != nil {
			return Plan{}, err
		}
	}
	return plan, nil
}

func classify(node *pgquerypb.Node, c *StmtClass) error {
	switch n := node.GetNode().(type) {
	case *pgquerypb.Node_ListenStmt:
		return refuse("LISTEN is not supported through the router")
	case *pgquerypb.Node_NotifyStmt:
		return refuse("NOTIFY is not supported through the router")
	case *pgquerypb.Node_UnlistenStmt:
		return refuse("UNLISTEN is not supported through the router")
	case *pgquerypb.Node_DeclareCursorStmt:
		if n.DeclareCursorStmt.GetOptions()&cursorOptHold != 0 {
			return refuse("WITH HOLD cursors are not supported through the router")
		}
	case *pgquerypb.Node_CreateStmt:
		if n.CreateStmt.GetRelation().GetRelpersistence() == "t" {
			return refuse("temporary tables are not supported through the router")
		}
	case *pgquerypb.Node_CreateTableAsStmt:
		if n.CreateTableAsStmt.GetInto().GetRel().GetRelpersistence() == "t" {
			return refuse("temporary tables are not supported through the router")
		}
	case *pgquerypb.Node_VariableSetStmt:
		s := n.VariableSetStmt
		if s.GetIsLocal() {
			return nil
		}
		switch s.GetKind() {
		case pgquerypb.VariableSetKind_VAR_SET_VALUE, pgquerypb.VariableSetKind_VAR_SET_DEFAULT,
			pgquerypb.VariableSetKind_VAR_SET_CURRENT, pgquerypb.VariableSetKind_VAR_RESET:
			c.SetGUC, c.GUCName = true, strings.ToLower(s.GetName())
		case pgquerypb.VariableSetKind_VAR_RESET_ALL:
			c.SetGUC, c.GUCName = true, ""
		case pgquerypb.VariableSetKind_VAR_SET_MULTI:
			if strings.EqualFold(s.GetName(), "SESSION CHARACTERISTICS") {
				c.SetGUC, c.GUCName = true, "session characteristics"
			}
		}
	}
	return nil
}

func refuse(msg string) error {
	return pgwire.Errorf(pgwire.CodeFeatureNotSupported, "%s", msg)
}
