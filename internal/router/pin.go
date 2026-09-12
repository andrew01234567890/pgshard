package router

import (
	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// pinnedShard is the session's effective pgshard.shard, or nil when the
// session routes normally. Read from the session's GUC list for the same
// reason fanoutCeiling is: that list is what survives a backend change, and
// a second copy of the value would be a second thing to keep in step.
func (e *Executor) pinnedShard() *int32 {
	var pin *int32
	for _, list := range [][]gucEntry{e.gucs, e.staged} {
		for _, g := range list {
			if g.name != plan.ShardGUC {
				continue
			}
			// A value that does not parse cannot reach here: checkShardPin
			// refuses the SET before it is recorded.
			id, err := plan.ParseShardPin(gucValueOf(g.sql))
			if err != nil {
				continue
			}
			pin = id
		}
	}
	return pin
}

// checkShardPin validates a SET of pgshard.shard before it is recorded.
//
// Targeting one shard reaches past the shard map, so unlike every other
// client-facing pgshard setting it WIDENS what a session can see: a session
// that can pin its own shard can read rows the map would have routed it
// away from. It is therefore an operator's tool and is refused to anyone
// who does not hold pgshard_admin.
//
// It cannot change inside a transaction either. A transaction that began
// against one routing and continued against another would have statements
// from both in it, and the parts already sent cannot be taken back.
func (e *Executor) checkShardPin(class StmtClass) error {
	if !class.SetGUC || class.GUCName != plan.ShardGUC {
		return nil
	}
	if _, err := plan.ParseShardPin(class.GUCValue); err != nil {
		return err
	}
	if e.r.cfg.CatalogAccess == nil || !e.r.cfg.CatalogAccess.MayAdminister(e.info.User) {
		err := pgwire.Errorf(pgwire.CodeInsufficientPrivilege,
			"permission denied to set %s: targeting one shard reaches past the shard map", plan.ShardGUC)
		err.Hint = "it is an operator's tool and needs membership of " + catalog.RoleAdmin
		return err
	}
	if e.inClientTransaction() {
		err := pgwire.Errorf(codeInvalidParamVal, "%s cannot be changed inside a transaction", plan.ShardGUC)
		err.Hint = "set it before BEGIN, or commit first: statements already sent cannot be re-routed"
		return err
	}
	return nil
}
