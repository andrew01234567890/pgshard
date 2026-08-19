package router

import (
	"context"

	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// Shard names one shard of one shard set.
type Shard struct {
	Set string
	ID  int32
}

// CatalogShardSet is the pseudo shard set that fronts the catalog database.
const CatalogShardSet = "catalog"

// DefaultShardSet is the shard set unsharded databases live in.
const DefaultShardSet = plan.DefaultShardSet

// StmtClass is what the planner learned about a statement that the session
// needs to track beyond routing.
type StmtClass = plan.StmtClass

// Planner decides which shards a statement touches (see package plan).
type Planner struct {
	inner *plan.Planner
	// before, when set, runs ahead of every Plan call (tests use it to
	// inject faults).
	before func(sql string)
}

// NewPlanner builds a Planner with a bounded parse cache.
func NewPlanner() *Planner { return &Planner{inner: plan.New()} }

// Plan plans sql for a session of the router; sessions on the catalog shard
// set plan every statement onto the catalog shard.
func (p *Planner) Plan(ctx context.Context, sess plan.Session, sql string) (plan.Plan, error) {
	if p.before != nil {
		p.before(sql)
	}
	return p.inner.Plan(ctx, sess, sql)
}
