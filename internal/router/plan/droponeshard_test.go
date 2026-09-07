package plan

import (
	"context"
	"strings"
	"testing"
)

// Everything DROP did not name explicitly fell through to the home shard.
// A database exists in every group, so the objects inside one do too: a
// function dropped on shard 0 was left standing on every other shard, and
// nothing said so. Seventeen object types took that path.
//
// pgshard cannot fan these out, because it never created them -- CREATE
// FUNCTION, CREATE AGGREGATE and CREATE EXTENSION are all refused here. So
// the honest answer is the one the CREATE gives: not through the router.
func TestADropThePlannerCannotFanOutIsRefused(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"drop function f(int)",
		"drop aggregate agg(int)",
		"drop extension postgis",
		"drop domain d",
		"drop operator + (int, int)",
		"drop collation c",
		"drop cast (int as text)",
		"drop text search configuration c",
		"drop server s",
		"drop publication p",
		"drop statistics s",
	} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil {
			t.Errorf("%s: planned as %v over %v; the other shards keep the object", sql, p.Kind, p.Shards)
			continue
		}
		if !strings.Contains(err.Error(), "not available through the router") {
			t.Errorf("%s: refused as %v", sql, err)
		}
	}
}

// An object that belongs to a table lives where the table does. Sending
// these to the home shard removed a sharded table's trigger from shard 0
// and left it on every other one; for a row-level security policy that is a
// table protected on some shards and not others, with no error anywhere.
func TestDroppingAnObjectOnATableFollowsTheTable(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"drop trigger t on orders", ScopeAll},
		{"drop policy p on orders", ScopeAll},
		{"drop rule r on orders", ScopeAll},
		// An unsharded table has one copy, on the home shard, and so does
		// anything attached to it.
		{"drop trigger t on settings", ScopeHome},
		{"drop policy p on settings", ScopeHome},
		// Schema-qualified, so an off-by-one in the name indexing looks up
		// the wrong relation and gets the wrong scope.
		{"drop trigger t on public.orders", ScopeAll},
		{"drop policy p on public.settings", ScopeHome},
	} {
		p, err := New().Plan(context.Background(), session(snap), c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if p.Migration == nil {
			t.Errorf("%s: planned as %v with no migration, so it reaches one shard", c.sql, p.Kind)
			continue
		}
		if got := p.Migration.Scope; got != c.want {
			t.Errorf("%s: scope %q, want %q", c.sql, got, c.want)
		}
	}
}

// Renaming has the same two rules as dropping, and had the same defect: an
// object attached to a table follows the table, and an object pgshard did
// not create is refused rather than renamed on one shard. ALTER TABLE ...
// RENAME CONSTRAINT is the one that matters -- a statement any migration
// tool emits, which renamed a sharded table's constraint on shard 0 and
// left every other shard holding the old name.
func TestRenamingFollowsTheSameRulesAsDropping(t *testing.T) {
	snap := fixture(t)
	for _, c := range []struct{ sql, want string }{
		{"alter trigger t on orders rename to u", ScopeAll},
		{"alter policy p on orders rename to q", ScopeAll},
		{"alter rule r on orders rename to q", ScopeAll},
		{"alter table orders rename constraint a to b", ScopeAll},
		{"alter table settings rename constraint a to b", ScopeHome},
	} {
		p, err := New().Plan(context.Background(), session(snap), c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if p.Migration == nil {
			t.Errorf("%s: planned as %v with no migration, so it reaches one shard", c.sql, p.Kind)
			continue
		}
		if got := p.Migration.Scope; got != c.want {
			t.Errorf("%s: scope %q, want %q", c.sql, got, c.want)
		}
	}
	// And the objects that exist in every group, which pgshard never
	// created and cannot fan out.
	for _, sql := range []string{
		"alter function f(int) rename to g",
		"alter aggregate agg(int) rename to agg2",
		"alter collation c rename to d",
		"alter statistics st rename to st2",
		"alter function f(int) owner to bob",
		"alter function f(int) set schema s",
		"alter domain d owner to bob",
	} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil {
			t.Errorf("%s: planned as %v over %v; the other shards keep the old name", sql, p.Kind, p.Shards)
			continue
		}
		if !strings.Contains(err.Error(), "not available through the router") {
			t.Errorf("%s: refused as %v", sql, err)
		}
		// The refusal must name what the user typed, not the table case
		// the shared helper is written for.
		if strings.Contains(err.Error(), "ALTER TABLE") {
			t.Errorf("%s: refused as %v, which names a table", sql, err)
		}
	}
}
