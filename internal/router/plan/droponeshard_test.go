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
