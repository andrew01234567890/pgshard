package plan

import (
	"context"
	"strings"
	"testing"
)

func pinnedPlan(t *testing.T, shard int32, sql string) (Plan, error) {
	t.Helper()
	sess := session(fixture(t))
	sess.PinnedShard = &shard
	return New().Plan(context.Background(), sess, sql)
}

// An operator debugging a shard has, until now, had to connect to its
// PostgreSQL directly -- which means holding a credential that reaches a
// shard with none of the router's refusals in front of it. That is the
// credential the router exists to avoid handing out.
func TestAPinnedSessionReadsTheShardItNamed(t *testing.T) {
	for _, sql := range []string{
		"select * from orders",
		"select * from orders where tenant_id = 1",
		"insert into orders (tenant_id, id) values (1, 2)",
		"update orders set amount = 1 where tenant_id = 1",
		"delete from orders where tenant_id = 1",
	} {
		p, err := pinnedPlan(t, 2, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if p.Kind != Pinned {
			t.Errorf("%s: kind %v, want Pinned", sql, p.Kind)
		}
		if len(p.Shards) != 1 || p.Shards[0] != 2 {
			t.Errorf("%s: shards %v, want [2]", sql, p.Shards)
		}
	}
}

// A keyed read that would have gone elsewhere goes to the pin: that is the
// point, and it is also why the setting is an operator's.
func TestAPinOverridesTheShardMap(t *testing.T) {
	unpinned, err := New().Plan(context.Background(), session(fixture(t)), "select * from orders where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(unpinned.Shards) != 1 {
		t.Fatalf("the fixture no longer routes this to one shard: %v", unpinned.Shards)
	}
	other := unpinned.Shards[0] + 1
	if other > 3 {
		other = 0
	}
	p, err := pinnedPlan(t, other, "select * from orders where tenant_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Shards) != 1 || p.Shards[0] != other {
		t.Fatalf("shards %v, want the pin %d rather than the map's %d", p.Shards, other, unpinned.Shards[0])
	}
}

// A schema change runs across the cluster, not on one shard, so it is
// refused rather than quietly applied to the pin -- which would leave the
// other shards behind with nothing saying so.
func TestAPinnedSessionRefusesWhatCannotMeanOneShard(t *testing.T) {
	for _, c := range []struct{ sql, msg string }{
		{"create table t (id int)", "schema change cannot run while"},
		{"alter table orders add column x int", "schema change cannot run while"},
		{"drop table orders", "schema change cannot run while"},
		{"select nextval('invoice_numbers')", "nextval() over a global sequence cannot run while"},
	} {
		_, err := pinnedPlan(t, 1, c.sql)
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: %v, want %q", c.sql, err, c.msg)
		}
	}
}

// Session and transaction control keep taking the ordinary path, or a
// pinned session could not RESET the pin.
func TestAPinnedSessionStillRunsSessionControl(t *testing.T) {
	for _, sql := range []string{"begin", "commit", "set search_path = public", "show search_path", "reset pgshard.shard"} {
		p, err := pinnedPlan(t, 1, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if p.Kind != SessionLocal {
			t.Errorf("%s: kind %v, want SessionLocal", sql, p.Kind)
		}
	}
}

// Naming a shard that is not serving is a mistake worth an error, not a
// statement sent nowhere.
func TestAPinMustNameAServingShard(t *testing.T) {
	_, err := pinnedPlan(t, 99, "select * from orders")
	if err == nil || !strings.Contains(err.Error(), "not serving in this shard set") {
		t.Fatalf("%v, want a refusal naming the shard set", err)
	}
}

func TestParseShardPin(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int32
		ok   bool
	}{
		{"0", 0, true}, {"3", 3, true}, {" 7 ", 7, true},
	} {
		got, err := ParseShardPin(c.in)
		if err != nil || got == nil || *got != c.want {
			t.Errorf("%q: %v %v", c.in, got, err)
		}
	}
	if got, err := ParseShardPin(""); err != nil || got != nil {
		t.Errorf("empty is a RESET: %v %v", got, err)
	}
	for _, bad := range []string{"shard-a", "-1", "1.5", "one"} {
		if _, err := ParseShardPin(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

// A reference write belongs on every shard, in one transaction: ADR 16
// makes that atomicity a property pgshard does not trade away. Pinning
// would write exactly one copy, and a join answered locally on any shard
// would then return a different answer depending on where it ran, with
// nothing reporting it.
func TestAPinnedSessionRefusesAReferenceWrite(t *testing.T) {
	for _, sql := range []string{
		"insert into regions (id, name) values (7, 'eu')",
		"update regions set name = 'eu' where id = 7",
		"delete from regions where id = 7",
	} {
		_, err := pinnedPlan(t, 1, sql)
		if err == nil || !strings.Contains(err.Error(), "reference table cannot run while") {
			t.Errorf("%s: %v, want the reference-write refusal", sql, err)
		}
	}
	// Reading one is fine: that is what a pinned session is for.
	if _, err := pinnedPlan(t, 1, "select * from regions"); err != nil {
		t.Errorf("a pinned reference READ is the point of the setting: %v", err)
	}
}
