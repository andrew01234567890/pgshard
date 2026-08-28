package controller

import (
	"math"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

func TestKeyHashExprMirrorsPlacement(t *testing.T) {
	seed := "8816678312871386365::int8"
	cases := []struct{ col, typ, want string }{
		{"tenant_id", "bigint", `hashint8extended("tenant_id"::int8, ` + seed + `)`},
		{"id", "integer", `hashint8extended("id"::int8, ` + seed + `)`},
		{"id", "smallint", `hashint8extended("id"::int8, ` + seed + `)`},
		{"slug", "text", `hashtextextended("slug"::text, ` + seed + `)`},
		{"code", "character varying(20)", `hashtextextended("code"::text, ` + seed + `)`},
		{"ref", "uuid", `uuid_hash_extended("ref", ` + seed + `)`},
		{`we"ird`, "text", `hashtextextended("we""ird"::text, ` + seed + `)`},
	}
	for _, c := range cases {
		got, err := KeyHashExpr(c.col, c.typ)
		if err != nil || got != c.want {
			t.Errorf("KeyHashExpr(%q, %q) = %q, %v; want %q", c.col, c.typ, got, err, c.want)
		}
	}
	for _, typ := range []string{"numeric", "double precision", "timestamp with time zone", "bytea"} {
		if _, err := KeyHashExpr("k", typ); err == nil {
			t.Errorf("%s must be refused", typ)
		}
	}
	// Blank-padded character types compare with trailing spaces ignored and
	// their ::text cast strips them, so the row filter and the router would
	// hash different bytes for equal keys.
	for _, typ := range []string{"character(3)", "character", "bpchar", "char(1)"} {
		if _, err := KeyHashExpr("k", typ); err == nil || !strings.Contains(err.Error(), "blank-padded") {
			t.Errorf("%s must be refused as blank-padded: %v", typ, err)
		}
	}
}

func TestRangeFilterDropsKeySpaceBounds(t *testing.T) {
	h := "h(k)"
	cases := []struct {
		r    placement.Range
		want string
	}{
		{placement.Range{Start: math.MinInt64, End: math.MaxInt64}, "true"},
		{placement.Range{Start: math.MinInt64, End: -1}, "h(k) <= -1"},
		{placement.Range{Start: 0, End: math.MaxInt64}, "h(k) >= 0"},
		{placement.Range{Start: -5, End: 7}, "h(k) >= -5 AND h(k) <= 7"},
	}
	for _, c := range cases {
		if got := RangeFilter(h, c.r); got != c.want {
			t.Errorf("RangeFilter(%v) = %q, want %q", c.r, got, c.want)
		}
	}
}

func TestCreatePublicationSQL(t *testing.T) {
	got := CreatePublicationSQL("pgshard_reshard_g2_t1", []PublishedTable{
		{Schema: "public", Name: "orders", Filter: `hashint8extended("tenant_id"::int8, 1::int8) >= 0`},
		{Schema: "audit", Name: "ev ents"},
	})
	want := `CREATE PUBLICATION "pgshard_reshard_g2_t1" FOR TABLE "public"."orders" WHERE (hashint8extended("tenant_id"::int8, 1::int8) >= 0), "audit"."ev ents" WITH (publish = 'insert, update, delete')`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if got := CreatePublicationSQL("pgshard_reshard_g2_ref", nil); got != `CREATE PUBLICATION "pgshard_reshard_g2_ref" WITH (publish = 'insert, update, delete')` {
		t.Fatalf("empty publication: %s", got)
	}
	// A partitioned sharded table must publish via the partition root so its
	// root-level row filter applies and its rows are copied, not skipped.
	part := CreatePublicationSQL("pgshard_reshard_g2_t2", []PublishedTable{
		{Schema: "public", Name: "orders", Filter: `hashint8extended("tenant_id"::int8, 1::int8) >= 0`, Partitioned: true},
	})
	if !strings.Contains(part, "publish_via_partition_root = true") {
		t.Fatalf("partitioned publication missing via-root: %s", part)
	}
	// Non-partitioned publications must NOT add the option.
	if strings.Contains(want, "publish_via_partition_root") {
		t.Fatalf("non-partitioned publication should not set via-root: %s", want)
	}
}

func TestCreateSubscriptionSQL(t *testing.T) {
	got := CreateSubscriptionSQL("pgshard_reshard_g2_t1_s0", "host=src dbname=app password='p''w'", []string{"pgshard_reshard_g2_t1", "pgshard_reshard_g2_ref"}, SubscriptionOptions{Slot: "pgshard_reshard_g2_t1_s0"})
	want := `CREATE SUBSCRIPTION "pgshard_reshard_g2_t1_s0" CONNECTION 'host=src dbname=app password=''p''''w''' PUBLICATION "pgshard_reshard_g2_t1", "pgshard_reshard_g2_ref" WITH (copy_data = true, create_slot = true, enabled = true, streaming = parallel, two_phase = false, origin = any, slot_name = 'pgshard_reshard_g2_t1_s0')`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if got := CreateSubscriptionSQL("s", "c", []string{"p"}, SubscriptionOptions{Slot: "s", Failover: true}); got[len(got)-len("failover = true)"):] != "failover = true)" {
		t.Fatalf("failover option missing: %s", got)
	}
}

func TestNames(t *testing.T) {
	if PublicationName(2, 1) != "pgshard_reshard_g2_t1" || ReferencePublicationName(3) != "pgshard_reshard_g3_ref" ||
		HomePublicationName(3) != "pgshard_reshard_g3_home" || SubscriptionName(2, 1, 0) != "pgshard_reshard_g2_t1_s0" {
		t.Fatal("names changed")
	}
}

func TestAggregateAndCaughtUp(t *testing.T) {
	if Aggregate(nil).CaughtUp(1) {
		t.Fatal("no subscriptions is not caught up")
	}
	reports := []SubscriptionProgress{
		{Rels: map[RelState]int{'r': 3}, LagBytes: 100, Enabled: true},
		{Rels: map[RelState]int{'r': 1, 'd': 2}, LagBytes: 5000, Enabled: true},
		{Rels: map[RelState]int{'r': 2}, LagBytes: 0, Enabled: false},
	}
	p := Aggregate(reports)
	if p.Subscriptions != 3 || p.TablesTotal != 8 || p.TablesReady != 6 || p.LagBytes != 5000 || p.Paused != 1 {
		t.Fatalf("aggregate %+v", p)
	}
	if p.CaughtUp(1 << 20) {
		t.Fatal("tables still copying")
	}
	reports[1].Rels = map[RelState]int{'r': 3}
	p = Aggregate(reports)
	if !p.CaughtUp(5001) || p.CaughtUp(5000) {
		t.Fatalf("lag threshold: %+v", p)
	}
	reports[2].LagBytes = LagUnknown
	p = Aggregate(reports)
	if p.LagBytes != LagUnknown || p.CaughtUp(math.MaxInt64) {
		t.Fatalf("unknown lag must win: %+v", p)
	}
	p = Aggregate([]SubscriptionProgress{{Rels: map[RelState]int{'r': 1}, LagBytes: LagUnknown, Enabled: true}, {Rels: map[RelState]int{'r': 1}, LagBytes: 7, Enabled: true}})
	if p.LagBytes != LagUnknown {
		t.Fatalf("unknown lag must stick: %+v", p)
	}
}

func TestThrottleHysteresis(t *testing.T) {
	const hi, lo = 100, 40
	cases := []struct {
		paused bool
		lag    int64
		want   bool
	}{
		{false, 0, false}, {false, 40, false}, {false, 41, false}, {false, 99, false}, {false, 100, true},
		{true, 150, true}, {true, 99, true}, {true, 41, true}, {true, 40, false}, {true, 0, false},
	}
	for _, c := range cases {
		if got := Throttle(c.paused, c.lag, hi, lo); got != c.want {
			t.Errorf("Throttle(%v, %d) = %v, want %v", c.paused, c.lag, got, c.want)
		}
	}
	c := &Copier{ThrottleHigh: 100}
	if hi, lo := c.watermarks(); hi != 100 || lo != 25 {
		t.Fatalf("watermarks %d %d", hi, lo)
	}
	c = &Copier{}
	if hi, lo := c.watermarks(); hi != DefaultThrottleHi || lo != DefaultThrottleLo {
		t.Fatalf("default watermarks %d %d", hi, lo)
	}
	c = &Copier{ThrottleHigh: 10, ThrottleLow: 50}
	if hi, lo := c.watermarks(); hi != 10 || lo != 2 {
		t.Fatalf("inverted watermarks %d %d", hi, lo)
	}
}

func TestHomeTarget(t *testing.T) {
	ranges, _ := placement.Split(4)
	if HomeTarget(ranges) != 2 {
		t.Fatalf("keyspace id 0 lies in the third quarter, got %d", HomeTarget(ranges))
	}
	one, _ := placement.Split(1)
	if HomeTarget(one) != 0 {
		t.Fatal("single range")
	}
	exact := placement.RangeSet{{Start: math.MinInt64, End: -1}, {Start: 0, End: 0}, {Start: 1, End: math.MaxInt64}}
	if HomeTarget(exact) != 1 {
		t.Fatalf("keyspace id 0 exactly: got %d", HomeTarget(exact))
	}
}

func TestAgentAddr(t *testing.T) {
	for in, want := range map[string]string{"host:5432": "host:9090", "10.0.0.1:5432": "10.0.0.1:9090", "host": "host:9090", "[::1]:5432": "[::1]:9090"} {
		if got, err := AgentAddr(in, 0); err != nil || got != want {
			t.Errorf("AgentAddr(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if got, _ := AgentAddr("h:5432", 7000); got != "h:7000" {
		t.Fatalf("custom port: %s", got)
	}
	if _, err := AgentAddr("", 0); err == nil {
		t.Fatal("empty endpoint")
	}
}

func TestExpandShardTemplate(t *testing.T) {
	got := ExpandShardTemplate("postgres://u:p@c-{group}-rw:5432/{db}?set={set}&id={id}", "g2", 1, "shard-1-g2", "app")
	if got != "postgres://u:p@c-shard-1-g2-rw:5432/app?set=g2&id=1" {
		t.Fatal(got)
	}
}

// TestProgressNamesItsBlockers: "5/8 tables ready, lag 0 bytes" is what a
// stalled reshard reported for 35 minutes, and it does not say which three
// tables are stuck -- the diagnosis needed the must-gather and the target's
// PostgreSQL log. The status should name them.
func TestProgressNamesItsBlockers(t *testing.T) {
	reports := []SubscriptionProgress{
		{Name: "pgshard_reshard_g2_t0_s0", Rels: map[RelState]int{'r': 2}, LagBytes: 0, Enabled: true},
		{Name: "pgshard_reshard_g2_t0_s1", Rels: map[RelState]int{'r': 3, 'd': 3}, LagBytes: 0, Enabled: true,
			Blockers: []string{"public.ledger(d)", "public.orders(d)", "public.accounts(d)"}},
	}
	p := Aggregate(reports)
	if p.TablesReady != 5 || p.TablesTotal != 8 {
		t.Fatalf("progress %d/%d, want 5/8", p.TablesReady, p.TablesTotal)
	}
	got := p.Describe()
	for _, want := range []string{"5/8 tables ready", "pgshard_reshard_g2_t0_s1", "public.ledger(d)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("progress %q does not name %q; an operator still cannot tell which target or table is blocking", got, want)
		}
	}
	// Bounded: a workflow moving hundreds of tables must not produce an
	// unbounded status message.
	if len(p.Blockers) > BlockerSamples {
		t.Fatalf("%d blockers, want at most %d", len(p.Blockers), BlockerSamples)
	}
}

func TestProgressWithoutBlockersIsUnchanged(t *testing.T) {
	p := Aggregate([]SubscriptionProgress{{Name: "s", Rels: map[RelState]int{'r': 4}, LagBytes: 12}})
	if got := p.Describe(); got != "4/4 tables ready, lag 12 bytes" {
		t.Fatalf("describe = %q", got)
	}
}

// TestSubscriptionWithNoTablesIsNotCaughtUp: a subscription PostgreSQL has
// not yet filled pg_subscription_rel for reports no relations, and counting
// its zero tables as zero outstanding made the switch gate open before any
// data had been copied. The e2e upgrade then switched writes to a target
// holding 1875 of 6400 acknowledged rows.
func TestSubscriptionWithNoTablesIsNotCaughtUp(t *testing.T) {
	fresh := SubscriptionProgress{Name: "pgshard_reshard_g2_t0_s0", Rels: map[RelState]int{}, LagBytes: 0, Enabled: true}
	p := Aggregate([]SubscriptionProgress{fresh})
	if p.Enumerating != 1 || p.TablesTotal != 0 {
		t.Fatalf("aggregate %+v", p)
	}
	if p.CaughtUp(1 << 20) {
		t.Fatal("a subscription that has copied nothing must not read as caught up")
	}
	if !strings.Contains(p.Describe(), "no tables yet") {
		t.Fatalf("the gate must say why it is closed: %q", p.Describe())
	}
	// One subscription still enumerating holds the gate even when the
	// others have finished.
	done := SubscriptionProgress{Name: "pgshard_reshard_g2_t1_s0", Rels: map[RelState]int{'r': 4}, LagBytes: 0, Enabled: true}
	if Aggregate([]SubscriptionProgress{done, fresh}).CaughtUp(1 << 20) {
		t.Fatal("one subscription with no tables must hold the gate")
	}
	if !Aggregate([]SubscriptionProgress{done}).CaughtUp(1 << 20) {
		t.Fatal("a finished subscription is caught up")
	}
}
