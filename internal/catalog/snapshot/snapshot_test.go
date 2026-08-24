package snapshot

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

func i64(v int64) *int64 { return &v }

func testSnapshot() *Snapshot {
	s := &Snapshot{
		ShardSets: map[string][]Range{},
		Serving:   map[ShardKey]Serving{},
	}
	for _, r := range []catalog.ShardRange{
		{ShardSet: "default", ShardID: 0, Upper: i64(-100)},
		{ShardSet: "default", ShardID: 1, Lower: i64(-100), Upper: i64(0)},
		{ShardSet: "default", ShardID: 2, Lower: i64(0), Upper: i64(100)},
		{ShardSet: "default", ShardID: 3, Lower: i64(100)},
	} {
		s.ShardSets["default"] = append(s.ShardSets["default"], rangeFromCatalog(r))
	}
	return s
}

func TestLocate(t *testing.T) {
	s := testSnapshot()
	cases := map[int64]int32{
		math.MinInt64: 0, -101: 0, -100: 1, -1: 1, 0: 2, 99: 2, 100: 3, math.MaxInt64: 3,
	}
	for key, want := range cases {
		got, err := s.Locate("default", key)
		if err != nil || got != want {
			t.Errorf("Locate(%d) = %d, %v; want %d", key, got, err, want)
		}
	}
	if _, err := s.Locate("nope", 1); !errors.Is(err, ErrUnknownShardSet) {
		t.Fatalf("unknown shard set: %v", err)
	}
	s.ShardSets["gap"] = []Range{{ShardID: 0, Start: 0, End: 10}}
	if _, err := s.Locate("gap", 11); !errors.Is(err, ErrKeyUncovered) {
		t.Fatalf("uncovered key: %v", err)
	}
	if id, err := s.Locate("gap", 10); err != nil || id != 0 {
		t.Fatalf("inclusive end: %d %v", id, err)
	}
}

func TestCheckGeneration(t *testing.T) {
	if err := CheckGeneration(3, 3); err != nil {
		t.Fatal(err)
	}
	if err := CheckGeneration(3, 0); err != nil {
		t.Fatal(err)
	}
	err := CheckGeneration(3, 4)
	var stale *StaleGeneration
	if !errors.As(err, &stale) || stale.Routed != 3 || stale.Observed != 4 {
		t.Fatalf("expected StaleGeneration, got %v", err)
	}
}

func TestRolesNeverPrinted(t *testing.T) {
	r := &Roles{verifiers: map[string]string{"alice": "SCRAM-SHA-256$4096:secretsalt$stored:server"}}
	for _, out := range []string{fmt.Sprint(r), fmt.Sprintf("%v", r), fmt.Sprintf("%+v", r), fmt.Sprintf("%#v", r), r.String()} {
		if strings.Contains(out, "secretsalt") || strings.Contains(out, "alice") {
			t.Fatalf("verifier leaked: %s", out)
		}
	}
	if v, ok := r.Verifier("alice"); !ok || !strings.HasPrefix(v, "SCRAM") {
		t.Fatal("verifier lookup failed")
	}
	if s := testSnapshot().String(); strings.Contains(s, "SCRAM") || !strings.Contains(s, "shard_sets=1") {
		t.Fatalf("unexpected snapshot string %q", s)
	}
}

func TestConsistencyWatcher(t *testing.T) {
	s := testSnapshot()
	for id := int32(0); id < 4; id++ {
		s.Serving[ShardKey{"default", id}] = Serving{State: "serving"}
	}
	w := NewConsistencyWatcher()
	tr := w.Observe(s)
	if len(tr) != 1 || tr[0].From != Unknown || tr[0].To != Consistent {
		t.Fatalf("first observe: %+v", tr)
	}
	if tr := w.Observe(s); len(tr) != 0 {
		t.Fatalf("no change expected: %+v", tr)
	}
	s.Serving[ShardKey{"default", 2}] = Serving{State: "migrating"}
	tr = w.Observe(s)
	if len(tr) != 1 || tr[0].To != Inconsistent || len(tr[0].Blocking) != 1 || tr[0].Blocking[0].ShardID != 2 {
		t.Fatalf("migrating: %+v", tr)
	}
	s.Serving[ShardKey{"default", 2}] = Serving{State: "fenced"}
	if tr := w.Observe(s); len(tr) != 0 {
		t.Fatalf("fenced stays inconsistent: %+v", tr)
	}
	s.Serving[ShardKey{"default", 2}] = Serving{State: "serving"}
	delete(s.Serving, ShardKey{"default", 3})
	if tr := w.Observe(s); len(tr) != 0 || w.State("default") != Inconsistent {
		t.Fatalf("missing status blocks: %+v", tr)
	}
	s.Serving[ShardKey{"default", 3}] = Serving{State: "serving"}
	tr = w.Observe(s)
	if len(tr) != 1 || tr[0].From != Inconsistent || tr[0].To != Consistent {
		t.Fatalf("back to consistent: %+v", tr)
	}
}

func TestMigratingConsidersServingSetOnly(t *testing.T) {
	s := testSnapshot()
	if s.ServingShardSet() != "default" {
		t.Fatalf("empty ServingSet must fall back to default, got %q", s.ServingShardSet())
	}
	s.Serving[ShardKey{"g2", 0}] = Serving{Migrating: true}
	if s.Migrating() {
		t.Fatal("a migrating shard outside the serving set must not fence writes")
	}
	s.Serving[ShardKey{"default", 1}] = Serving{Migrating: true}
	if !s.Migrating() {
		t.Fatal("a migrating serving shard must fence writes")
	}
	s.ServingSet = "g2"
	s.Serving[ShardKey{"g2", 0}] = Serving{}
	if s.Migrating() {
		t.Fatal("after the flip the retired set's fence is irrelevant")
	}
}
