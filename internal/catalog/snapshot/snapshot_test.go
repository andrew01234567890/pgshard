package snapshot

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

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
	r := &Roles{verifiers: map[string]RoleCred{"alice": {Verifier: "SCRAM-SHA-256$4096:secretsalt$stored:server", CanLogin: true}}}
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

// TestFreshnessIsBoundedByMaxAge: a watcher whose reloads fail keeps the
// last snapshot it managed to read, so age is the only thing that tells a
// current view from one that stopped being refreshed an hour ago.
func TestFreshnessIsBoundedByMaxAge(t *testing.T) {
	loaded := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	var w Watcher
	if w.Fresh(loaded) {
		t.Error("a watcher with no snapshot is not fresh")
	}
	if got := w.AgeSeconds(loaded); got != -1 {
		t.Errorf("age with no snapshot = %v, want -1", got)
	}
	w.current.Store(&Snapshot{LoadedAt: loaded})
	for _, c := range []struct {
		at    time.Time
		fresh bool
	}{
		{loaded, true},
		{loaded.Add(MaxAge), true},
		{loaded.Add(MaxAge + time.Millisecond), false},
		{loaded.Add(time.Hour), false},
	} {
		if got := w.Fresh(c.at); got != c.fresh {
			t.Errorf("fresh at %s = %v, want %v", c.at.Sub(loaded), got, c.fresh)
		}
	}
	if got := w.AgeSeconds(loaded.Add(90 * time.Second)); got != 90 {
		t.Errorf("age = %v, want 90", got)
	}
	// A snapshot built by hand carries no load time and is never stale, so
	// tests and fixtures are not silently refused.
	if (&Snapshot{}).Stale(loaded) {
		t.Error("a snapshot with no load time must not be stale")
	}
}

// TestAReloadOfAnUnchangedCatalogPlansTheSame: the watcher swaps a freshly
// built snapshot in on every reload whether or not the catalog moved, so
// comparing snapshots by identity replanned every prepared statement of
// every session on its next Bind, every reload, for ever.
func TestAReloadOfAnUnchangedCatalogPlansTheSame(t *testing.T) {
	build := func() *Snapshot {
		s := &Snapshot{
			LoadedAt:           time.Now(),
			ShardMapGeneration: 7,
			DesiredGeneration:  9,
			ServingSet:         "default",
			ShardSets:          map[string][]Range{"default": {{ShardID: 0, Start: math.MinInt64, End: 0}, {ShardID: 1, Start: 1, End: math.MaxInt64}}},
			Serving: map[ShardKey]Serving{
				{ShardSet: "default", ShardID: 0}: {PrimaryEndpoint: "a:5432", Epoch: 3, State: "serving"},
				{ShardSet: "default", ShardID: 1}: {PrimaryEndpoint: "b:5432", Epoch: 4, State: "serving"},
			},
			Databases: map[string]catalog.Database{"app": {Name: "app", DefaultPlacement: "unsharded", HomeShard: 0}},
			Tables: map[TableKey]Placement{
				{Database: "app", SchemaName: "public", TableName: "orders"}: {Placement: "sharded", ShardKey: "tenant_id", Generation: 3},
			},
			Sequences: map[string]bool{"app.public.orders_id_seq": true},
		}
		s.index()
		return s
	}

	first, second := build(), build()
	if first == second {
		t.Fatal("the fixture must build two snapshots, as two reloads do")
	}
	if !SamePlanning(first, second) {
		t.Fatal("two reloads of an unchanged catalog must not replan anything")
	}
	// Read at a different moment: the same catalog, said again.
	later := build()
	later.LoadedAt = first.LoadedAt.Add(time.Hour)
	later.index()
	if !SamePlanning(first, later) {
		t.Fatal("when the catalog was read is not what a plan depends on")
	}

	// Anything the snapshot says about the catalog does replan.
	for name, change := range map[string]func(*Snapshot){
		"shard map generation": func(s *Snapshot) { s.ShardMapGeneration = 8 },
		"desired generation":   func(s *Snapshot) { s.DesiredGeneration = 10 },
		"serving set":          func(s *Snapshot) { s.ServingSet = "g2" },
		"a range bound":        func(s *Snapshot) { s.ShardSets["default"][0].End = 5 },
		"a shard's state": func(s *Snapshot) {
			s.Serving[ShardKey{ShardSet: "default", ShardID: 0}] = Serving{State: "provisioning"}
		},
		"a table's placement": func(s *Snapshot) {
			s.Tables[TableKey{Database: "app", SchemaName: "public", TableName: "orders"}] = Placement{Placement: "reference"}
		},
		"a new table": func(s *Snapshot) {
			s.Tables[TableKey{Database: "app", SchemaName: "public", TableName: "new"}] = Placement{}
		},
		"a database default": func(s *Snapshot) { s.Databases["app"] = catalog.Database{Name: "app", DefaultPlacement: "sharded"} },
		"a global sequence":  func(s *Snapshot) { s.Sequences["app.public.new_id_seq"] = true },
		"the write fence":    func(s *Snapshot) { s.WriteFence = true },
	} {
		changed := build()
		change(changed)
		changed.index()
		if SamePlanning(first, changed) {
			t.Errorf("%s changed and nothing replanned", name)
		}
	}

	// A snapshot built by hand rather than loaded carries no fingerprint,
	// and must compare equal only to itself: assuming otherwise would let a
	// fixture keep a plan it should not.
	byHand := &Snapshot{ShardMapGeneration: 7}
	if SamePlanning(byHand, first) || SamePlanning(byHand, &Snapshot{ShardMapGeneration: 7}) {
		t.Fatal("a snapshot with no fingerprint must not be taken for another")
	}
	if !SamePlanning(byHand, byHand) {
		t.Fatal("a snapshot is always itself")
	}
}
