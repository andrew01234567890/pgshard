package pooler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestSnapshotSourceStopsServingOnAStaleView: generation and epoch are the
// fence the pooler enforces on the router's behalf. A watcher whose reloads
// fail keeps its last snapshot, so the pooler went on enforcing a fence
// from a view of the catalog that had stopped being refreshed -- and a
// router frozen at the same generation passes it.
func TestSnapshotSourceStopsServingOnAStaleView(t *testing.T) {
	shard := snapshot.ShardKey{ShardSet: "default", ShardID: 1}
	base := View{Generation: 1, Epoch: 1, Serving: true}
	var w snapshot.Watcher
	s := &SnapshotSource{Watcher: &w, Shard: shard, Base: base}
	if got := s.View(); !got.Serving {
		t.Fatal("before the first snapshot the pooler serves its configured view")
	}
	w.SetForTest(&snapshot.Snapshot{LoadedAt: time.Now(), ShardMapGeneration: 9,
		Serving: map[snapshot.ShardKey]snapshot.Serving{shard: {Epoch: 4}}})
	if got := s.View(); !got.Serving || got.Generation != 9 || got.Epoch != 4 {
		t.Fatalf("a fresh snapshot must be served: %+v", got)
	}
	w.SetForTest(&snapshot.Snapshot{LoadedAt: time.Now().Add(-snapshot.MaxAge - time.Second),
		ShardMapGeneration: 9, Serving: map[snapshot.ShardKey]snapshot.Serving{shard: {Epoch: 4}}})
	if got := s.View(); got.Serving {
		t.Fatalf("a view older than MaxAge must stop the pooler serving: %+v", got)
	}
}

// notServing is a Source whose view has stopped being refreshed, which is
// what SnapshotSource reports once its snapshot is older than MaxAge.
type notServing struct{ v View }

func (s notServing) View() View { return s.v }

// TestAPoolerThatCannotSeeTheCatalogRefusesEvenAMatchingGeneration: a stale
// view still carries the last generation and epoch the pooler read, and
// nothing looked at Serving -- so the fence was enforced from numbers that
// had stopped meaning anything. A router still on those numbers was admitted
// on a shard whose ranges may have moved.
func TestAPoolerThatCannotSeeTheCatalogRefusesEvenAMatchingGeneration(t *testing.T) {
	s := NewServer(Config{Source: notServing{View{Generation: 7, Epoch: 3}}})
	res, err := s.Reserve(context.Background(), &pgshardv1.ReserveRequest{SessionId: "r",
		Generation: &pgshardv1.Generation{ShardMapGeneration: 7, PrimaryEpoch: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetError() == nil {
		t.Fatal("a stale pooler admitted a request because its last-read generation happened to match")
	}
	if got := res.GetError().GetMessage(); !strings.Contains(got, "catalog view is stale") {
		t.Errorf("refusal %q must name the pooler as the stale party, not the router", got)
	}
}

// TestAStaticSourceIsAlwaysServing: Serving is what a snapshot-backed source
// clears when it goes stale. A static view is configured rather than
// refreshed, so the zero value of the field must not mean "refuse
// everything".
func TestAStaticSourceIsAlwaysServing(t *testing.T) {
	s := NewStaticSource(View{Generation: 4})
	if !s.View().Serving {
		t.Fatal("a static source built from a plain View does not serve")
	}
	s.Set(View{Generation: 5})
	if !s.View().Serving {
		t.Fatal("a static source stopped serving after Set")
	}
}
