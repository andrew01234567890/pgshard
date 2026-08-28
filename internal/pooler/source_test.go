package pooler

import (
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
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
