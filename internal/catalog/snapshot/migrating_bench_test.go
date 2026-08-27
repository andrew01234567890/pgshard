package snapshot

import (
	"fmt"
	"testing"
)

func snapshotWithShards(n int, indexed bool) *Snapshot {
	s := &Snapshot{ServingSet: "default", Serving: make(map[ShardKey]Serving, n)}
	for i := 0; i < n; i++ {
		s.Serving[ShardKey{ShardSet: "default", ShardID: int32(i)}] = Serving{State: "serving"}
	}
	if indexed {
		s.index()
	}
	return s
}

// BenchmarkMigrating measures the check the router makes on every write. It
// walked the whole serving map per statement, so write admission cost
// O(shards) even in the steady state with no migration in flight.
func BenchmarkMigrating(b *testing.B) {
	for _, n := range []int{1, 64, 1024, 8192} {
		s := snapshotWithShards(n, true)
		b.Run(fmt.Sprintf("indexed/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if s.Migrating() {
					b.Fatal("unexpected")
				}
			}
		})
		scan := snapshotWithShards(n, false)
		b.Run(fmt.Sprintf("scan/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if scan.Migrating() {
					b.Fatal("unexpected")
				}
			}
		})
	}
}

// TestMigratingAgreesWithTheScan: the precomputed answer must equal what the
// walk would have returned, including for a snapshot built directly rather
// than through Load -- which must still scan rather than report a stale false.
func TestMigratingAgreesWithTheScan(t *testing.T) {
	cases := []struct {
		name string
		set  map[ShardKey]Serving
		want bool
	}{
		{"none", map[ShardKey]Serving{{ShardSet: "default", ShardID: 0}: {}}, false},
		{"one fenced", map[ShardKey]Serving{
			{ShardSet: "default", ShardID: 0}: {},
			{ShardSet: "default", ShardID: 1}: {Migrating: true}}, true},
		{"fenced on another set only", map[ShardKey]Serving{
			{ShardSet: "default", ShardID: 0}: {},
			{ShardSet: "g2", ShardID: 0}:      {Migrating: true}}, false},
	}
	for _, c := range cases {
		direct := &Snapshot{ServingSet: "default", Serving: c.set}
		if got := direct.Migrating(); got != c.want {
			t.Errorf("%s: a directly built snapshot reported %v, want %v", c.name, got, c.want)
		}
		loaded := &Snapshot{ServingSet: "default", Serving: c.set}
		loaded.index()
		if got := loaded.Migrating(); got != c.want {
			t.Errorf("%s: an indexed snapshot reported %v, want %v", c.name, got, c.want)
		}
	}
}
