package snapshot

import (
	"fmt"
	"slices"
	"testing"
)

func snapshotWithRanges(n int, indexed bool) *Snapshot {
	s := &Snapshot{ServingSet: "default", ShardSets: map[string][]Range{}, Serving: map[ShardKey]Serving{}}
	for i := range n {
		s.ShardSets["default"] = append(s.ShardSets["default"], Range{ShardID: int32(i)})
		s.Serving[ShardKey{ShardSet: "default", ShardID: int32(i)}] = Serving{State: "serving"}
	}
	if indexed {
		s.index()
	}
	return s
}

// BenchmarkShardIDs measures what a scatter or a reference write asks for
// while it is being planned. It rebuilt the list, deduplicated it against
// itself and sorted it every time, so planning one statement over the whole
// shard set cost O(shards^2) comparisons for an answer that does not change
// while the snapshot lives.
func BenchmarkShardIDs(b *testing.B) {
	for _, n := range []int{1, 64, 1024} {
		s := snapshotWithRanges(n, true)
		b.Run(fmt.Sprintf("indexed/%d", n), func(b *testing.B) {
			for range b.N {
				if len(s.ShardIDs("default")) != n {
					b.Fatal("unexpected")
				}
			}
		})
		scan := snapshotWithRanges(n, false)
		b.Run(fmt.Sprintf("scan/%d", n), func(b *testing.B) {
			for range b.N {
				if len(scan.ShardIDs("default")) != n {
					b.Fatal("unexpected")
				}
			}
		})
	}
}

// TestShardIDsAgreesWithTheScan: the precomputed answer must equal what the
// walk would have returned, including for a snapshot built directly rather
// than through Load -- which must still scan rather than report nothing.
func TestShardIDsAgreesWithTheScan(t *testing.T) {
	for _, n := range []int{0, 1, 7} {
		indexed := snapshotWithRanges(n, true)
		scan := snapshotWithRanges(n, false)
		if got, want := indexed.ShardIDs("default"), scan.ShardIDs("default"); !slices.Equal(got, want) {
			t.Fatalf("%d shards: indexed %v, scan %v", n, got, want)
		}
		if !slices.IsSorted(indexed.ShardIDs("default")) {
			t.Fatalf("%d shards: not ascending: %v", n, indexed.ShardIDs("default"))
		}
	}
	// A set nobody asked about is empty, not the default set's answer.
	if got := snapshotWithRanges(4, true).ShardIDs("g2"); got != nil {
		t.Fatalf("unknown set = %v, want nil", got)
	}
}

// Duplicated ids in the ranges -- two ranges of one shard -- collapse, and
// the result is still ascending.
func TestShardIDsDeduplicates(t *testing.T) {
	s := &Snapshot{ShardSets: map[string][]Range{"default": {{ShardID: 2}, {ShardID: 0}, {ShardID: 2}, {ShardID: 1}}}}
	s.index()
	if got := s.ShardIDs("default"); !slices.Equal(got, []int32{0, 1, 2}) {
		t.Fatalf("ShardIDs = %v, want [0 1 2]", got)
	}
}

// TestReshardingAgreesWithTheScan: same contract as Migrating's.
func TestReshardingAgreesWithTheScan(t *testing.T) {
	for _, state := range []string{"serving", "provisioning"} {
		s := &Snapshot{Serving: map[ShardKey]Serving{{ShardSet: "default", ShardID: 0}: {State: state}}}
		want := s.Resharding()
		s.index()
		if got := s.Resharding(); got != want {
			t.Fatalf("%s: indexed %v, scan %v", state, got, want)
		}
	}
}

// A loaded snapshot answers from what it computed once, which is the point:
// the same backing array comes back every time, so a scatter planned twice
// pays for the list once. It is also why the slice must not be written to.
func TestShardIDsAreComputedOnce(t *testing.T) {
	s := snapshotWithRanges(8, true)
	first, second := s.ShardIDs("default"), s.ShardIDs("default")
	if len(first) == 0 || &first[0] != &second[0] {
		t.Fatal("ShardIDs rebuilt the list; a loaded snapshot must answer from what it indexed")
	}
	// And a snapshot built by hand still answers, by scanning.
	byHand := snapshotWithRanges(8, false)
	if got := byHand.ShardIDs("default"); len(got) != 8 {
		t.Fatalf("unindexed snapshot = %v, want the scan's answer", got)
	}
}
