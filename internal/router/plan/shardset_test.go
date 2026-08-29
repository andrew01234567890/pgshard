package plan

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// sameShardsByScan is what the sorted comparison replaced: set equality
// found by scanning one slice for each element of the other. It is kept
// here as the reference the new comparison is checked against, since the
// change swaps an algorithm rather than rewriting an expression.
func sameShardsByScan(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sortedSet(in []int32) []int32 {
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}

// TestSortedShardSetsMatchTheScanTheyReplaced: resolving a plan compared
// two shard sets by scanning one for each element of the other, which is
// the square of the shard count on every Bind. Sorting and comparing is
// only correct if it agrees on every shape -- duplicates in either side,
// different lengths that hold the same set, and the empty set.
func TestSortedShardSetsMatchTheScanTheyReplaced(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	pick := func(n, hi int) []int32 {
		out := make([]int32, n)
		for i := range out {
			out[i] = int32(rng.IntN(hi))
		}
		return out
	}
	for range 5000 {
		a, b := pick(rng.IntN(6), 4), pick(rng.IntN(6), 4)
		sa, sb := sortedSet(a), sortedSet(b)
		// The scan only claims to be a set comparison when neither side
		// repeats itself; the sorted form is compared after compaction,
		// which is what resolving now does to both sides.
		if want, got := sameShardsByScan(sa, sb), slices.Equal(sa, sb); want != got {
			t.Fatalf("sets %v vs %v: scan says %v, sorted says %v", sa, sb, want, got)
		}
	}
}

// TestSortedSetIsSortedAndUnique: the shard list a plan carries is relied
// on for its order, and it used to be sorted at the end of resolving.
func TestSortedSetIsSortedAndUnique(t *testing.T) {
	got := sortedSet([]int32{3, 1, 3, 0, 1, 2, 2})
	if !slices.Equal(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("sortedSet = %v", got)
	}
	if s := sortedSet(nil); len(s) != 0 {
		t.Fatalf("empty: %v", s)
	}
}
