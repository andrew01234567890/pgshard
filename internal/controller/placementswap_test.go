package controller

import (
	"strings"
	"testing"
)

func TestKeysetBoundsEscapeBackslash(t *testing.T) {
	back, plain := `a\1`, "abc"
	last := &Tuple{Values: []*string{&back, &plain, nil}}
	got := keysetBounds(last, []int{0, 1, 2}, []string{"id", "v", "n"}, map[string]string{"id": "text", "v": "text", "n": "bigint"})
	want := []string{`E'a\\1'::text`, `'abc'::text`, `NULL::bigint`}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("bounds %v want %v", got, want)
	}
}

func TestPlacementMismatches(t *testing.T) {
	src := rowDigest{Rows: 10, Hash: 77}
	if m := placementMismatches("reference", src, map[int32]rowDigest{0: src, 1: src}); len(m) != 0 {
		t.Fatalf("reference match: %v", m)
	}
	if m := placementMismatches("reference", src, map[int32]rowDigest{0: src, 1: {Rows: 9, Hash: 70}}); len(m) != 1 || !strings.Contains(m[0], "shard 1") {
		t.Fatalf("reference mismatch: %v", m)
	}
	if m := placementMismatches("sharded", src, map[int32]rowDigest{0: {Rows: 4, Hash: 30}, 1: {Rows: 6, Hash: 47}}); len(m) != 0 {
		t.Fatalf("sharded match: %v", m)
	}
	if m := placementMismatches("sharded", src, map[int32]rowDigest{0: {Rows: 4, Hash: 30}, 1: {Rows: 5, Hash: 40}}); len(m) != 1 {
		t.Fatalf("sharded mismatch: %v", m)
	}
	if m := placementMismatches("unsharded", src, map[int32]rowDigest{0: src, 1: {}}); len(m) != 0 {
		t.Fatalf("unsharded match: %v", m)
	}
}
