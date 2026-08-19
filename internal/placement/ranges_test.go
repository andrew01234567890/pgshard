package placement

import (
	"math"
	"testing"
)

func TestSplitCoversKeySpace(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 16, 1000} {
		s, err := Split(n)
		if err != nil {
			t.Fatal(err)
		}
		if len(s) != n {
			t.Fatalf("Split(%d) gave %d ranges", n, len(s))
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("Split(%d): %v", n, err)
		}
		var minSize, maxSize uint64 = math.MaxUint64, 0
		for _, r := range s {
			size := uint64(r.End-r.Start) + 1
			if size < minSize {
				minSize = size
			}
			if size > maxSize {
				maxSize = size
			}
		}
		if maxSize-minSize > 1 {
			t.Fatalf("Split(%d): sizes range %d..%d", n, minSize, maxSize)
		}
	}
	if _, err := Split(0); err == nil {
		t.Fatal("Split(0) accepted")
	}
}

func TestSplitTwoIsSignBoundary(t *testing.T) {
	s, _ := Split(2)
	want := RangeSet{{math.MinInt64, -1}, {0, math.MaxInt64}}
	if s[0] != want[0] || s[1] != want[1] {
		t.Fatalf("Split(2)=%v want %v", s, want)
	}
}

func TestValidateRejects(t *testing.T) {
	bad := map[string]RangeSet{
		"empty":     {},
		"inverted":  {{5, 4}},
		"no bottom": {{-5, math.MaxInt64}},
		"no top":    {{math.MinInt64, 5}},
		"gap":       {{math.MinInt64, 0}, {2, math.MaxInt64}},
		"overlap":   {{math.MinInt64, 0}, {0, math.MaxInt64}},
		"unsorted":  {{0, math.MaxInt64}, {math.MinInt64, -1}},
	}
	for name, s := range bad {
		if err := s.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := (RangeSet{{math.MinInt64, math.MaxInt64}}).Validate(); err != nil {
		t.Error(err)
	}
}

func TestLocate(t *testing.T) {
	s, _ := Split(4)
	for i, r := range s {
		for _, id := range []int64{r.Start, r.End, r.Start + (r.End-r.Start)/2} {
			if got := s.Locate(id); got != i {
				t.Errorf("Locate(%d)=%d want %d", id, got, i)
			}
		}
	}
	if s.Locate(math.MinInt64) != 0 || s.Locate(math.MaxInt64) != 3 {
		t.Error("extremes misplaced")
	}
}

func TestMerge(t *testing.T) {
	s, _ := Split(4)
	m, err := s.Merge(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 || m[1] != (Range{s[1].Start, s[2].End}) || m[0] != s[0] || m[2] != s[3] {
		t.Fatalf("Merge gave %v", m)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, ij := range [][2]int{{1, 1}, {2, 1}, {-1, 0}, {0, 4}} {
		if _, err := s.Merge(ij[0], ij[1]); err == nil {
			t.Errorf("Merge%v accepted", ij)
		}
	}
	if _, err := (RangeSet{{0, 1}, {2, 3}}).Merge(0, 1); err == nil {
		t.Error("invalid set merged")
	}
}
