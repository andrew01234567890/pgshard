package placement

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// Range is an inclusive interval of the int64 key space.
type Range struct {
	Start int64
	End   int64
}

// Contains reports whether id lies in r.
func (r Range) Contains(id int64) bool { return id >= r.Start && id <= r.End }

func (r Range) String() string { return fmt.Sprintf("[%d,%d]", r.Start, r.End) }

// RangeSet is a set of ranges that together must cover the whole key space
// exactly once. Ranges are kept sorted by Start.
type RangeSet []Range

var errEmpty = errors.New("placement: range set is empty")

// Validate checks that the ranges are well-formed, sorted, contiguous and
// cover MinInt64..MaxInt64.
func (s RangeSet) Validate() error {
	if len(s) == 0 {
		return errEmpty
	}
	for i, r := range s {
		if r.Start > r.End {
			return fmt.Errorf("placement: range %d %s is inverted", i, r)
		}
	}
	if s[0].Start != math.MinInt64 {
		return fmt.Errorf("placement: first range %s does not start at the bottom of the key space", s[0])
	}
	for i := 1; i < len(s); i++ {
		if s[i-1].End == math.MaxInt64 || s[i].Start != s[i-1].End+1 {
			return fmt.Errorf("placement: gap or overlap between %s and %s", s[i-1], s[i])
		}
	}
	if s[len(s)-1].End != math.MaxInt64 {
		return fmt.Errorf("placement: last range %s does not reach the top of the key space", s[len(s)-1])
	}
	return nil
}

// Locate returns the index of the range containing id. The set must be valid.
func (s RangeSet) Locate(id int64) int {
	return sort.Search(len(s), func(i int) bool { return s[i].End >= id })
}

// Split divides the key space into n ranges of as equal size as possible.
func Split(n int) (RangeSet, error) {
	if n <= 0 {
		return nil, fmt.Errorf("placement: split into %d ranges", n)
	}
	out := make(RangeSet, 0, n)
	// The key space has 2^64 keys, one more than MaxUint64 holds.
	width := uint64(math.MaxUint64) / uint64(n)
	rem := uint64(math.MaxUint64)%uint64(n) + 1
	if rem == uint64(n) {
		width++
		rem = 0
	}
	start := uint64(0)
	for i := 0; i < n; i++ {
		size := width
		if uint64(i) < rem {
			size++
		}
		end := start + size - 1
		out = append(out, Range{Start: fromOffset(start), End: fromOffset(end)})
		start = end + 1
	}
	return out, nil
}

// Merge replaces the adjacent ranges s[i..j] (inclusive) with one range.
func (s RangeSet) Merge(i, j int) (RangeSet, error) {
	if i < 0 || j >= len(s) || i >= j {
		return nil, fmt.Errorf("placement: merge indices %d..%d out of %d ranges", i, j, len(s))
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	out := make(RangeSet, 0, len(s)-(j-i))
	out = append(out, s[:i]...)
	out = append(out, Range{Start: s[i].Start, End: s[j].End})
	out = append(out, s[j+1:]...)
	return out, nil
}

// fromOffset maps an offset from MinInt64 back to a signed key.
func fromOffset(u uint64) int64 { return int64(u ^ (1 << 63)) }
