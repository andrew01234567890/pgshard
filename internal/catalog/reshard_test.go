package catalog

import (
	"math"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

func TestShardSetName(t *testing.T) {
	for gen, want := range map[int64]string{0: "default", 1: "default", 2: "g2", 17: "g17"} {
		if got := ShardSetName(gen); got != want {
			t.Errorf("ShardSetName(%d) = %s, want %s", gen, got, want)
		}
	}
}

func TestInt8RangeRendersUnboundedEnds(t *testing.T) {
	cases := map[placement.Range]string{
		{Start: math.MinInt64, End: math.MaxInt64}: "[,)",
		{Start: math.MinInt64, End: -1}:            "[,0)",
		{Start: 0, End: math.MaxInt64}:             "[0,)",
		{Start: 5, End: 9}:                         "[5,10)",
	}
	for r, want := range cases {
		if got := Int8Range(r); got != want {
			t.Errorf("Int8Range(%v) = %s, want %s", r, got, want)
		}
	}
}

func TestRangeSetRoundTripsSplit(t *testing.T) {
	for n := 1; n <= 8; n++ {
		ranges, err := placement.Split(n)
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]ShardRange, 0, n)
		for i, r := range ranges {
			row := ShardRange{ShardSet: "x", ShardID: int32(i)}
			if r.Start != math.MinInt64 {
				lo := r.Start
				row.Lower = &lo
			}
			if r.End != math.MaxInt64 {
				hi := r.End + 1
				row.Upper = &hi
			}
			rows = append(rows, row)
		}
		got := RangeSet(rows)
		if err := got.Validate(); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		for i := range ranges {
			if got[i] != ranges[i] {
				t.Fatalf("n=%d range %d: %v != %v", n, i, got[i], ranges[i])
			}
		}
	}
}

func TestValidateShardRangesRequiresKeyOrdinals(t *testing.T) {
	zero, hundred := int64(0), int64(100)
	dense := []ShardRange{
		{ShardID: 0, Upper: &zero},
		{ShardID: 1, Lower: &zero, Upper: &hundred},
		{ShardID: 2, Lower: &hundred},
	}
	if err := ValidateShardRanges(dense); err != nil {
		t.Fatalf("dense IDs rejected: %v", err)
	}

	sparse := append([]ShardRange(nil), dense...)
	sparse[1].ShardID = 20
	if err := ValidateShardRanges(sparse); err == nil {
		t.Fatal("shard IDs that routing would renumber were accepted")
	}

	swapped := append([]ShardRange(nil), dense...)
	swapped[0].ShardID, swapped[2].ShardID = 2, 0
	if err := ValidateShardRanges(swapped); err == nil {
		t.Fatal("shard IDs out of key order were accepted")
	}
}
