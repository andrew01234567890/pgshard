package controller

import "testing"

// TestTheHoldIsBoundedByBytesNotOnlyOperations: catch-up holds committed
// operations before applying them, so a round's worth goes out together
// rather than one round trip per row. The hold was bounded by the
// OPERATION COUNT alone -- and an operation carries a whole row rendered
// as SQL, so 2000 of them is a few hundred kilobytes for a narrow table
// and gigabytes for one with a megabyte in a column.
//
// Counting operations bounds the count of something that has no bounded
// size, which is not a bound at all. This is the same mistake as a row
// channel bounded by rows, and it was made in the same week.
func TestTheHoldIsBoundedByBytesNotOnlyOperations(t *testing.T) {
	if applyFlushBytes <= 0 {
		t.Fatal("there is no byte bound on the hold")
	}
	// The byte bound has to bite before the count does for a wide row, or
	// it is decoration: one megabyte a row reaches it in single figures.
	wide := 1 << 20
	if applyFlushBytes/wide >= applyFlushOps {
		t.Fatalf("at %d bytes a row the count bound (%d) still fires first; the byte bound (%d) never applies",
			wide, applyFlushOps, applyFlushBytes)
	}
	// And it must not bite so early that a narrow table pays a round trip
	// per handful of rows: at 200 bytes a row the count bound should win.
	narrow := 200
	if applyFlushBytes/narrow < applyFlushOps {
		t.Fatalf("at %d bytes a row the byte bound (%d) fires before the count (%d); narrow tables lose the batching",
			narrow, applyFlushBytes, applyFlushOps)
	}
}
