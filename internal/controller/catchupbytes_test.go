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

// TestTheOpenTransactionHasABoundNoFlushCanProvide: the committed hold can
// always be shortened by flushing it. The transaction being decoded cannot
// -- its operations are not applicable until it commits -- so it needs a
// bound of its own, and without one a peek that fills without reaching a
// commit quadruples its limit and decodes the same transaction again from
// the start, retaining more of it each round.
func TestTheOpenTransactionHasABoundNoFlushCanProvide(t *testing.T) {
	if catchUpMaxOpenBytes <= applyFlushBytes {
		t.Fatalf("the open bound (%d) is not above the flush bound (%d); an ordinary round would trip it",
			catchUpMaxOpenBytes, applyFlushBytes)
	}
	// Generous enough that a normal large transaction still replicates:
	// a hundred thousand rows of a kilobyte is inside it.
	if catchUpMaxOpenBytes < 100_000*1024 {
		t.Fatalf("the open bound (%d) refuses transactions a placement should carry", catchUpMaxOpenBytes)
	}
}
