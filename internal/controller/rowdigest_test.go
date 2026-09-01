package controller

import "testing"

// TestADigestIsHarderToFoolThanASum: verification decides whether a cutover
// proceeds, so what it costs to fool matters. A sum is commutative and
// additive, so any two rows swapped for two others of the same total pass
// unnoticed -- a target holding wrong rows verifying clean, which is the
// answer that lets the cutover continue.
//
// The second combination is over a different algebra, so a substitution
// that preserves the sum still has to preserve this one.
func TestADigestIsHarderToFoolThanASum(t *testing.T) {
	// Two rows hashing to 10 and 20, against two hashing to 15 and 15:
	// same count, same sum, different data.
	honest := rowDigest{Rows: 1, Hash: 10, XOR: 10}.add(rowDigest{Rows: 1, Hash: 20, XOR: 20})
	forged := rowDigest{Rows: 1, Hash: 15, XOR: 15}.add(rowDigest{Rows: 1, Hash: 15, XOR: 15})

	if honest.Rows != forged.Rows || honest.Hash != forged.Hash {
		t.Fatalf("the fixture is wrong: it must agree on count and sum, got %+v and %+v", honest, forged)
	}
	if honest == forged {
		t.Fatalf("a substitution preserving the count and sum still verified clean: %+v", honest)
	}
}

// TestDigestsCombineAcrossSources: a target's prediction is the sum of what
// several sources contribute, so combining has to be associative and
// commutative in both aggregates or the order sources are read in would
// change the answer.
func TestDigestsCombineAcrossSources(t *testing.T) {
	a := rowDigest{Rows: 3, Hash: 7, XOR: 7}
	b := rowDigest{Rows: 5, Hash: 11, XOR: 11}
	c := rowDigest{Rows: 2, Hash: 13, XOR: 13}
	if got, want := a.add(b).add(c), c.add(b).add(a); got != want {
		t.Fatalf("reading sources in a different order gave %+v and %+v", got, want)
	}
	if got := a.add(b); got.Rows != 8 || got.Hash != 18 || got.XOR != 7^11 {
		t.Fatalf("combined digest %+v", got)
	}
}
