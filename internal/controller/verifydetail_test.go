package controller

import (
	"strings"
	"testing"
)

// TestVerifyDetailSeparatesForeignRowsFromDisagreement guards the distinction a
// mismatch has to make. The prediction covers one target's range while the
// count covers everything the target holds, so a bare mismatch cannot say
// whether the target is carrying rows that are not its own or whether the two
// sides disagree about the range they share. Those have very different causes.
func TestVerifyDetailSeparatesForeignRowsFromDisagreement(t *testing.T) {
	want := rowDigest{Rows: 1730, Hash: -59913961284}

	t.Run("target carries rows outside its range", func(t *testing.T) {
		got := rowDigest{Rows: 1740, Hash: -58625136507}
		d := verifyDetail(got, want, want)
		if !strings.Contains(d, "matches") || !strings.Contains(d, "10 row(s) it holds outside its range") {
			t.Fatalf("detail must name the foreign rows: %q", d)
		}
	})

	t.Run("the two sides disagree about the shared range", func(t *testing.T) {
		got := rowDigest{Rows: 1740, Hash: -58625136507}
		d := verifyDetail(got, want, got)
		if !strings.Contains(d, "disagree about that range") {
			t.Fatalf("detail must say the range itself disagrees: %q", d)
		}
		if strings.Contains(d, "outside") {
			t.Fatalf("nothing is outside the range here: %q", d)
		}
	})

	t.Run("both at once", func(t *testing.T) {
		got := rowDigest{Rows: 1740, Hash: 1}
		inRange := rowDigest{Rows: 1735, Hash: 2}
		d := verifyDetail(got, want, inRange)
		if !strings.Contains(d, "1735 rows") || !strings.Contains(d, "5 row(s) outside its range") {
			t.Fatalf("detail must report both halves: %q", d)
		}
	})
}
