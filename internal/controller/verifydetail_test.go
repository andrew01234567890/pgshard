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

// TestSourcesMovedMessageSeparatesAFenceLeakFromDisagreement: a target
// holding more than its sources predict cannot happen while the fence
// holds, because replication only carries rows the sources have. So the
// re-read decides which bug it is, and a failed cutover's status is all
// anyone gets afterwards -- it has to say which.
func TestSourcesMovedMessageSeparatesAFenceLeakFromDisagreement(t *testing.T) {
	was := rowDigest{Rows: 1730, Hash: -59913961284}

	still := sourcesMovedMessage(was, was)
	if !strings.Contains(still, "the target and its sources disagree") {
		t.Errorf("an unchanged re-read must name the disagreement: %q", still)
	}
	if strings.Contains(still, "fence") {
		t.Errorf("an unchanged re-read must not blame the fence: %q", still)
	}

	moved := sourcesMovedMessage(rowDigest{Rows: 1740, Hash: -58625136507}, was)
	if !strings.Contains(moved, "the fence did not hold") {
		t.Errorf("a moved re-read must name the fence: %q", moved)
	}
	for _, want := range []string{"1740", "-58625136507"} {
		if !strings.Contains(moved, want) {
			t.Errorf("the new prediction is the evidence and must be in the message; %q lacks %s", moved, want)
		}
	}

	// A hash that changed without the count changing is still movement:
	// an UPDATE replicates as well as an INSERT.
	if got := sourcesMovedMessage(rowDigest{Rows: was.Rows, Hash: was.Hash + 1}, was); !strings.Contains(got, "did not hold") {
		t.Errorf("a changed hash at the same count is movement too: %q", got)
	}
}
