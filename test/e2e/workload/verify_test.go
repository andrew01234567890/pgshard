package workload

import (
	"context"
	"strings"
	"testing"
)

func counter(c Counted) Counter {
	return func(context.Context, int64, int64) (Counted, error) { return c, nil }
}

func TestAnIntactLedgerHasNoViolations(t *testing.T) {
	vs, err := Verify(context.Background(), []int64{7}, []int64{100},
		counter(Counted{Rows: 100, Distinct: 100, High: 100}))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 0 {
		t.Fatalf("intact ledger reported %v", vs)
	}
}

// The failure this whole suite exists to catch: the cluster came back and
// serves reads, but fewer acknowledged rows survive than were promised.
func TestALostAcknowledgedCommitIsAViolation(t *testing.T) {
	vs, err := Verify(context.Background(), []int64{7}, []int64{100},
		counter(Counted{Rows: 97, Distinct: 97, High: 100}))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || !strings.Contains(vs[0].Detail, "3 acknowledged commits were lost") {
		t.Fatalf("want one lost-commit violation, got %v", vs)
	}
}

// Uniqueness is enforced per shard, so a row that landed on its owner AND
// somewhere else is invisible to a per-owner count. Rows above distinct is
// the only signal.
func TestARowInTwoPlacesIsAViolation(t *testing.T) {
	vs, err := Verify(context.Background(), []int64{7}, []int64{100},
		counter(Counted{Rows: 105, Distinct: 100, High: 100}))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || !strings.Contains(vs[0].Detail, "5 rows exist in more than one place") {
		t.Fatalf("want one duplication violation, got %v", vs)
	}
}

// A run that acknowledged nothing proves nothing, and must not be allowed
// to read as a pass: zero acknowledged rows trivially all survive.
func TestAStreamThatAcknowledgedNothingIsAViolation(t *testing.T) {
	vs, err := Verify(context.Background(), []int64{7}, []int64{0},
		counter(Counted{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || !strings.Contains(vs[0].Detail, "proves nothing") {
		t.Fatalf("want a violation for an empty run, got %v", vs)
	}
}

func TestMismatchedStreamsAndMarksIsAnError(t *testing.T) {
	if _, err := Verify(context.Background(), []int64{1, 2}, []int64{5}, counter(Counted{})); err == nil {
		t.Fatal("a stream/mark mismatch must be an error, not a silent partial check")
	}
}
