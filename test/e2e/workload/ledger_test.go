package workload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAcknowledgedMarkAdvancesOnlyOnSuccess(t *testing.T) {
	var mu sync.Mutex
	var calls int
	l := &AckedLedger{
		Streams: []int64{7}, Table: "ledger", Batch: 5,
		Retry: time.Millisecond, Pace: time.Millisecond,
		Exec: func(_ context.Context, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			// The second batch never answers. Everything after it must
			// stay unacknowledged, because a workload that counted an
			// unanswered write would assert a promise never made.
			if calls >= 2 {
				return errors.New("connection reset")
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for l.Failures() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	acked := l.Finish()
	if acked[0] != 5 {
		t.Fatalf("acknowledged high-water is %d, want 5: only the first batch was answered", acked[0])
	}
	if l.Failures() == 0 {
		t.Fatal("no failures recorded, so the test did not exercise the failing path")
	}
}

func TestWhyNamesTheStreamThatAcknowledgedNothing(t *testing.T) {
	l := &AckedLedger{
		Streams: []int64{11}, Table: "ledger", Retry: time.Millisecond,
		Exec: func(context.Context, string) error { return errors.New("fenced: stale generation") },
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for l.Failures() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	l.Finish()
	why := l.Why()
	if !strings.Contains(why, "stream 11") || !strings.Contains(why, "fenced") {
		t.Fatalf("Why() must name the stream and its last error, got %q", why)
	}
}

// A workload that never ran and one whose every write failed both leave
// the acknowledged counts at zero. Only the attempt counters separate
// them, and a chaos test that cannot tell them apart reports a broken
// harness as a data-loss defect.
func TestAttemptsSeparateANonRunFromATotalFailure(t *testing.T) {
	l := &AckedLedger{
		Streams: []int64{1}, Table: "ledger", Retry: time.Millisecond,
		Exec: func(context.Context, string) error { return errors.New("no") },
	}
	if l.Attempts() != 0 {
		t.Fatal("a workload that has not started has made no attempts")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for l.Attempts() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	acked := l.Finish()
	if acked[0] != 0 {
		t.Fatalf("nothing was acknowledged, got high-water %d", acked[0])
	}
	if l.Attempts() == 0 {
		t.Fatal("attempts must record that the workload really ran")
	}
}

func TestInsertSQLCarriesTheKeyInTheStatement(t *testing.T) {
	sql := InsertSQL("ledger", 42, 1, 3)
	for _, want := range []string{"(1, 42, 1)", "(2, 42, 1)", "(3, 42, 1)", "ON CONFLICT DO NOTHING"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("InsertSQL missing %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, "SELECT") {
		t.Fatalf("the shard key must be readable from the statement, not produced by a subquery: %s", sql)
	}
}
