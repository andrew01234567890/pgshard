package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestRunSaysWhyItIsNotLeading guards a silence that has already cost time. A
// controller that cannot take the leader lock said nothing at all, so a run
// where one was never leader looked exactly like a run where one was working
// and the reshard was stuck. It has to say so, with how long it has waited.
func TestRunSaysWhyItIsNotLeading(t *testing.T) {
	var buf bytes.Buffer
	now := time.Unix(1000, 0)
	r := &Reconciler{
		// An unreachable catalog: lead() cannot even connect, so this exercises
		// the other arm and must still report.
		DSN:           "postgres://127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1",
		Logger:        slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		RetryInterval: time.Millisecond,
		Now:           func() time.Time { return now },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)

	if got := buf.String(); !strings.Contains(got, "controller leadership ended") {
		t.Fatalf("a controller that cannot reach the catalog must say so: %q", got)
	}
}

// TestLeaderWaitIntervalIsBounded pins the pacing: a controller waiting behind
// another leader reports on a bounded interval rather than every retry, which
// at the default retry would be a line every second for the whole wait.
func TestLeaderWaitIntervalIsBounded(t *testing.T) {
	if leaderWaitLogInterval < 30*time.Second {
		t.Fatalf("leaderWaitLogInterval = %s: too chatty for a long wait", leaderWaitLogInterval)
	}
	if leaderWaitLogInterval > 5*time.Minute {
		t.Fatalf("leaderWaitLogInterval = %s: too quiet to explain a stalled run", leaderWaitLogInterval)
	}
}
