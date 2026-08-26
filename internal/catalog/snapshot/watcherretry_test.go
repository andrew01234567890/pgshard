package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWatcherKeepsTryingTheFirstReload guards a startup race that has no
// recovery. The first reload used to return its error, and both callers run
// Run in a goroutine that only logs what it returns, so a router or pooler that
// started before the catalog accepted connections served for the rest of its
// life with no snapshot -- stamping every request with generation zero and
// having each one refused as a stale routing generation.
func TestWatcherKeepsTryingTheFirstReload(t *testing.T) {
	// A port nothing listens on: the first reload cannot succeed.
	w := NewWatcher("postgres://127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1", Options{
		ReloadInterval: time.Hour,
		DisableListen:  true,
		Logf:           func(string, ...any) {},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		cancel()
		t.Fatalf("the watcher gave up on an unreachable catalog: %v", err)
	case <-time.After(2 * time.Second):
	}
	if w.Current() != nil {
		t.Fatal("no snapshot can have loaded from an unreachable catalog")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelling must end the watcher, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the watcher ignored cancellation")
	}
}
