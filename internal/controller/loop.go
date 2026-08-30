package controller

import (
	"context"
	"log/slog"
	"time"
)

// stallAfter is how long one pass of a controller loop may run before it is
// reported, and how often it is reported after that. A pass carries no
// deadline of its own and its loop is a single goroutine, so a shard that
// accepts a connection and then never answers stops the loop for the life
// of the process. Without this the only symptom is that the loop's log
// lines stop.
const stallAfter = time.Minute

// runLoop drives pass on every tick while leader() holds, and reports a
// pass that stops making progress. pass logs its own outcome.
func runLoop(ctx context.Context, interval time.Duration, leader func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if leader != nil && !leader() {
			continue
		}
		watchPass(ctx, stallAfter, log, name, pass)
	}
}

func watchPass(ctx context.Context, stall time.Duration, log func() *slog.Logger, name string, pass func(context.Context)) {
	done := make(chan struct{})
	go func() {
		started := time.Now()
		t := time.NewTicker(stall)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if l := log(); l != nil {
					l.Warn(name+" pass is not finishing", "running_for", time.Since(started).Round(time.Second))
				}
			}
		}
	}()
	defer close(done)
	pass(ctx)
}
