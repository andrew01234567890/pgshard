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

// leaderCheck is how often a pass in flight is asked whether its loop is
// still the leader. Fast relative to a pass, because the window it closes
// is the one a handover opens.
const leaderCheck = time.Second

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
		watchPass(ctx, stallAfter, leader, log, name, pass)
	}
}

// watchPass runs one pass, reporting it if it stalls and CANCELLING it if
// the loop stops being the leader while it runs.
//
// Checking leadership before the pass is not enough: these passes write
// catalog state -- the applier applies DDL, the resolver commits and rolls
// back prepared transactions, the copier drives a reshard -- and a pass
// that began as leader used to run to completion after the term had moved
// on. During a handover the old leader and the new one then write at the
// same time, each believing it is alone. Cancelling is safe because a pass
// is already interruptible: the process can die mid-pass at any moment,
// so every one of them is written to be resumed rather than completed.
func watchPass(ctx context.Context, stall time.Duration, leader func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
	passCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		started := time.Now()
		stalled := time.NewTicker(stall)
		defer stalled.Stop()
		lost := time.NewTicker(leaderCheck)
		defer lost.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-lost.C:
				if leader != nil && !leader() {
					if l := log(); l != nil {
						l.Warn(name+" pass abandoned: no longer the leader",
							"running_for", time.Since(started).Round(time.Second))
					}
					cancel()
					return
				}
			case <-stalled.C:
				if l := log(); l != nil {
					l.Warn(name+" pass is not finishing", "running_for", time.Since(started).Round(time.Second))
				}
			}
		}
	}()
	defer close(done)
	pass(passCtx)
}
