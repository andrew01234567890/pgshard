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
//
// The pass is NOT abandoned when the term drops, which is right only for
// a loop that holds a workflow CLAIM -- the copier and the placer.
// Nothing releases a claim: claimWorkflow lets a successor in only once
// owned_at is DefaultOwnerLease old, and there is no site anywhere that
// sets owner back to NULL. So abandoning one of those partway leaves the
// claim, and any write pause it had raised, standing for five minutes --
// where letting the pass finish clears it inside the cutover's own
// timeout. That is a worse outage than the one being avoided, and during
// the same event.
//
// Everything else uses runLoopStoppable.
func runLoop(ctx context.Context, interval time.Duration, leader func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
	loop(ctx, interval, leader, nil, log, name, pass)
}

// runLoopStoppable is runLoop for a pass that is abandoned partway
// through when its loop stops being the leader.
//
// Leadership is otherwise only a gate on STARTING a pass: one that began
// as leader runs to completion on the loop's own context, so during a
// handover a demoted leader goes on working beside the new one. For the
// resolver that means committing and rolling back prepared transactions;
// for the applier, DDL statements on shards; for the stream monitor,
// status rows a new leader is writing too.
//
// Catalog fences do not cover this. The applier stamps its writes with
// the term it latched, so its CATALOG writes are refused -- but the
// statement already in flight on a shard is not a catalog write, and no
// fence reaches it. The resolver fences nothing at all.
//
// Safe because these passes hold no claim and persist their progress
// before each side effect: a cancelled pass is one that stopped early,
// which the next leader resumes from the row. It is the same property
// their recovery from process death rests on.
func runLoopStoppable(ctx context.Context, interval time.Duration, leader func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
	loop(ctx, interval, leader, leader, log, name, pass)
}

func loop(ctx context.Context, interval time.Duration, leader, stopOn func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
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
		watchPass(ctx, stallAfter, stopOn, log, name, pass)
	}
}

// watchPass runs one pass, reporting it if it stalls and, when stopOn is
// set and goes false, cancelling it. A nil stopOn never cancels.
func watchPass(ctx context.Context, stall time.Duration, stopOn func() bool, log func() *slog.Logger, name string, pass func(context.Context)) {
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
				if stopOn != nil && !stopOn() {
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
