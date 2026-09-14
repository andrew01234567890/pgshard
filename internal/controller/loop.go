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

// runLoopHoldingClaims drives pass on every tick while leader() holds,
// for the loops that take a workflow claim, and hands those claims back
// once the term is lost.
//
// The pass is NOT abandoned when the term drops, unlike runLoopStoppable:
// the claim is what keeps a successor off a workflow this pass is still
// driving, so the pass has to be allowed to finish.
//
// The order is the whole point. A claim is otherwise surrendered only by
// expiry, so a successor waits out DefaultOwnerLease before touching a
// workflow -- five minutes of a reshard standing still. But releasing it
// the moment leadership ends would be worse: these passes are deliberately
// not cancelled (see runLoop), so the old leader is still inside
// driveCutover, and a successor that claimed then would drive the same
// reshard beside it. ownedExec stops the old pass at its next catalog
// write, which is not the same as having stopped -- the shard-side
// statements in between reach no fence at all.
//
// So the release happens HERE, after the pass has returned: the old driver
// has finished, and only then does the workflow become claimable.
func runLoopHoldingClaims(ctx context.Context, interval time.Duration, leader func() bool, log func() *slog.Logger, name string, pass func(context.Context), release func(context.Context) (int64, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	held := false
	for {
		select {
		case <-ctx.Done():
			// Shutdown, and the pass is not running: this select is
			// between ticks. Handing back here rather than from the
			// leadership callback keeps the release sequenced after the
			// pass in every case, and does not depend on that goroutine
			// winning a race against pool.Close and process exit.
			if held {
				handBack(ctx, log, name, release)
			}
			return
		case <-t.C:
		}
		if leader != nil && !leader() {
			if held {
				held = false
				handBack(ctx, log, name, release)
			}
			continue
		}
		held = true
		watchPass(ctx, stallAfter, nil, log, name, pass)
		if leader != nil && !leader() {
			held = false
			handBack(ctx, log, name, release)
		}
	}
}

// handBack releases on a context of its own: the usual reason it runs is
// that the loop's context has just been cancelled, and a shutdown must not
// wait on the catalog.
func handBack(ctx context.Context, log func() *slog.Logger, name string, release func(context.Context) (int64, error)) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	switch n, err := release(rctx); {
	case err != nil:
		if l := log(); l != nil {
			l.Warn(name+": could not hand back its workflows; a successor waits out the lease", "err", err)
		}
	case n > 0:
		if l := log(); l != nil {
			l.Info(name+" handed its workflows back", "workflows", n)
		}
	}
}

// releaseTimeout bounds that hand-back.
const releaseTimeout = 5 * time.Second

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
