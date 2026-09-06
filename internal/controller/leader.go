package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// LeaderLockKey is the pg_advisory_lock key one controller holds while it
// is the leader.
const LeaderLockKey int64 = 0x7067736861726443

// Reconciler runs the reconcile loop as long as it holds the leader lock.
type Reconciler struct {
	DSN    string
	Logger *slog.Logger
	// LockKey defaults to LeaderLockKey.
	LockKey int64
	// Interval bounds the time between passes when no notification arrives.
	Interval time.Duration
	// RetryInterval is the wait between leadership attempts.
	RetryInterval time.Duration
	// OnResult, when set, observes every completed pass.
	OnResult func(Result)
	// OnLeader, when set, observes leadership changes. term is the
	// leadership term just taken, and 0 when leadership ends.
	OnLeader func(leader bool, term int64)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (r *Reconciler) settings() (int64, time.Duration, time.Duration, *slog.Logger) {
	key, interval, retry, logger := r.LockKey, r.Interval, r.RetryInterval, r.Logger
	if key == 0 {
		key = LeaderLockKey
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if retry <= 0 {
		retry = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return key, interval, retry, logger
}

var errNotLeader = errors.New("controller: leader lock held elsewhere")

// leaderWaitLogInterval is how often a controller that cannot take leadership
// says so. Silence here is indistinguishable from a controller that is working,
// which has cost real time: a run was read as a stalled reshard when the
// controller had simply not been leader for the whole window.
const leaderWaitLogInterval = time.Minute

// Run tries to become leader and reconciles until ctx is done. A lost
// connection drops leadership; Run then campaigns again, saying so
// periodically rather than waiting in silence.
func (r *Reconciler) Run(ctx context.Context) error {
	_, _, retry, logger := r.settings()
	var waitingSince time.Time
	var nextWaitLog time.Time
	for {
		err := r.lead(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, errNotLeader):
			now := r.now()
			if waitingSince.IsZero() {
				waitingSince, nextWaitLog = now, now
			}
			if !now.Before(nextWaitLog) {
				logger.Info("another controller holds leadership; waiting",
					"waiting", now.Sub(waitingSince).Round(time.Second))
				nextWaitLog = now.Add(leaderWaitLogInterval)
			}
		default:
			waitingSince, nextWaitLog = time.Time{}, time.Time{}
			logger.Warn("controller leadership ended", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Reconciler) lead(ctx context.Context) error {
	key, interval, _, logger := r.settings()
	conn, err := pgx.Connect(ctx, r.DSN)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return errNotLeader
	}
	// Taken before the first pass and only once the previous holder's
	// connection is gone, so a bumped term is proof that the controller
	// which held it no longer leads. The workers stamp their writes with
	// it, which is what stops a pass that lost the lock part-way through:
	// the in-process flag below is read between ticks, and says "leader"
	// for the rest of a pass that is no longer one.
	term, err := catalog.TakeLeaderTerm(ctx, conn)
	if err != nil {
		return err
	}
	if r.OnLeader != nil {
		r.OnLeader(true, term)
		defer r.OnLeader(false, 0)
	}
	logger.Info("controller is leader", "term", term)
	for _, ch := range []string{catalog.DesiredChannel, catalog.ServingChannel} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}
	for {
		res, err := Reconcile(ctx, conn, logger)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			logger.Error("reconcile pass failed", "err", err)
			if conn.IsClosed() {
				return err
			}
		} else if r.OnResult != nil {
			r.OnResult(res)
		}
		if err := waitForChange(ctx, conn, interval); err != nil {
			return err
		}
	}
}

// waitForChange blocks until a notification arrives or interval passes; a
// timeout leaves the connection usable.
func waitForChange(ctx context.Context, conn *pgx.Conn, interval time.Duration) error {
	wctx, cancel := context.WithTimeout(ctx, interval)
	defer cancel()
	_, err := conn.WaitForNotification(wctx)
	switch {
	case err == nil, errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		return nil
	default:
		return err
	}
}
