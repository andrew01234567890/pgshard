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
	// OnLeader, when set, observes leadership changes.
	OnLeader func(leader bool)
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

// Run tries to become leader and reconciles until ctx is done. A lost
// connection drops leadership; Run then campaigns again.
func (r *Reconciler) Run(ctx context.Context) error {
	_, _, retry, logger := r.settings()
	for {
		err := r.lead(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, errNotLeader) {
			logger.Warn("controller leadership ended", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
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
	if r.OnLeader != nil {
		r.OnLeader(true)
		defer r.OnLeader(false)
	}
	logger.Info("controller is leader")
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
