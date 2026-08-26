package controller

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// countingHandler records how many log records a loop emitted.
type countingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }
func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// TestOnlyTheLeaderRunsTheMutatingLoops: the copier, placer, resolver,
// stream monitor and barrier recovery ran on every replica. They create and
// drop replication objects, move data, rename tables and commit prepared
// transactions, so two controllers doing that at once produce half-built
// replication, failed cutovers and inconsistent workflow state.
//
// The pool here points at a closed port, so a pass that runs fails and logs
// and a pass that is correctly skipped is silent. That distinguishes the two
// without a database.
func TestOnlyTheLeaderRunsTheMutatingLoops(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/pgshard?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	loops := map[string]func(ctx context.Context, logger *slog.Logger, leader func() bool){
		"copier": func(ctx context.Context, l *slog.Logger, leader func() bool) {
			(&Copier{Pool: pool, Logger: l}).Run(ctx, time.Millisecond, leader)
		},
		"placer": func(ctx context.Context, l *slog.Logger, leader func() bool) {
			(&Placer{Pool: pool, Logger: l}).Run(ctx, time.Millisecond, leader)
		},
		"resolver": func(ctx context.Context, l *slog.Logger, leader func() bool) {
			(&Resolver{Pool: pool, Logger: l}).Run(ctx, time.Millisecond, leader)
		},
		"stream monitor": func(ctx context.Context, l *slog.Logger, leader func() bool) {
			(&StreamMonitor{Pool: pool, Logger: l}).Run(ctx, time.Millisecond, leader)
		},
		"barrier recovery": func(ctx context.Context, l *slog.Logger, leader func() bool) {
			(&Barrier{Store: &PGBarrierStore{Pool: pool}, Logger: l}).RunRecovery(ctx, time.Millisecond, leader)
		},
	}

	for name, run := range loops {
		t.Run(name, func(t *testing.T) {
			follower := &countingHandler{}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			run(ctx, slog.New(follower), func() bool { return false })
			cancel()
			if n := follower.count(); n != 0 {
				t.Fatalf("a follower ran %d %s passes; only the leader may", n, name)
			}

			leader := &countingHandler{}
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			run(ctx, slog.New(leader), func() bool { return true })
			cancel()
			if leader.count() == 0 {
				t.Fatalf("the leader ran no %s pass, so the follower check proves nothing", name)
			}
		})
	}
}
