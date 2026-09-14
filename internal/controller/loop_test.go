package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLoopReportsAPassThatNeverFinishes(t *testing.T) {
	out := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(out, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchPass(ctx, 10*time.Millisecond, nil, func() *slog.Logger { return log }, "wedged", func(ctx context.Context) {
			close(entered)
			<-ctx.Done()
		})
	}()
	<-entered

	deadline := time.After(2 * time.Second)
	for !strings.Contains(out.String(), "wedged pass is not finishing") {
		select {
		case <-deadline:
			t.Fatalf("a pass that never returned was never reported: %q", out.String())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestLoopStopsReportingOnceAPassReturns(t *testing.T) {
	out := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(out, nil))
	watchPass(context.Background(), 10*time.Millisecond, nil, func() *slog.Logger { return log }, "quick", func(context.Context) {})
	time.Sleep(50 * time.Millisecond)
	if strings.Contains(out.String(), "not finishing") {
		t.Fatalf("a pass that returned was reported as stalled: %q", out.String())
	}
}

func TestShardDialIsBounded(t *testing.T) {
	cfg, err := shardConnConfig("postgres://someone@shard-0:5432/postgres", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectTimeout != DefaultShardConnectTimeout {
		t.Fatalf("connect timeout = %v, want %v", cfg.ConnectTimeout, DefaultShardConnectTimeout)
	}

	cfg, err = shardConnConfig("postgres://someone@shard-0:5432/postgres?connect_timeout=3", "other", "ddl", "pw")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectTimeout != 3*time.Second {
		t.Fatalf("connect timeout = %v, want the DSN's 3s", cfg.ConnectTimeout)
	}
	if cfg.Database != "other" || cfg.User != "ddl" || cfg.Password != "pw" {
		t.Fatalf("overrides lost: %s %s %s", cfg.Database, cfg.User, cfg.Password)
	}
}

// TestAPassIsCancelledWhenLeadershipIsLost. Leadership was checked only
// before a pass started, and the pass then ran to completion on the loop's
// own context. These passes write catalog state -- the applier applies
// DDL, the resolver commits and rolls back prepared transactions, the
// copier drives a reshard -- so a handover left the old leader and the new
// one writing at the same time, each believing it was alone.
func TestAPassIsCancelledWhenLeadershipIsLost(t *testing.T) {
	var leader atomic.Bool
	leader.Store(true)
	var buf syncBuffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	started := make(chan struct{})
	var passErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchPass(context.Background(), time.Minute, leader.Load, func() *slog.Logger { return log }, "worker",
			func(ctx context.Context) {
				close(started)
				<-ctx.Done()
				passErr = ctx.Err()
			})
	}()

	<-started
	leader.Store(false)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the pass was never cancelled after leadership was lost; it would go on writing beside the new leader")
	}
	if passErr == nil {
		t.Fatal("the pass's context was not cancelled")
	}
	if !strings.Contains(buf.String(), "no longer the leader") {
		t.Errorf("nothing said why the pass stopped: %s", buf.String())
	}
}

// TestAPassIsNotCancelledWhileStillTheLeader guards the other direction:
// a long pass that keeps its term must be left alone, or every pass longer
// than the leadership check would be killed a second after it began.
func TestAPassIsNotCancelledWhileStillTheLeader(t *testing.T) {
	var buf syncBuffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	var cancelled bool
	watchPass(context.Background(), time.Minute, func() bool { return true }, func() *slog.Logger { return log }, "worker",
		func(ctx context.Context) {
			time.Sleep(3 * leaderCheck)
			cancelled = ctx.Err() != nil
		})
	if cancelled {
		t.Error("a pass that kept its term was cancelled anyway")
	}
}

// TestClaimsAreHandedBackOnlyAfterThePassEnds. A claim is what keeps a
// successor off a workflow the old leader is still driving, and these
// passes are deliberately not cancelled on demotion -- so releasing the
// moment leadership ends would hand the workflow over while the old pass
// was still inside it. ownedExec stops that pass at its NEXT catalog
// write, which is not the same as having stopped.
func TestClaimsAreHandedBackOnlyAfterThePassEnds(t *testing.T) {
	var releaseErr error // always nil here; the failing release is the loop's to log, not this test's subject
	var leader atomic.Bool
	leader.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inPass := make(chan struct{})
	finish := make(chan struct{})
	var releasedDuringPass atomic.Bool
	var releases atomic.Int64
	var passRunning atomic.Bool

	go runLoopHoldingClaims(ctx, time.Millisecond, leader.Load, func() *slog.Logger { return slog.New(slog.DiscardHandler) },
		"claimer",
		func(context.Context) {
			passRunning.Store(true)
			select {
			case inPass <- struct{}{}:
			default:
			}
			<-finish
			passRunning.Store(false)
		},
		func(context.Context) (int64, error) {
			if passRunning.Load() {
				releasedDuringPass.Store(true)
			}
			return releases.Add(1), releaseErr
		})

	<-inPass
	leader.Store(false) // demoted while the pass is still running
	time.Sleep(50 * time.Millisecond)
	if releases.Load() != 0 {
		t.Fatal("the claim was handed back while the pass was still driving the workflow; a successor would claim it and drive it too")
	}
	close(finish)

	deadline := time.Now().Add(5 * time.Second)
	for releases.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if releases.Load() == 0 {
		t.Fatal("the claim was never handed back, so a successor waits out the lease")
	}
	if releasedDuringPass.Load() {
		t.Error("a hand-back overlapped a running pass")
	}
}
