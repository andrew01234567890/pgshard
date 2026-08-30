package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
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
		watchPass(ctx, 10*time.Millisecond, func() *slog.Logger { return log }, "wedged", func(ctx context.Context) {
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
	watchPass(context.Background(), 10*time.Millisecond, func() *slog.Logger { return log }, "quick", func(context.Context) {})
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
