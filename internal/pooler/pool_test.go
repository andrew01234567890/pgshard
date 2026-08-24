package pooler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestPoolBudgetCapsBackends(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 2, MaxPerRole: 2, AcquireTimeout: 100 * time.Millisecond}, pg.dial)
	ctx := context.Background()
	a, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, "db", "bob", nil, nil); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("third backend must exceed the shard budget: %v", err)
	}
	if live, _ := p.Stats(); live != 2 {
		t.Fatalf("live = %d", live)
	}
	p.Release(a)
	c, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil || c != a {
		t.Fatalf("idle backend must be reused: %v %v", c, err)
	}
	p.Release(b)
	if _, err := p.Acquire(ctx, "db", "bob", nil, nil); err != nil {
		t.Fatalf("bob must evict alice's idle backend: %v", err)
	}
	if d := pg.dials.Load(); d != 3 {
		t.Fatalf("dials = %d, want 3", d)
	}
}

func TestPoolPerRoleCapPreventsStarvation(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 3, MaxPerRole: 2, AcquireTimeout: 100 * time.Millisecond}, pg.dial)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := p.Acquire(ctx, "db", "hot", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Acquire(ctx, "db", "hot", nil, nil); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("hot role must be capped at MaxPerRole: %v", err)
	}
	if _, err := p.Acquire(ctx, "db", "quiet", nil, nil); err != nil {
		t.Fatalf("quiet role must still get a backend: %v", err)
	}
	if live, _ := p.Stats(); live != 3 {
		t.Fatalf("live = %d", live)
	}
}

func TestPoolWaitsForSlotThenProceeds(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 1, AcquireTimeout: 2 * time.Second}, pg.dial)
	ctx := context.Background()
	a, _ := p.Acquire(ctx, "db", "alice", nil, nil)
	got := make(chan error, 1)
	go func() {
		_, err := p.Acquire(ctx, "db", "bob", nil, nil)
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	p.Release(a)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never woke")
	}
}

func TestPoolExpiryAndClose(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 2, MaxLifetime: 10 * time.Millisecond}, pg.dial)
	ctx := context.Background()
	a, _ := p.Acquire(ctx, "db", "alice", nil, nil)
	p.Release(a)
	time.Sleep(20 * time.Millisecond)
	b, _ := p.Acquire(ctx, "db", "alice", nil, nil)
	if b == a {
		t.Fatal("expired backend must not be reused")
	}
	if pg.dials.Load() != 2 {
		t.Fatalf("dials = %d", pg.dials.Load())
	}
	c, _ := p.Acquire(ctx, "db", "alice", nil, nil)
	p.Release(b)
	p.Close()
	if _, idle := p.Stats(); idle != 0 {
		t.Fatal("idle backends must be closed")
	}
	p.Release(c)
	if live, _ := p.Stats(); live != 0 {
		t.Fatalf("live = %d after close", live)
	}
	if _, err := p.Acquire(ctx, "db", "alice", nil, nil); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v", err)
	}
	if b.conn != nil || c.conn != nil {
		t.Fatal("released backends must be closed after Close")
	}
}

func TestPoolNeverReusesBackendWithUnflushedMessages(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 2}, pg.dial)
	ctx := context.Background()
	a, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.send(&pgproto3.Sync{}) // buffered, never flushed
	p.Release(a)
	if _, idle := p.Stats(); idle != 0 {
		t.Fatal("a backend holding unflushed messages must not return to the idle set")
	}
	b, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b == a {
		t.Fatal("backend with unflushed messages was reused")
	}
	if pg.dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2 (a fresh backend)", pg.dials.Load())
	}
}

func TestPoolHooksObserveDialsAndWaits(t *testing.T) {
	pg := newFakePG()
	var dials, dialErrs, waits int
	cfg := PoolConfig{MaxBackends: 1, MaxPerRole: 1, AcquireTimeout: 50 * time.Millisecond,
		OnDial: func(err error) {
			if err != nil {
				dialErrs++
			} else {
				dials++
			}
		},
		OnWait: func() { waits++ }}
	p := newPool(cfg, pg.dial)
	ctx := context.Background()
	a, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dials != 1 || waits != 0 {
		t.Fatalf("dials=%d waits=%d after first acquire", dials, waits)
	}
	if _, err := p.Acquire(ctx, "db", "alice", nil, nil); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("full pool must exhaust the budget: %v", err)
	}
	if waits != 1 {
		t.Fatalf("waits = %d, want 1", waits)
	}
	p.Release(a)
	if dialErrs != 0 {
		t.Fatalf("dialErrs = %d", dialErrs)
	}
}
