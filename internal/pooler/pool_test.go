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

func TestPoolPerRoleCapIsGlobalAcrossDatabases(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 8, MaxPerRole: 2, AcquireTimeout: 100 * time.Millisecond}, pg.dial)
	ctx := context.Background()
	if _, err := p.Acquire(ctx, "db1", "hot", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, "db2", "hot", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, "db3", "hot", nil, nil); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("a role must not get MaxPerRole backends per database: %v", err)
	}
	if _, err := p.Acquire(ctx, "db3", "quiet", nil, nil); err != nil {
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

func TestIdleReuseRequiresMatchingSCRAMKeys(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 4, AcquireTimeout: 100 * time.Millisecond}, pg.dial)
	ctx := context.Background()
	keysA := [][]byte{[]byte("client-key-a"), []byte("server-key-a")}
	a, err := p.Acquire(ctx, "db", "alice", keysA[0], keysA[1])
	if err != nil {
		t.Fatal(err)
	}
	p.Release(a)
	stolen, err := p.Acquire(ctx, "db", "alice", []byte("client-key-b"), []byte("server-key-b"))
	if err != nil {
		t.Fatal(err)
	}
	if stolen == a {
		t.Fatal("idle backend was handed to a session with different SCRAM keys")
	}
	if d := pg.dials.Load(); d != 2 {
		t.Fatalf("dials = %d, want 2 (mismatched keys must dial fresh)", d)
	}
	p.Release(stolen)
	b, err := p.Acquire(ctx, "db", "alice", keysA[0], keysA[1])
	if err != nil {
		t.Fatal(err)
	}
	if d := pg.dials.Load(); d != 3 {
		t.Fatalf("dials = %d, want 3 (backend for stale keys must not be reused)", d)
	}
	p.Release(b)
	c, err := p.Acquire(ctx, "db", "alice", keysA[0], keysA[1])
	if err != nil || c != b {
		t.Fatalf("matching keys must reuse the idle backend: %v %v", c, err)
	}
}

// TestAcquireTakesABackendReleasedWhileItWaits: a released backend keeps
// its per-role slot and joins the idle list, so nothing frees the
// semaphore. A waiter watching the semaphore alone therefore sat until its
// acquire timeout while a backend it could have used was idle -- a
// five-second stall on a healthy pool, reported to the client as no
// backend available.
func TestAcquireTakesABackendReleasedWhileItWaits(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 4, MaxPerRole: 1, AcquireTimeout: 5 * time.Second}, pg.dial)
	ctx := context.Background()
	held, err := p.Acquire(ctx, "db", "alice", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	p.cfg.OnWait = func() { close(waiting) }

	got := make(chan *Backend, 1)
	errc := make(chan error, 1)
	go func() {
		b, err := p.Acquire(ctx, "db", "alice", nil, nil)
		if err != nil {
			errc <- err
			return
		}
		got <- b
	}()
	<-waiting
	start := time.Now()
	p.Release(held)

	select {
	case b := <-got:
		if b != held {
			t.Fatalf("waiter got a different backend: it should have reused the released one")
		}
		// Anything near the acquire timeout means it timed out and
		// retried rather than being woken by the release.
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("waiter took %s to see a backend released the moment it started waiting", waited)
		}
	case err := <-errc:
		t.Fatalf("waiter failed while a backend was idle: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("waiter never saw the released backend")
	}
	if d := pg.dials.Load(); d != 1 {
		t.Fatalf("dials = %d, want the released backend reused rather than a new one", d)
	}
}

// TestEvictionTakesTheColdestIdleBackend: the idle list is LIFO, so its
// last entry is the warmest backend there is -- the one with the hottest
// prepared statements and the one most likely to be wanted next. Eviction
// took exactly that one, from whichever role pool the map happened to
// yield first, while older connections sat untouched.
func TestEvictionTakesTheColdestIdleBackend(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 3, MaxPerRole: 3, AcquireTimeout: time.Second}, pg.dial)
	defer p.Close()
	ctx := context.Background()

	var held []*Backend
	for _, role := range []string{"alice", "bob", "carol"} {
		b, err := p.Acquire(ctx, "db", role, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, b)
	}
	// Released oldest first, and each release stamps lastUsed, so alice's
	// is the coldest and carol's the warmest.
	for i, b := range held {
		b.lastUsed = time.Now().Add(time.Duration(i-len(held)) * time.Minute)
		p.Release(b)
	}

	// A fourth role has to displace one of them: it must be the coldest.
	d, err := p.Acquire(ctx, "db", "dave", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release(d)
	for _, role := range []string{"bob", "carol"} {
		b, err := p.Acquire(ctx, "db", role, nil, nil)
		if err != nil {
			t.Fatalf("%s should still have its idle backend: %v", role, err)
		}
		if b != held[1] && b != held[2] {
			t.Errorf("%s got a fresh backend, so a warm one was evicted", role)
		}
		p.Release(b)
	}
	if d := pg.dials.Load(); d != 4 {
		t.Errorf("dials = %d, want 4: evicting the coldest costs exactly one redial", d)
	}
}

// TestIdleBackendsAreReapedWhileQuiet: an idle lifetime used to be noticed
// only by the next acquire, so the connections a spike created stayed for
// as long as the pool was quiet -- which is exactly when nothing arrives
// to notice them.
func TestIdleBackendsAreReapedWhileQuiet(t *testing.T) {
	pg := newFakePG()
	p := newPool(PoolConfig{MaxBackends: 4, MaxPerRole: 4, MaxIdleTime: 50 * time.Millisecond, AcquireTimeout: time.Second}, pg.dial)
	defer p.Close()
	ctx := context.Background()
	for _, role := range []string{"alice", "bob"} {
		b, err := p.Acquire(ctx, "db", role, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		p.Release(b)
	}
	if _, idle := p.Stats(); idle != 2 {
		t.Fatalf("idle = %d, want the two just released", idle)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, idle := p.Stats(); idle == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, idle := p.Stats()
	t.Fatalf("%d backends still idle well past MaxIdleTime with nothing touching the pool", idle)
}
