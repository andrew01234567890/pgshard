package router

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

func TestDecideFailoverTable(t *testing.T) {
	cases := []struct {
		name                       string
		trigger, inTxn, outputSent bool
		buffered, capacity         int
		want                       failoverAction
	}{
		{"no trigger", false, false, false, 0, 4, failoverPass},
		{"no trigger in txn", false, true, false, 0, 4, failoverPass},
		{"clean statement waits", true, false, false, 0, 4, failoverWait},
		{"last slot waits", true, false, false, 3, 4, failoverWait},
		{"output already sent passes", true, false, true, 0, 4, failoverPass},
		{"output sent in txn passes", true, true, true, 0, 4, failoverPass},
		{"in txn fails 40001", true, true, false, 0, 4, failoverFailTxn},
		{"in txn even when full", true, true, false, 4, 4, failoverFailTxn},
		{"cap reached refuses", true, false, false, 4, 4, failoverRefuse},
		{"over cap refuses", true, false, false, 9, 4, failoverRefuse},
	}
	for _, c := range cases {
		if got := decideFailover(c.trigger, c.inTxn, c.outputSent, c.buffered, c.capacity); got != c.want {
			t.Errorf("%s: got %d want %d", c.name, got, c.want)
		}
	}
}

func TestIsFailover(t *testing.T) {
	if !isFailover(pgwire.Errorf("55000", "stale")) {
		t.Fatal("55000 must trigger")
	}
	if !isFailover(&refusedError{pgwire.Errorf("08006", "refused")}) {
		t.Fatal("refused connection must trigger")
	}
	if isFailover(pgwire.Errorf("08006", "lost mid-statement")) {
		t.Fatal("a stream lost after sending must not trigger: the statement may have run")
	}
	if isFailover(nil) || isFailover(errors.New("x")) || isFailover(pgwire.Errorf("42P01", "no")) {
		t.Fatal("unrelated errors must not trigger")
	}
	var pe *pgwire.Error
	if !errors.As(&refusedError{pgwire.Errorf("08006", "refused")}, &pe) || pe.Code != "08006" {
		t.Fatal("refusedError must unwrap to the client-visible 08006")
	}
}

func (h *harness) fence(state string) *snapshot.Snapshot {
	prev := h.snap()
	next := *prev
	next.Serving = map[snapshot.ShardKey]snapshot.Serving{}
	for k, v := range prev.Serving {
		v.State = state
		next.Serving[k] = v
	}
	h.setSnap(&next)
	return prev
}

func TestFencedShardBuffersUntilServing(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	serving := h.fence("fenced")
	go func() {
		time.Sleep(250 * time.Millisecond)
		h.setSnap(serving)
	}()
	start := time.Now()
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("buffered select: %v", err)
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Fatalf("select was not held during the fence (%s)", d)
	}
	if h.r.Buffered(Shard{Set: DefaultShardSet}) != 0 {
		t.Fatal("buffer count not released")
	}
}

func TestFencedShardTimesOutWithoutPrimary(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	h.fence("migrating")
	start := time.Now()
	_, err := conn.Exec(context.Background(), "select 1")
	if sqlstate(err) != "08006" {
		t.Fatalf("expired buffer: %v", err)
	}
	if d := time.Since(start); d < 700*time.Millisecond {
		t.Fatalf("returned after %s, before the buffer window", d)
	}
}

func TestFencedShardInsideTransactionIs40001(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	serving := h.fence("fenced")
	start := time.Now()
	_, err := conn.Exec(ctx, "select 1")
	if sqlstate(err) != "40001" || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("in-transaction statement during fence: %v after %s", err, time.Since(start))
	}
	if conn.PgConn().TxStatus() != 'I' {
		t.Fatalf("the aborted transaction must be gone, status %c", conn.PgConn().TxStatus())
	}
	h.setSnap(serving)
	if _, err := conn.Exec(ctx, "rollback"); err != nil {
		t.Fatalf("rollback after failover: %v", err)
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after failover: %v", err)
	}
}

func TestStaleGenerationInsideTransactionIs40001(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	fresh := h.snap()
	stale := *fresh
	stale.ShardMapGeneration = 6
	h.setSnap(&stale)
	if _, err := conn.Exec(ctx, "select 1"); sqlstate(err) != "40001" {
		t.Fatalf("stale generation inside transaction: %v", err)
	}
	h.setSnap(fresh)
	if _, err := conn.Exec(ctx, "rollback"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestStaleGenerationRetriesAfterSnapshotChange(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	fresh := h.snap()
	stale := *fresh
	stale.ShardMapGeneration = 6
	h.setSnap(&stale)
	go func() {
		time.Sleep(150 * time.Millisecond)
		h.setSnap(fresh)
	}()
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("select across generation change: %v", err)
	}
}

// TestReacquireWaitsForThePoolerToDetach covers the window abort leaves open:
// it cancels this side of the RPC without waiting for the pooler, which
// refuses a second Execute stream while the session is still attached. The
// retry must wait for the release instead of reporting a dead connection.
func TestReacquireWaitsForThePoolerToDetach(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	hold := make(chan struct{})
	h.fp.mu.Lock()
	h.fp.holdDetach = hold
	h.fp.mu.Unlock()
	refused := make(chan struct{})
	h.fp.mu.Lock()
	h.fp.fenced = refused
	h.fp.mu.Unlock()
	fresh := h.snap()
	stale := *fresh
	stale.ShardMapGeneration = 6
	h.setSnap(&stale)
	// Publish the fresh map only once the pooler has actually refused the
	// stale one: a timer could let the statement through before the stream
	// was ever dropped, and the test would pass without exercising anything.
	go func() {
		select {
		case <-refused:
		case <-time.After(5 * time.Second):
		}
		h.setSnap(fresh)
	}()
	// Let the aborted handler finish only once the router has asked for the
	// release. A router that reacquires without asking gets past this and
	// the assertion below reports what the client actually saw.
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			h.fp.mu.Lock()
			released := len(h.fp.releases) > 0
			h.fp.mu.Unlock()
			if released {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		close(hold)
	}()
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("retry across the abort window: %v", err)
	}
}

func TestBufferCapRefusesWith53300(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.fence("fenced")
	conns := []*pgx.Conn{h.connect(t, h.dsn("app", "secret", "app")), h.connect(t, h.dsn("app", "secret", "app"))}
	errc := make(chan error, 2)
	for _, c := range conns {
		go func() { _, err := c.Exec(ctx, "select 1"); errc <- err }()
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.r.Buffered(Shard{Set: DefaultShardSet}) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	third := h.connect(t, h.dsn("app", "secret", "app"))
	if _, err := third.Exec(ctx, "select 1"); sqlstate(err) != "53300" {
		t.Fatalf("over cap: %v", err)
	}
	for range conns {
		if err := <-errc; sqlstate(err) != "08006" {
			t.Fatalf("buffered statement: %v", err)
		}
	}
}

func TestStreamLossAfterSendIsNotRetried(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	h.fp.dropAfter = "select 1"
	_, err := conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol)
	if sqlstate(err) != "08006" {
		t.Fatalf("stream loss after output: %v", err)
	}
	h.fp.mu.Lock()
	dropped := h.fp.dropped
	h.fp.mu.Unlock()
	if dropped != 1 {
		t.Fatalf("statement was retried: dropped=%d", dropped)
	}
}

func TestClientCancelWhileBuffered(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	h.fence("fenced")
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = conn.PgConn().CancelRequest(ctx)
	}()
	start := time.Now()
	if _, err := conn.Exec(ctx, "select 1"); sqlstate(err) != "57014" || time.Since(start) > 600*time.Millisecond {
		t.Fatalf("cancel while buffered: %v after %s", err, time.Since(start))
	}
}

func TestOutputAlreadySentPassesFailoverErrorThrough(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	start := time.Now()
	_, err := conn.Exec(context.Background(), "select midrow_stale", pgx.QueryExecModeSimpleProtocol)
	if sqlstate(err) != "55000" {
		t.Fatalf("stale after output: %v", err)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("statement with output was buffered for %s", d)
	}
}

func TestRefusedPoolerWhileServingUsesTransportWindow(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()
	h := newHarnessWith(t, newFakePooler(), deadAddr, func(c *Config) { c.Buffering.TransportWindow = 100 * time.Millisecond })
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	start := time.Now()
	_, err = conn.Exec(context.Background(), "select 1")
	if sqlstate(err) != "08006" {
		t.Fatalf("refused pooler: %v", err)
	}
	if d := time.Since(start); d < 100*time.Millisecond || d > 500*time.Millisecond {
		t.Fatalf("waited %s; want the transport window, not the failover window", d)
	}
}

func TestRetryWindowTable(t *testing.T) {
	h := newHarness(t)
	sh := Shard{Set: DefaultShardSet}
	refused := &refusedError{pgwire.Errorf("08006", "refused")}
	if got := h.r.retryWindow(sh, refused); got != h.r.cfg.Buffering.TransportWindow {
		t.Fatalf("refused while serving: %s", got)
	}
	if got := h.r.retryWindow(sh, pgwire.Errorf(codeStaleGeneration, "stale")); got != h.r.cfg.Buffering.Window {
		t.Fatalf("stale generation: %s", got)
	}
	h.fence("fenced")
	if got := h.r.retryWindow(sh, refused); got != h.r.cfg.Buffering.Window {
		t.Fatalf("refused while fenced: %s", got)
	}
}
