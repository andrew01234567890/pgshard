package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeListener records what the drainer did before and during Shutdown.
type fakeListener struct {
	mu            sync.Mutex
	shutdowns     int
	readyAtCall   bool
	stateAtCall   DrainState
	deadlineAtCal time.Duration
	err           error
	block         chan struct{}
	d             *Drainer
}

func (f *fakeListener) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	f.shutdowns++
	f.readyAtCall = f.d.Ready()
	f.stateAtCall = f.d.State()
	if dl, ok := ctx.Deadline(); ok {
		f.deadlineAtCal = time.Until(dl)
	}
	f.mu.Unlock()
	if f.block != nil {
		<-f.block
	}
	return f.err
}

func TestDrainerSequence(t *testing.T) {
	fl := &fakeListener{}
	d := NewDrainer(fl, 5*time.Second, 30*time.Second)
	fl.d = d
	var slept time.Duration
	var readyDuringDelay bool
	var stateDuringDelay DrainState
	d.sleep = func(_ context.Context, dur time.Duration) {
		slept = dur
		readyDuringDelay = d.Ready()
		stateDuringDelay = d.State()
	}
	if !d.Ready() || d.State() != DrainServing {
		t.Fatal("must start ready")
	}
	if err := d.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if slept != 5*time.Second || readyDuringDelay || stateDuringDelay != DrainNotReady {
		t.Fatalf("delay: slept %s ready %v state %s", slept, readyDuringDelay, stateDuringDelay)
	}
	if fl.shutdowns != 1 || fl.readyAtCall || fl.stateAtCall != DrainDraining {
		t.Fatalf("shutdown: %+v", fl)
	}
	if fl.deadlineAtCal < 29*time.Second || fl.deadlineAtCal > 30*time.Second {
		t.Fatalf("shutdown deadline %s, want the drain timeout", fl.deadlineAtCal)
	}
	if d.State() != DrainStopped || d.Ready() {
		t.Fatalf("after drain: %s", d.State())
	}
	if err := d.Drain(context.Background()); err != nil || fl.shutdowns != 1 {
		t.Fatal("second Drain must be a no-op")
	}
}

func TestDrainerReportsShutdownErrorAndSkipsDelayOnCancel(t *testing.T) {
	fl := &fakeListener{err: errors.New("forced")}
	d := NewDrainer(fl, time.Hour, time.Second)
	fl.d = d
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := d.Drain(ctx); err == nil || err.Error() != "forced" {
		t.Fatalf("err %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancelled context must skip the delay")
	}
}

func TestDrainerHandler(t *testing.T) {
	fl := &fakeListener{block: make(chan struct{})}
	d := NewDrainer(fl, 0, time.Second)
	fl.d = d
	srv := httptest.NewServer(d.Handler())
	defer srv.Close()
	get := func(path string) (int, string) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		buf := make([]byte, 64)
		n, _ := resp.Body.Read(buf)
		return resp.StatusCode, string(buf[:n])
	}
	if code, _ := get("/readyz"); code != http.StatusOK {
		t.Fatalf("readyz while serving: %d", code)
	}
	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Fatalf("healthz: %d", code)
	}
	done := make(chan struct{})
	go func() { _ = d.Drain(context.Background()); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for d.State() != DrainDraining && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if code, body := get("/readyz"); code != http.StatusServiceUnavailable || body != "draining\n" {
		t.Fatalf("readyz while draining: %d %q", code, body)
	}
	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Fatal("healthz must stay 200 while draining")
	}
	close(fl.block)
	<-done
	if code, body := get("/readyz"); code != http.StatusServiceUnavailable || body != "stopped\n" {
		t.Fatalf("readyz after drain: %d %q", code, body)
	}
}

// TestReadyRequiresACatalogSnapshot guards a router that reports ready while it
// cannot route. With no snapshot every request carries generation zero and the
// pooler refuses it, so Kubernetes must be told to keep traffic away rather
// than sent statements that are certain to fail.
func TestReadyRequiresACatalogSnapshot(t *testing.T) {
	d := NewDrainer(&fakeListener{}, 0, 0)
	if !d.Ready() {
		t.Fatal("a serving drainer with no Routable check must be ready")
	}

	routable := false
	d.Routable = func() bool { return routable }
	if d.Ready() {
		t.Error("a router with no catalog snapshot must not be ready")
	}
	routable = true
	if !d.Ready() {
		t.Error("a router that has loaded a snapshot must be ready")
	}

	// Draining still wins: a snapshot does not make a draining router ready.
	d.state.Store(int32(DrainDraining))
	if d.Ready() {
		t.Error("a draining router must not be ready whatever its snapshot")
	}
}
