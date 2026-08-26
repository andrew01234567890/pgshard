package router

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// DrainState is where a Drainer is in the shutdown sequence.
type DrainState int32

// Drain states, in order.
const (
	// DrainServing: ready, accepting connections.
	DrainServing DrainState = iota
	// DrainNotReady: readiness reports false while endpoints propagate; the
	// listener is still open so late-arriving connections are served.
	DrainNotReady
	// DrainDraining: the listener is closed; sessions are being drained.
	DrainDraining
	// DrainStopped: every session is gone or was forced.
	DrainStopped
)

func (s DrainState) String() string {
	switch s {
	case DrainServing:
		return "serving"
	case DrainNotReady:
		return "not-ready"
	case DrainDraining:
		return "draining"
	case DrainStopped:
		return "stopped"
	}
	return "unknown"
}

// Shutdowner is the listener side of a drain: pgwire.Server.Shutdown.
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

// Drainer sequences a graceful stop: readiness flips false, a delay lets
// load balancers stop sending new connections, then the listener closes and
// sessions drain until Timeout, after which they are forced.
type Drainer struct {
	srv     Shutdowner
	delay   time.Duration
	timeout time.Duration
	state   atomic.Int32
	// Routable reports whether the router can actually route: it has a
	// catalog snapshot. Without one every statement is refused for a stale
	// generation, so reporting ready would have Kubernetes send traffic
	// here that cannot be served. Nil means the check does not apply.
	Routable func() bool
	// sleep is replaceable so tests need no wall clock.
	sleep func(ctx context.Context, d time.Duration)
}

// NewDrainer builds a Drainer around srv.
func NewDrainer(srv Shutdowner, delay, timeout time.Duration) *Drainer {
	return &Drainer{srv: srv, delay: delay, timeout: timeout, sleep: sleepCtx}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// State reports the current drain state.
func (d *Drainer) State() DrainState { return DrainState(d.state.Load()) }

// Ready reports whether new connections should be sent here.
func (d *Drainer) Ready() bool {
	if d.State() != DrainServing {
		return false
	}
	return d.Routable == nil || d.Routable()
}

// Drain runs the sequence once; ctx cancellation skips the delay but the
// session drain still gets its full timeout. It returns Shutdown's error.
func (d *Drainer) Drain(ctx context.Context) error {
	if !d.state.CompareAndSwap(int32(DrainServing), int32(DrainNotReady)) {
		return nil
	}
	d.sleep(ctx, d.delay)
	d.state.Store(int32(DrainDraining))
	sctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	err := d.srv.Shutdown(sctx)
	d.state.Store(int32(DrainStopped))
	return err
}

// Handler serves /readyz (200 while serving, 503 otherwise) and /healthz
// (200 while the process is up).
func (d *Drainer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if d.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		if d.State() == DrainServing {
			_, _ = w.Write([]byte("no catalog snapshot\n"))
			return
		}
		_, _ = w.Write([]byte(d.State().String() + "\n"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
