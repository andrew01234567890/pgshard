package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Health is the view of the instance the probes need; Instance satisfies
// it and tests use fakes.
type Health interface {
	// Started reports whether the postmaster answers connection attempts,
	// even with "the database system is starting up".
	Started(ctx context.Context) error
	// IsPrimary reports the current role.
	IsPrimary() bool
	// ReplayLagBytes returns the streaming lag on a standby.
	ReplayLagBytes(ctx context.Context) (int64, error)
	// PrimaryAcceptsWrites reports SELECT NOT pg_is_in_recovery().
	PrimaryAcceptsWrites(ctx context.Context) error
}

// Probes serves the kubelet HTTP endpoints.
type Probes struct {
	Health      Health
	MaxLagBytes int64
	// KubeReachable reports whether the API server answers; nil when the
	// agent runs without a kube API.
	KubeReachable func(ctx context.Context) bool
	Peers         []string
	Client        *http.Client
	// Fenced is called when a primary decides it is isolated.
	Fenced func()
	// IsolationGrace is how long a primary must stay isolated (kube API and
	// every peer unreachable on consecutive probes) before it fences; a
	// single slow probe under load must not take the primary down. Zero
	// means DefaultIsolationGrace.
	IsolationGrace time.Duration
	// now is the clock; nil means time.Now.
	now func() time.Time

	isolatedSince time.Time

	once sync.Once
	mu   sync.Mutex
}

// Handler returns the probe mux.
func (p *Probes) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/startz", p.startz)
	mux.HandleFunc("/readyz", p.readyz)
	mux.HandleFunc("/livez", p.livez)
	mux.HandleFunc("/failsafe", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (p *Probes) startz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	respond(w, p.Health.Started(ctx))
}

func (p *Probes) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	respond(w, p.Ready(ctx))
}

// Ready implements /readyz.
func (p *Probes) Ready(ctx context.Context) error {
	if p.Health.IsPrimary() {
		return p.Health.PrimaryAcceptsWrites(ctx)
	}
	lag, err := p.Health.ReplayLagBytes(ctx)
	if err != nil {
		return err
	}
	if lag > p.MaxLagBytes {
		return fmt.Errorf("replay lag %d bytes exceeds %d", lag, p.MaxLagBytes)
	}
	return nil
}

func (p *Probes) livez(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	err := p.Live(ctx)
	if p.isolatedLongEnough(err != nil) && p.Fenced != nil {
		p.once.Do(p.Fenced)
	}
	respond(w, err)
}

// DefaultIsolationGrace is the isolation window before a primary fences.
const DefaultIsolationGrace = 30 * time.Second

func (p *Probes) isolatedLongEnough(isolated bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !isolated {
		p.isolatedSince = time.Time{}
		return false
	}
	now := time.Now
	if p.now != nil {
		now = p.now
	}
	if p.isolatedSince.IsZero() {
		p.isolatedSince = now()
		return false
	}
	grace := p.IsolationGrace
	if grace == 0 {
		grace = DefaultIsolationGrace
	}
	return now().Sub(p.isolatedSince) >= grace
}

// Live implements /livez. A standby is always live. A primary is live when
// the kube API answers or, without it, when at least one peer's /failsafe
// answers; a primary that reaches nothing is isolated.
func (p *Probes) Live(ctx context.Context) error {
	if !p.Health.IsPrimary() {
		return nil
	}
	if p.KubeReachable != nil {
		if p.KubeReachable(ctx) {
			return nil
		}
		if len(p.Peers) == 0 {
			return errors.New("kube API unreachable and no peers configured")
		}
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	var errs []error
	for _, peer := range p.Peers {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			errs = append(errs, fmt.Errorf("peer %s unreachable: %w", peer, err))
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errs = append(errs, fmt.Errorf("peer %s returned %d", peer, resp.StatusCode))
			continue
		}
		return nil
	}
	if len(errs) == 0 {
		return errors.New("kube API unreachable and no peers configured")
	}
	return fmt.Errorf("kube API unreachable and every peer failed: %w", errors.Join(errs...))
}

func respond(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}
