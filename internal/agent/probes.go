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
	// IsPrimary reports the current role, and whether it could be read at
	// all: a probe that cannot tell must not answer as though it could.
	IsPrimary() (bool, error)
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
	// LeaseStale reports whether the primary's lease has gone unrenewed for
	// longer than its duration. Losing a lease is meant to fence the primary,
	// but that runs inside the agent's own renew goroutine, so an agent that
	// is frozen or wedged never gets there and keeps PostgreSQL writable while
	// the operator promotes someone else. Failing liveness instead hands the
	// deadline to the kubelet, which is outside anything the agent controls.
	// Nil when the agent holds no lease.
	LeaseStale func() bool
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
	primary, err := p.Health.IsPrimary()
	if err != nil {
		return fmt.Errorf("reading the instance role: %w", err)
	}
	if primary {
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
	// A member whose role cannot be read is treated as a primary here: the
	// checks below are what stop an isolated primary from staying alive,
	// and skipping them on an unreadable role would skip exactly the case
	// where something is already wrong with the data directory.
	primary, err := p.Health.IsPrimary()
	if err == nil && !primary {
		return nil
	}
	if p.LeaseStale != nil && p.LeaseStale() {
		return errors.New("primary lease not renewed within its duration")
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
	// Concurrently, and this is not an optimisation. The kubelet gives the
	// whole handler ONE timeout, and asking peers one after another makes
	// the worst case grow with the member count -- three replicas cost the
	// kube probe plus two peer timeouts, five cost four. A probe that
	// outruns the kubelet's patience is failed regardless of what the peers
	// would have said, which is precisely the vote this failsafe exists to
	// take. One round trip now, whatever the group size.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		ok  bool
		err error
	}
	results := make(chan result, len(p.Peers))
	for _, peer := range p.Peers {
		go func(peer string) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: fmt.Errorf("peer %s unreachable: %w", peer, err)}
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				results <- result{err: fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)}
				return
			}
			results <- result{ok: true}
		}(peer)
	}
	var errs []error
	for range p.Peers {
		r := <-results
		if r.ok {
			// One peer that can see us is the whole answer; the cancel
			// above ends the rest.
			return nil
		}
		errs = append(errs, r.err)
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
