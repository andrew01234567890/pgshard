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

	once sync.Once
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
	if err != nil && p.Fenced != nil {
		p.once.Do(p.Fenced)
	}
	respond(w, err)
}

// Live implements /livez. A standby is always live. A primary is live when
// the kube API answers; without it, only when every peer's /failsafe
// answers, so an isolated primary fences itself.
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
	for _, peer := range p.Peers {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, peer, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("peer %s unreachable: %w", peer, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)
		}
	}
	return nil
}

func respond(w http.ResponseWriter, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}
