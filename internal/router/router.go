package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Config wires a Router.
type Config struct {
	Snapshot SnapshotFunc
	Poolers  *Poolers
	Planner  *Planner
	Logger   *slog.Logger
	// CatalogDatabase is the client-visible name of the catalog database;
	// sessions opening it are routed to the catalog shard set. Default
	// "pgshard".
	CatalogDatabase string
	// Peers receives cancel keys no local session owns; nil drops them.
	Peers CancelForwarder
	// Buffering tunes failover buffering; zero values pick defaults.
	Buffering Buffering
	// Scatter bounds multi-shard reads; zero values pick defaults.
	Scatter ScatterConfig
}

// CancelForwarder delivers a cancel key to the router instances it may
// belong to.
type CancelForwarder interface {
	Forward(ctx context.Context, key pgwire.CancelKey)
}

// Router creates executors for authenticated sessions and dispatches cancel
// requests to the pooler serving each session.
type Router struct {
	cfg    Config
	prefix string

	scatter *scatterSlots

	mu       sync.Mutex
	sessions map[uint64]*Executor
	buffered map[Shard]int
}

// New validates cfg and returns a Router.
func New(cfg Config) (*Router, error) {
	if cfg.Snapshot == nil || cfg.Poolers == nil {
		return nil, fmt.Errorf("router: Snapshot and Poolers are required")
	}
	if cfg.Planner == nil {
		cfg.Planner = NewPlanner()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.CatalogDatabase == "" {
		cfg.CatalogDatabase = "pgshard"
	}
	cfg.Buffering = cfg.Buffering.withDefaults()
	cfg.Scatter = cfg.Scatter.withDefaults()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	return &Router{cfg: cfg, prefix: hex.EncodeToString(b[:]), scatter: newScatterSlots(cfg.Scatter.MaxStreams),
		sessions: map[uint64]*Executor{}, buffered: map[Shard]int{}}, nil
}

// NewExecutor implements pgwire.Config.NewExecutor: it resolves the session's
// database to its home shard and refuses databases the catalog does not know
// with 3D000.
func (r *Router) NewExecutor(info pgwire.SessionInfo) (pgwire.Executor, error) {
	if info.Auth == nil || info.Auth.SCRAM == nil {
		return nil, pgwire.Errorf(pgwire.CodeInvalidAuthorization, "session was not authenticated with SCRAM")
	}
	home, err := r.homeShard(info.Database)
	if err != nil {
		return nil, err
	}
	e := newExecutor(r, info, home)
	r.mu.Lock()
	r.sessions[info.ID] = e
	r.mu.Unlock()
	return e, nil
}

func (r *Router) homeShard(database string) (Shard, error) {
	if database == r.cfg.CatalogDatabase {
		return Shard{Set: CatalogShardSet, ID: 0}, nil
	}
	snap := r.cfg.Snapshot()
	if snap == nil {
		return Shard{}, pgwire.Errorf("57P03", "the catalog snapshot is not loaded yet")
	}
	d, ok := snap.Databases[database]
	if !ok {
		return Shard{}, pgwire.Errorf("3D000", "database %q does not exist", database)
	}
	return Shard{Set: DefaultShardSet, ID: d.HomeShard}, nil
}

func (r *Router) forget(e *Executor) {
	r.mu.Lock()
	if r.sessions[e.info.ID] == e {
		delete(r.sessions, e.info.ID)
	}
	r.mu.Unlock()
}

// CancelHandler returns a pgwire.CancelHandler that verifies the key with
// srv and then interrupts the session's backend through its pooler. Keys no
// local session owns go to Config.Peers, when set.
func (r *Router) CancelHandler(srv *pgwire.Server) pgwire.CancelHandler {
	return func(ctx context.Context, key pgwire.CancelKey) {
		if r.CancelLocal(ctx, srv, key) || r.cfg.Peers == nil {
			return
		}
		r.cfg.Peers.Forward(ctx, key)
	}
}

// CancelLocal cancels the local session key names, if any, and reports
// whether one matched. Peer routers call this for forwarded keys.
func (r *Router) CancelLocal(ctx context.Context, srv *pgwire.Server, key pgwire.CancelKey) bool {
	if !srv.CancelLocal(key) {
		return false
	}
	r.mu.Lock()
	e := r.sessions[uint64(key.PID)]
	r.mu.Unlock()
	if e != nil {
		e.cancelBackend(ctx)
	}
	return true
}

// Sessions reports the number of live executors.
func (r *Router) Sessions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}
