package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andrew01234567890/pgshard/internal/metrics"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// Config wires a Router.
type Config struct {
	// Now is the clock the router reads for snapshot staleness; nil means
	// time.Now.
	Now      func() time.Time
	Snapshot SnapshotFunc
	Poolers  *Poolers
	Planner  *Planner
	Logger   *slog.Logger
	// CatalogDatabase is the client-visible name of the catalog database;
	// sessions opening it are routed to the catalog shard set. Default
	// "pgshard".
	CatalogDatabase string
	// CatalogPhysicalDatabase is the database the catalog group actually
	// holds the pgshard schema in. It differs from CatalogDatabase, which
	// is the name clients connect with: the catalog is a SCHEMA inside an
	// ordinary database, so a session opened as `dbname=pgshard` has to
	// reach a backend on this one. Empty means "postgres", which is what
	// the operator's own catalog DSN uses.
	CatalogPhysicalDatabase string
	// Peers receives cancel keys no local session owns; nil drops them.
	Peers CancelForwarder
	// Buffering tunes failover buffering; zero values pick defaults.
	Buffering Buffering
	// Scatter bounds multi-shard reads; zero values pick defaults.
	Scatter ScatterConfig
	// CrossShardLockTimeout bounds a lock wait once a transaction spans
	// shards. Each shard's deadlock detector sees only its own wait
	// edges, so a cycle running across two of them is invisible to both
	// and nothing ends it; this is what does. Zero picks the default,
	// negative disables it and restores an unbounded wait.
	CrossShardLockTimeout time.Duration
	// Decisions is the durable log two-phase commit decides through; nil
	// refuses transactions that write to more than one shard.
	Decisions DecisionLog
	// Sequences serves the global sequences of sharded tables; nil refuses
	// statements that need one.
	Sequences *SequenceAllocator
	// Migrations queues DDL for the controller's applier; nil refuses DDL.
	Migrations MigrationQueue
	// MaxSessions caps the authenticated sessions this router holds at
	// once, whatever role they belong to. The pre-authentication cap
	// releases its slot the moment a session authenticates, and a role
	// carries no connection limit unless one was set on it, so without
	// this one login could hold sessions until the router ran out of
	// memory and took every tenant with it. Zero means DefaultMaxSessions;
	// negative means no cap.
	MaxSessions int
	// MaxSessionsPerRole caps the sessions one role may hold when the role
	// itself carries no connection limit, so a single credential cannot
	// take the whole router and the memory that goes with it: every
	// authenticated session may declare a message body up to
	// pgwire.DefaultMaxMessageBodyLen, and that product is what a router
	// pod has to survive. Zero leaves roles without a limit unlimited,
	// which is what a single-tenant deployment wants.
	MaxSessionsPerRole int
	// RoleLimits reports a role's connection limit; nil leaves limits
	// unenforced.
	RoleLimits RoleLimiter
}

// RoleLimiter reports how many sessions a role may hold open at once. ok is
// false when the role has no limit.
type RoleLimiter interface {
	ConnectionLimit(user string) (int32, bool)
}

// CancelForwarder delivers a cancel key to the router instances it may
// belong to.
type CancelForwarder interface {
	Forward(ctx context.Context, key pgwire.CancelKey)
}

// Router creates executors for authenticated sessions and dispatches cancel
// requests to the pooler serving each session.
type Router struct {
	cfg      Config
	prefix   string
	metrics  *metrics.Router
	mhandler http.Handler

	scatter *scatterSlots

	// inDoubt counts transactions whose commit decision is durable but
	// whose participants the router could not finish itself.
	inDoubt atomic.Int64

	// fenceSeen is when this router last observed the cluster write pause,
	// in unix nanoseconds. A pause is short and a statement can outlive it,
	// so a shard's read-only refusal is attributed to a pause we saw
	// recently as well as to one still up.
	fenceSeen atomic.Int64

	mu       sync.Mutex
	sessions map[uint64]*Executor
	// perUser counts the live sessions of each role, for connection limits.
	perUser  map[string]int32
	buffered map[Shard]int
	// fenceWaiting counts statements held by the cluster write fence.
	fenceWaiting int
	// prepared caches whether a shard's PostgreSQL accepts prepared
	// transactions.
	prepared map[Shard]bool
}

// New validates cfg and returns a Router.
func New(cfg Config) (*Router, error) {
	if cfg.Snapshot == nil || cfg.Poolers == nil {
		return nil, fmt.Errorf("router: Snapshot and Poolers are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.CatalogDatabase == "" {
		cfg.CatalogDatabase = "pgshard"
	}
	if cfg.CatalogPhysicalDatabase == "" {
		cfg.CatalogPhysicalDatabase = "postgres"
	}
	cfg.Buffering = cfg.Buffering.withDefaults()
	cfg.Scatter = cfg.Scatter.withDefaults()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	rt := &Router{cfg: cfg, prefix: hex.EncodeToString(b[:]), scatter: newScatterSlots(cfg.Scatter.MaxStreams, cfg.Scatter.MaxWait),
		sessions: map[uint64]*Executor{}, buffered: map[Shard]int{}, prepared: map[Shard]bool{}}
	reg := metrics.NewRegistry("router")
	rt.metrics = metrics.NewRouter(reg, func() float64 { return float64(rt.Sessions()) },
		func() float64 {
			age, ok := rt.cfg.Snapshot().Age(rt.now())
			if !ok {
				return -1
			}
			return age.Seconds()
		})
	rt.mhandler = metrics.Handler(reg)
	if rt.cfg.Planner == nil {
		rt.cfg.Planner = NewPlannerWithMetrics(rt.metrics)
	}
	return rt, nil
}

func (r *Router) now() time.Time {
	if r.cfg.Now != nil {
		return r.cfg.Now()
	}
	return time.Now()
}

func (r *Router) preparedCapacity(sh Shard) (ok, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ok, known = r.prepared[sh]
	return ok, known
}

func (r *Router) setPreparedCapacity(sh Shard, ok bool) {
	r.mu.Lock()
	r.prepared[sh] = ok
	r.mu.Unlock()
}

// InDoubt reports how many two-phase commits this router left to the
// resolver since it started.
func (r *Router) InDoubt() int64 { return r.inDoubt.Load() }

// MetricsHandler serves the router's registry in the Prometheus text format.
func (r *Router) MetricsHandler() http.Handler { return r.mhandler }

// Metrics is the router's metric set. The change stream server is built
// beside the router rather than inside it -- vstream imports router, so
// router cannot import vstream -- and it cannot report what its buffers
// hold without being handed the collectors.
func (r *Router) Metrics() *metrics.Router { return r.metrics }

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
	// Read the limit before taking the lock: RoleCache holds its own mutex
	// across a catalog reload, and blocking r.mu on that would stall every
	// session close, cancel and buffer reservation behind catalog latency.
	limit, limited := r.limitFor(info.User)
	r.mu.Lock()
	if limit := r.maxSessions(); limit > 0 && len(r.sessions) >= limit {
		r.mu.Unlock()
		return nil, pgwire.Errorf(pgwire.CodeTooManyConnections, "sorry, too many clients already: this router holds %d sessions", limit)
	}
	if limited && r.perUser[info.User] >= limit {
		r.mu.Unlock()
		return nil, pgwire.Errorf(codeBufferFull, "too many connections for role %q", info.User)
	}
	if r.perUser == nil {
		r.perUser = map[string]int32{}
	}
	r.perUser[info.User]++
	r.sessions[info.ID] = e
	r.mu.Unlock()
	r.metrics.Connections.Inc()
	return e, nil
}

// DefaultMaxSessions is the live-session cap a router applies when its
// configuration names none. A session is a goroutine, its buffers and its
// pooler streams, so the number is a memory bound rather than a policy:
// it is high enough that no ordinary workload meets it and low enough
// that meeting it refuses a connection instead of ending the process.
const DefaultMaxSessions = 5000

func (r *Router) maxSessions() int {
	if r.cfg.MaxSessions == 0 {
		return DefaultMaxSessions
	}
	return r.cfg.MaxSessions
}

// limitFor is the role's own connection limit, falling back to
// MaxSessionsPerRole for a role that carries none. nil RoleLimits leaves
// only the fallback.
func (r *Router) limitFor(user string) (int32, bool) {
	if r.cfg.RoleLimits != nil {
		if limit, ok := r.cfg.RoleLimits.ConnectionLimit(user); ok {
			return limit, true
		}
	}
	if r.cfg.MaxSessionsPerRole > 0 {
		return int32(r.cfg.MaxSessionsPerRole), true
	}
	return 0, false
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
	return Shard{Set: snap.ServingShardSet(), ID: d.HomeShard}, nil
}

func (r *Router) forget(e *Executor) {
	r.mu.Lock()
	if r.sessions[e.info.ID] == e {
		delete(r.sessions, e.info.ID)
		if n := r.perUser[e.info.User]; n > 1 {
			r.perUser[e.info.User] = n - 1
		} else {
			delete(r.perUser, e.info.User)
		}
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
