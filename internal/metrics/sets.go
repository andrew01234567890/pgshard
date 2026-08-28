package metrics

import "github.com/prometheus/client_golang/prometheus"

// Router is the router process metric set.
type Router struct {
	Connections      prometheus.Counter
	Queries          *prometheus.CounterVec
	PlanCacheHits    prometheus.Counter
	PlanCacheMiss    prometheus.Counter
	PlanCacheEvicted prometheus.Gauge
	PlanCacheBytes   prometheus.Gauge
	Refusals         *prometheus.CounterVec
	TwoPCCommits     prometheus.Counter
	TwoPCAborts      prometheus.Counter
	TwoPCInDoubt     prometheus.Counter
	BufferEvents     prometheus.Counter
	BufferSeconds    prometheus.Histogram
	ScatterFanout    prometheus.Histogram
	ShardLatency     *prometheus.HistogramVec
	activeSessions   prometheus.GaugeFunc
}

// NewRouter registers the router metric set on reg. sessions reports the
// live session count at scrape time.
func NewRouter(reg *prometheus.Registry, sessions func() float64) *Router {
	m := &Router{
		Connections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_connections_total", Help: "Client sessions accepted."}),
		Queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_queries_total", Help: "Statements planned, by plan kind and protocol opcode."}, []string{"kind", "opcode"}),
		PlanCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_plan_cache_hits_total", Help: "Parse cache hits."}),
		PlanCacheMiss: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_plan_cache_misses_total", Help: "Parse cache misses."}),
		PlanCacheEvicted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_router_plan_cache_evicted_total", Help: "Parse cache entries evicted."}),
		PlanCacheBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_router_plan_cache_bytes", Help: "Heap the parse cache accounts for."}),
		Refusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_refusals_total", Help: "Statements the router refused, by SQLSTATE."}, []string{"sqlstate"}),
		TwoPCCommits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_twopc_commits_total", Help: "Two-phase commits decided commit."}),
		TwoPCAborts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_twopc_aborts_total", Help: "Two-phase commits decided abort."}),
		TwoPCInDoubt: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_twopc_in_doubt_total", Help: "Two-phase commits left to the resolver."}),
		BufferEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_router_buffering_events_total", Help: "Statements buffered while a shard failed over."}),
		BufferSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgshard_router_buffering_seconds", Help: "Time statements spent buffered during failover.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12)}),
		ScatterFanout: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgshard_router_scatter_fanout", Help: "Shards touched by scatter statements.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128}}),
		ShardLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "pgshard_router_shard_latency_seconds", Help: "Per-shard statement latency.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"shard"}),
	}
	m.activeSessions = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "pgshard_router_active_sessions", Help: "Live client sessions."}, sessions)
	reg.MustRegister(m.Connections, m.Queries, m.PlanCacheHits, m.PlanCacheMiss, m.PlanCacheEvicted, m.PlanCacheBytes, m.Refusals,
		m.TwoPCCommits, m.TwoPCAborts, m.TwoPCInDoubt, m.BufferEvents, m.BufferSeconds,
		m.ScatterFanout, m.ShardLatency, m.activeSessions)
	return m
}

// CacheHit satisfies the parser cache metrics hook.
func (m *Router) CacheHit() { m.PlanCacheHits.Inc() }

// CacheMiss satisfies the parser cache metrics hook.
func (m *Router) CacheMiss() { m.PlanCacheMiss.Inc() }

// CacheEvicted satisfies the parser cache metrics hook.
func (m *Router) CacheEvicted(total int) { m.PlanCacheEvicted.Set(float64(total)) }

// CacheLiveBytes satisfies the parser cache metrics hook.
func (m *Router) CacheLiveBytes(n int) { m.PlanCacheBytes.Set(float64(n)) }

// Pooler is the pooler process metric set.
type Pooler struct {
	BackendDials   *prometheus.CounterVec
	PoolWaits      prometheus.Counter
	PreparedHits   prometheus.Counter
	PreparedMisses prometheus.Counter
	StreamLagBytes prometheus.Gauge
}

// NewPooler registers the pooler metric set on reg. live and idle report
// backend counts at scrape time.
func NewPooler(reg *prometheus.Registry, live, idle func() float64) *Pooler {
	m := &Pooler{
		BackendDials: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_pooler_backend_dials_total", Help: "Backend dial attempts, by result."}, []string{"result"}),
		PoolWaits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_pooler_pool_waits_total", Help: "Acquires that blocked on a full pool."}),
		PreparedHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_pooler_prepared_cache_hits_total", Help: "Prepared statements already on the backend."}),
		PreparedMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_pooler_prepared_cache_misses_total", Help: "Prepared statements re-prepared on the backend."}),
		StreamLagBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_pooler_stream_lag_bytes", Help: "Change-stream replication lag in bytes."}),
	}
	reg.MustRegister(m.BackendDials, m.PoolWaits, m.PreparedHits, m.PreparedMisses, m.StreamLagBytes,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "pgshard_pooler_backends_live", Help: "Open backends."}, live),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "pgshard_pooler_backends_idle", Help: "Idle backends."}, idle))
	return m
}

// Agent is the agent process metric set.
type Agent struct {
	FenceEvents      prometheus.Counter
	BackupLastAge    prometheus.Gauge
	BackupLastSize   prometheus.Gauge
	BackupLastResult *prometheus.GaugeVec
	SlotWALStatus    *prometheus.GaugeVec
}

// NewAgent registers the agent metric set on reg. primary and lagBytes are
// read at scrape time.
func NewAgent(reg *prometheus.Registry, primary, lagBytes func() float64) *Agent {
	m := &Agent{
		FenceEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_agent_isolation_fence_events_total", Help: "Times the primary fenced itself off."}),
		BackupLastAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_agent_backup_last_age_seconds", Help: "Age of the newest completed pgBackRest backup."}),
		BackupLastSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_agent_backup_last_size_bytes", Help: "Size of the newest completed pgBackRest backup."}),
		BackupLastResult: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_agent_backup_last_result", Help: "1 for the result of the last backup attempt."}, []string{"result"}),
		SlotWALStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_agent_slot_wal_status", Help: "1 for each replication slot's wal_status."}, []string{"slot", "wal_status"}),
	}
	reg.MustRegister(m.FenceEvents, m.BackupLastAge, m.BackupLastSize, m.BackupLastResult, m.SlotWALStatus,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "pgshard_agent_primary", Help: "1 when this instance runs as primary."}, primary),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "pgshard_agent_replication_lag_bytes", Help: "Streaming replay lag on a standby; 0 on a primary."}, lagBytes))
	return m
}

// Controller is the controller process metric set; a catalog poller keeps
// the gauges current.
type Controller struct {
	Workflows        *prometheus.GaugeVec
	WorkflowProgress *prometheus.GaugeVec
	CutoverPaused    prometheus.Gauge
	InDoubt          prometheus.Gauge
	InDoubtOldestAge prometheus.Gauge
	Migrations       *prometheus.GaugeVec
	ResolvedTxns     *prometheus.CounterVec
}

// NewController registers the controller metric set on reg.
func NewController(reg *prometheus.Registry) *Controller {
	m := &Controller{
		Workflows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_workflows", Help: "Workflows in the catalog, by kind and state."}, []string{"kind", "state"}),
		WorkflowProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_workflow_progress", Help: "Progress fraction of each running workflow."}, []string{"kind", "id"}),
		CutoverPaused: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_cutover_paused", Help: "Workflows paused at cutover awaiting an operator."}),
		InDoubt: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_in_doubt_transactions", Help: "Undecided rows in pgshard.xact_decisions."}),
		InDoubtOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_in_doubt_oldest_age_seconds", Help: "Age of the oldest undecided two-phase commit."}),
		Migrations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_ddl_migrations", Help: "DDL migrations in the catalog, by state."}, []string{"state"}),
		ResolvedTxns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_controller_resolved_transactions_total", Help: "In-doubt transactions the resolver finished, by outcome."}, []string{"outcome"}),
	}
	reg.MustRegister(m.Workflows, m.WorkflowProgress, m.CutoverPaused, m.InDoubt, m.InDoubtOldestAge, m.Migrations, m.ResolvedTxns)
	return m
}

// Operator is the operator metric set; it registers on the
// controller-runtime registry next to the built-in reconcile metrics.
type Operator struct {
	Failovers      prometheus.Counter
	RollingUpdates *prometheus.GaugeVec
}

// NewOperator registers the operator metric set on reg.
func NewOperator(reg prometheus.Registerer) *Operator {
	m := &Operator{
		Failovers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgshard_operator_failovers_total", Help: "Primary failovers the operator drove."}),
		RollingUpdates: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_operator_rolling_update_pending", Help: "Pods still awaiting the current rolling update, by cluster."}, []string{"cluster"}),
	}
	reg.MustRegister(m.Failovers, m.RollingUpdates)
	return m
}
