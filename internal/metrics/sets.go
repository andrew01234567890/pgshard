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
	ShardStatements  *prometheus.CounterVec
	ShardRows        *prometheus.CounterVec
	ShardErrors      *prometheus.CounterVec
	// The change stream's buffers. A stream ends with
	// TRANSACTION_TOO_LARGE when one of the bounds trips, and by then the
	// consumer has to resume; watching the gauges is how anybody sees it
	// coming rather than reading about it afterwards.
	VStreamBufferedBytes prometheus.Gauge
	VStreamOpenTxns      prometheus.Gauge
	VStreamTooLarge      *prometheus.CounterVec
	activeSessions       prometheus.GaugeFunc
	snapshotAge          prometheus.GaugeFunc
}

// NewRouter registers the router metric set on reg. sessions reports the
// live session count and snapshotAge the age of the catalog view, both at
// scrape time; snapshotAge may be nil where there is no watcher.
func NewRouter(reg *prometheus.Registry, sessions, snapshotAge func() float64) *Router {
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
		// Latency alone cannot tell a shard that is busy from one that is
		// slow: both show as time. Counting the work as well as timing it
		// is what separates a hot shard from slow storage, and the label
		// is the shard, whose cardinality is the shard count.
		ShardStatements: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_shard_statements_total", Help: "Statements sent to each shard."}, []string{"shard"}),
		ShardRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_shard_rows_total", Help: "Data rows returned by each shard."}, []string{"shard"}),
		ShardErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_shard_errors_total", Help: "Statements each shard answered with an error."}, []string{"shard"}),
		VStreamBufferedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_router_vstream_buffered_bytes", Help: "Encoded events held for change-stream transactions that have not committed."}),
		VStreamOpenTxns: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_router_vstream_open_transactions", Help: "Interleaved in-progress change-stream transactions being assembled."}),
		VStreamTooLarge: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_router_vstream_too_large_total", Help: "Change streams ended because a buffer bound was exceeded, by which bound."}, []string{"bound"}),
	}
	m.activeSessions = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "pgshard_router_active_sessions", Help: "Live client sessions."}, sessions)
	if snapshotAge == nil {
		snapshotAge = func() float64 { return -1 }
	}
	m.snapshotAge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "pgshard_router_snapshot_age_seconds",
		Help: "Age of the catalog snapshot the router is serving; -1 before the first load."}, snapshotAge)
	reg.MustRegister(m.Connections, m.Queries, m.PlanCacheHits, m.PlanCacheMiss, m.PlanCacheEvicted, m.PlanCacheBytes, m.Refusals,
		m.TwoPCCommits, m.TwoPCAborts, m.TwoPCInDoubt, m.BufferEvents, m.BufferSeconds,
		m.ScatterFanout, m.ShardLatency, m.ShardStatements, m.ShardRows, m.ShardErrors,
		m.VStreamBufferedBytes, m.VStreamOpenTxns, m.VStreamTooLarge,
		m.activeSessions, m.snapshotAge)
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
// backend counts and snapshotAge the age of the catalog view, all at scrape
// time; snapshotAge may be nil where there is no watcher.
func NewPooler(reg *prometheus.Registry, live, idle, snapshotAge func() float64) *Pooler {
	if snapshotAge == nil {
		snapshotAge = func() float64 { return -1 }
	}
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
			Name: "pgshard_pooler_backends_idle", Help: "Idle backends."}, idle),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "pgshard_pooler_snapshot_age_seconds",
			Help: "Age of the catalog snapshot the pooler fences from; -1 when it has none."}, snapshotAge))
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
	CutoverPaused    *prometheus.GaugeVec
	InDoubt          prometheus.Gauge
	InDoubtOldestAge prometheus.Gauge
	// Decided counts rows the resolver has decided but could not finish,
	// and DecidedOldestAge their age since the decision. A decided
	// transaction still holds its locks, WAL and vacuum horizon on every
	// participant, so one the resolver keeps failing to finish costs the
	// same as an undecided one -- and the in-doubt gauges, which count
	// only undecided rows, say nothing about it.
	Decided          *prometheus.GaugeVec
	DecidedOldestAge *prometheus.GaugeVec
	// WorkflowStepAge is how long each running workflow has been on its
	// current cutover step.
	WorkflowStepAge *prometheus.GaugeVec
	// CutoverPauseSeconds is the write pause each completed cutover took.
	//
	// The guide promises a sub-second pause, which is the one moment a
	// reshard is visible to a workload, and nothing measured it outside the
	// test suite: CutoverPaused counts workflows held at a CONFIGURED pause
	// point, which is a different thing entirely. An operator whose pause
	// was seconds rather than sub-second had no way to know.
	CutoverPauseSeconds *prometheus.GaugeVec
	// ResolverUnresolved is what the last resolver pass could not settle:
	// decisions it could not finish, shards it could not search, and a
	// failed orphan sweep.
	ResolverUnresolved prometheus.Gauge
	Migrations         *prometheus.GaugeVec
	ResolvedTxns       *prometheus.CounterVec
}

// NewController registers the controller metric set on reg.
func NewController(reg *prometheus.Registry) *Controller {
	m := &Controller{
		Workflows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_workflows", Help: "Workflows in the catalog, by kind and state."}, []string{"kind", "state"}),
		WorkflowProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_workflow_progress", Help: "Progress fraction of each running workflow."}, []string{"kind", "id"}),
		CutoverPaused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_cutover_paused",
			Help: "Workflows held at a configured cutover pause, waiting for an operator to let them proceed."},
			[]string{"kind", "shard_set", "id", "pause"}),
		InDoubt: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_in_doubt_transactions", Help: "Undecided rows in pgshard.xact_decisions."}),
		InDoubtOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_in_doubt_oldest_age_seconds", Help: "Age of the oldest undecided two-phase commit."}),
		Decided: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_decided_transactions",
			Help: "Decided rows in pgshard.xact_decisions the resolver has not finished, by decision."}, []string{"decision"}),
		DecidedOldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_decided_oldest_age_seconds",
			Help: "Age since the decision of the oldest unfinished decided transaction, by decision."}, []string{"decision"}),
		WorkflowStepAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_workflow_step_age_seconds",
			Help: "How long a running workflow has been on its current cutover step."}, []string{"kind", "id", "stage", "step"}),
		CutoverPauseSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_cutover_pause_seconds",
			Help: "Write pause of each completed cutover, from raising the fence to serving the new map."}, []string{"kind", "id"}),
		ResolverUnresolved: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgshard_controller_resolver_unresolved",
			Help: "Transactions, shard scans and sweeps the last resolver pass could not settle."}),
		Migrations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgshard_controller_ddl_migrations", Help: "DDL migrations in the catalog, by state."}, []string{"state"}),
		ResolvedTxns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgshard_controller_resolved_transactions_total", Help: "In-doubt transactions the resolver finished, by outcome."}, []string{"outcome"}),
	}
	reg.MustRegister(m.Workflows, m.WorkflowProgress, m.CutoverPaused, m.InDoubt, m.InDoubtOldestAge,
		m.Decided, m.DecidedOldestAge, m.WorkflowStepAge, m.CutoverPauseSeconds, m.ResolverUnresolved, m.Migrations, m.ResolvedTxns)
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
