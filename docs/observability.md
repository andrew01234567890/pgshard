# Observability

Every pgshard process serves Prometheus metrics over HTTP at `/metrics`,
built on one shared package (`internal/metrics`): a per-process registry
carrying the Go runtime and process collectors plus a
`pgshard_build_info{process,version}` gauge.

The `/metrics` listeners are unauthenticated; keep them off untrusted
networks (an authenticating admin proxy is planned).

## Endpoints

| Process    | Where `/metrics` lives                                                        |
|------------|-------------------------------------------------------------------------------|
| router     | the `--health-listen` HTTP address (next to `/readyz` and `/healthz`)          |
| pooler     | `--metrics-listen` (a small dedicated HTTP listener; empty disables)           |
| agent      | the existing HTTP port `8080`, next to the kubelet probes                      |
| controller | `--metrics-listen` (empty disables)                                            |
| operator   | controller-runtime's `--metrics-bind-address` (default `:8080`)                |
| admin      | the admin UI listener itself                                                   |

Operator-rendered pods carry `prometheus.io/scrape: "true"`,
`prometheus.io/port` and `prometheus.io/path: /metrics` annotations: member
pods (agent on 8080; the pooler sidecar also listens on its own metrics port
9127), router pods (8080) and admin pods (8081). Any annotation-driven
Prometheus scrape config picks them up; with the Prometheus Operator, use a
PodMonitor selecting the `pgshard.io/cluster` label instead.

## Metrics catalogue

### Router (`pgshard_router_*`)

| Metric | Type | Meaning |
|---|---|---|
| `connections_total` | counter | client sessions accepted |
| `active_sessions` | gauge | live sessions |
| `queries_total{kind,opcode}` | counter | statements planned, by plan kind and protocol opcode (`simple`/`parse`) |
| `plan_cache_hits_total` / `plan_cache_misses_total` | counter | parse cache behaviour |
| `refusals_total{sqlstate}` | counter | statements the router refused |
| `twopc_commits_total` / `twopc_aborts_total` / `twopc_in_doubt_total` | counter | two-phase commit outcomes |
| `buffering_events_total` / `buffering_seconds` | counter / histogram | failover buffering |
| `scatter_fanout` | histogram | shards touched by scatter statements |
| `shard_latency_seconds{shard}` | histogram | per-shard statement latency |

### Pooler (`pgshard_pooler_*`)

| Metric | Type | Meaning |
|---|---|---|
| `backends_live` / `backends_idle` | gauge | pool size and idle backends |
| `pool_waits_total` | counter | acquires that blocked on a full pool |
| `backend_dials_total{result}` | counter | backend dials, `ok`/`error` |
| `prepared_cache_hits_total` / `prepared_cache_misses_total` | counter | prepared-statement reuse on backends |
| `stream_lag_bytes` | gauge | change-stream reader lag behind the server WAL end |

### Agent (`pgshard_agent_*`)

| Metric | Type | Meaning |
|---|---|---|
| `primary` | gauge | 1 on the primary |
| `replication_lag_bytes` | gauge | replay lag on a standby (−1 when unknown) |
| `slot_wal_status{slot,wal_status}` | gauge | 1 per slot per current `wal_status` |
| `backup_last_age_seconds` / `backup_last_size_bytes` / `backup_last_result{result}` | gauge | newest pgBackRest backup |
| `isolation_fence_events_total` | counter | primary self-fencing events |

The slot and backup gauges refresh once a minute on the primary.

### Controller (`pgshard_controller_*`)

| Metric | Type | Meaning |
|---|---|---|
| `workflows{kind,state}` | gauge | workflows in the catalog |
| `workflow_progress{kind,id}` | gauge | mean per-table progress of running/paused workflows |
| `cutover_paused` | gauge | workflows paused (awaiting an operator at cutover) |
| `in_doubt_transactions` / `in_doubt_oldest_age_seconds` | gauge | undecided `pgshard.xact_decisions` rows |
| `ddl_migrations{state}` | gauge | DDL migrations by state |
| `resolved_transactions_total{outcome}` | counter | what the resolver finished |

These gauges are polled from the catalog on the reconcile interval.

### Operator

controller-runtime's built-in `controller_runtime_reconcile_total` and
`controller_runtime_reconcile_errors_total` cover reconcile counts/errors.
pgshard adds `pgshard_operator_failovers_total` and
`pgshard_operator_rolling_update_pending{cluster}` on the same registry.

## Alerts

`config/monitoring/alerts.yaml` is a PrometheusRule manifest with the
documented rules: aged in-doubt 2PC, oversized decision log, replication
lag, slot WAL retention pressure (`wal_status` leaving reserved/extended
under `max_slot_wal_keep_size`), change-stream lag, backup staleness and
failures (including ArchiveDuplicateError surfaced as a failed backup),
operator reconcile errors and exceeded cutover pauses. Apply it directly
where the Prometheus Operator CRDs exist, or copy `spec.groups` into a
plain `rule_files` entry.

## Dashboard

`config/monitoring/dashboards/pgshard.json` is a Grafana dashboard covering
topology/HA, router traffic and transactions, pooler health, reshard/DDL
progress, and backups/slots. Import it as-is; every panel references only
the metric names above.

## Admin panels

The admin UI derives live views from the catalog (see `docs/admin.md`):
`/twopc` lists the decision log (gid, state, participants, age, decision)
and `/alerts` renders the currently firing conditions without needing a
Prometheus deployment.
