# pgshard-admin

`pgshard-admin serve` is a read-only web UI over the Kubernetes API. It shows
every `PgShardCluster` the service account can see and, per cluster, the
replication groups, their members, the cluster conditions and the shard map,
plus a backups panel: the bound backup policy, recent `PgShardBackup` runs,
`PgShardRestore` runs and the certified restore points of the catalog, and
a reshards panel: `PgShardReshard` records with their workflow progress,
table placement workflows and major upgrade workflows.

## Running

```
pgshard-admin serve --token-file PATH [--listen :8081] [--kubeconfig PATH] [--namespace NS] [--catalog-dsn DSN]
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--token-file` | required | File holding the credential every route but `/healthz` requires. |
| `--insecure-no-auth` | off | Serve to anyone who can reach the listener. Development only, and mutually exclusive with `--token-file`. |
| `--listen` | `:8081` | HTTP listen address. |
| `--kubeconfig` | in-cluster | Kubeconfig to use outside a pod. |
| `--namespace` | all namespaces | Restrict the watch and the clusters list to one namespace. |
| `--catalog-dsn` | none | PostgreSQL DSN of a catalog database; adds the catalog's `pgshard.shard_status` snapshot and a DDL card to the topology page, the certified restore points (`pgshard.restore_points`) to the backups page, the `pgshard.workflows` rows (newest 200) to the reshards page, and enables the migrations and streams panels. |

`--help` and `--version` behave as in every pgshard command.

## Pages and endpoints

| Path | Content |
|------|---------|
| `/` | Clusters list with backup overview cards (age of the last successful backup per cluster, failed backups count, restores in progress) and resharding cards (reshards in progress, the last recorded cutover pause with a link to its record). |
| `/clusters/{ns}/{name}` | Topology: conditions, groups (members: name, role, ready, epoch, replay lag, pod phase, node), shard map, catalog snapshot. The header shows `status.shardMapGeneration`. |
| `/clusters/{ns}/{name}/topology` | The topology fragment htmx swaps in on refresh. |
| `/backups` | Backups panel. Per cluster: the policy summary (store kind, bucket/container, prefix, endpoint, the *names* of the credential and encryption Secrets, schedules, retention, barrier schedule, health), recent `PgShardBackup` objects (kind, phase, started/finished, duration, per-group stanza, backup label, size, error) and `PgShardRestore` objects (target kind: time, lsn, name, xid, immediate, barrier or latest; phase incl. `Reconciling`; per-group progress; reconciliation summary: fence state, decisions, contradictions). A failed restore shows a `role="alert"` banner with its reason. Below the clusters, the certified restore points (name, created, shard map generation, per-group LSN and timeline; newest 50). |
| `/backups/panel` | The backups fragment htmx swaps in on refresh. |
| `/backups/{ns}/{name}` | One `PgShardBackup`: phase, label, times, per-group results. |
| `/restores/{ns}/{name}` | One `PgShardRestore`: phase, target, per-group progress, reconciliation (decisions, committed, rolled back, contradictions list), failure banner. |
| `/reshards` | Reshards panel. Per cluster: the active record from `status.reshard`, then every `PgShardReshard` (newest first) with generations and target shard count, phase, workflow stage, start time, targets ready, copy progress bars (tables ready over total, apply lag, paused subscriptions; per target when the workflow records `status.targets`), cutover pause, journal ids and message. A failed reshard shows a `role="alert"` banner. Below the clusters: placement workflows (`kind=table_placement`: database, table, from and to placement with shard key, state, stage, progress, message) and major upgrade workflows (`kind=upgrade`: versions, state, stage, message). Workflow data needs `--catalog-dsn`; without it the records are shown alone. |
| `/reshards/panel` | The reshards fragment htmx swaps in on refresh. |
| `/reshards/{ns}/{name}` | One `PgShardReshard` joined with its workflow: phase, stage and workflow state, workflow id, times, cutover pause, journal ids, message, error, cancel time and reason; aggregate copy progress; per-target table (range, group, readiness, primary, subscriptions, progress); stage timeline (record creation, `True` conditions, `status.history` entries when present, fence/flip/release/switch times, the current stage); cutover (source set, step and attempt, gate, pause point and proceed list, fence and switch times, write pause and fence hold, retirement time of the old groups, undone attempts) and the verify report (tables, rows, mismatches). |
| `/api/v1/reshards` | JSON reshards document the page is rendered from. |
| `/api/v1/reshards/{ns}/{name}` | JSON detail of one reshard. |
| `/upgrades` | Major-upgrade progress per cluster: the spec major against the serving and catalog majors, the shard-set replacement run (from → to major, stage/phase, rollback annotation state, retired set kept for reverse replication), precondition blockers when the operator reports `UpgradeBlocked`, and the catalog group upgrade (`status.catalogUpgrade`: stage, catch-up lag message, blockers, rollback request). Refreshes every 5 s. |
| `/upgrades/panel` | The upgrades fragment htmx swaps in on refresh. |
| `/api/v1/upgrades` | JSON upgrades document the page is rendered from. |
| `/api/v1/backups` | JSON list of backups, newest first. |
| `/api/v1/restores` | JSON list of restores, newest first. |
| `/api/v1/restore-points` | JSON list of certified restore points (empty without `--catalog-dsn`). |
| `/migrations` | DDL migrations panel (needs `--catalog-dsn`): newest first, `?database=` and `?state=` filters, 25 per page (`?page=`). Columns: id, database, kind, strategy, state, created, finished, duration, statement (truncated, expand for the full text), per-shard progress bar (applied/retrying/failed/pending) and the current step of a multistep migration. |
| `/migrations/{id}` | One migration: statement, steps with status (`pending`, `partial`, `running`, `failed`, `done`), per-shard table (state, step, attempts, last error). A DEGRADED banner appears when the statement applied on some shards and failed on another. |
| `/migrations/table`, `/migrations/{id}/detail` | The fragments htmx swaps in on refresh. |
| `/api/v1/migrations`, `/api/v1/migrations/{id}` | JSON documents the pages are rendered from. |
| `/streams` | Change streams (needs `--catalog-dsn`, otherwise 503): name, database, two-phase, state, created, active/inactive slot counts, lost slot count, largest WAL retained behind a slot, whether every slot is synced on the standbys. A `role="alert"` banner names the streams with a lost slot. |
| `/streams/{name}` | One stream: per-shard slot (shard, slot name, active, restart and confirmed LSNs, retained bytes, `wal_status`, invalidation reason, synced, failover, last update) and a LOST banner when any slot was invalidated. 404 for an unknown stream. |
| `/api/v1/streams`, `/api/v1/streams/{name}` | JSON documents the streams pages are rendered from. |
| `/api/v1/clusters` | JSON clusters list. |
| `/api/v1/clusters/{ns}/{name}` | JSON topology document the page is rendered from. |
| `/twopc` | Two-phase commit decision log (needs `--catalog-dsn`, otherwise 503): every `pgshard.xact_decisions` row with gid, state, participants, age and decision (`undecided` while preparing). The header counts logged and in-doubt rows and shows the oldest in-doubt age. Read-only. |
| `/alerts` | Currently firing conditions derived from the catalog and the Kubernetes API, without needing Prometheus: aged in-doubt 2PC (over 5 minutes), an oversized decision log, lost stream slots, workflows paused at cutover for over 30 minutes, and backup staleness (no completed backup, or the newest older than 26 hours). Read-only. |
| `/api/v1/twopc`, `/api/v1/alerts` | JSON documents the two pages are rendered from. |
| `/metrics` | Prometheus metrics of the admin process itself. |
| `/events` | Server-sent events. A `topology` event is sent on every PgShardCluster, PgShardGroup, PgShardBackupPolicy, PgShardBackup, PgShardRestore, PgShardReshard or member Pod change seen by the informers, and at least every 2 seconds. |
| `/healthz` | Liveness and readiness. |

The topology, backups, migrations, streams and reshards pages open `/events` and reload
their fragment on each event (streams requests carrying `HX-Request` get the
fragment only), so the view follows the cluster without a full page reload.
The catalog has no notify channel for migrations; the 2 second tick is what
keeps the migrations panel current. The clusters page shows a streams card
(count and lost count) when a catalog DSN is configured. Stream rows come from
`pgshard.streams` and `pgshard.stream_status`, which the controller's stream
monitor refreshes; `retained_bytes` is `pg_current_wal_lsn() - restart_lsn` on
the primary.

The backups panel never reads Secrets: policies are shown with the Secret
*names* their `credentials` and `encryption` references point at, and the
RBAC grants no access to Secrets at all.

## Deployment

Two ways to run it in a cluster:

1. **Per cluster, by the operator.** When `spec.admin.enabled` is true (the
   default) the operator creates `<cluster>-admin`: a ServiceAccount, a
   namespaced read-only Role and RoleBinding, a Deployment running
   `serve --namespace <ns>` and a ClusterIP Service on port 8081. The image
   comes from the operator's `--admin-image` flag (default
   `ghcr.io/andrew01234567890/pgshard-admin:latest`). Setting
   `spec.admin.enabled: false` removes these objects.
2. **Standalone.** `config/admin/` holds a Deployment, Service, ServiceAccount
   and a cluster-wide read-only ClusterRole for one instance that watches all
   namespaces: `make deploy-admin` (override `ADMIN_IMG`). That target
   generates the Secret `pgshard-admin` if it does not exist and prints how
   to read the token; the Deployment mounts it, so applying
   `config/admin/deployment.yaml` on its own needs the Secret created first.

Build the image with `make admin-image` or
`docker build -f Dockerfile.control --build-arg CMD=pgshard-admin .`.

## Security

* The UI is **read-only**: its RBAC grants only `get`, `list` and `watch` on
  the pgshard CRDs (clusters, groups, backup policies, backups, restores, reshards),
  Pods, PVCs, Services and Leases (`config/admin/rbac.yaml`; a unit test
  enforces this). Secrets are not readable.
* Every route but `/healthz` **requires a credential**: the token in
  `--token-file`, sent either as `Authorization: Bearer <token>` or as the
  password of HTTP basic auth (any user name, so a browser can be pointed at
  the UI and log in). Without it the answer is `401` with a `Basic`
  challenge, whatever the route. `/healthz` stays open because requiring the
  token for it would make the credential a dependency of the pod staying up.

  The operator generates the token per cluster into the Secret
  `<cluster>-admin` (key `token`) and mounts it; read it with
  `kubectl get secret <cluster>-admin -o jsonpath='{.data.token}' | base64 -d`.
  It is not the superuser password: reading the admin is not being able to
  write to PostgreSQL. `spec.admin.insecureNoAuth: true` serves the UI to
  anything that can reach its Service, which is a development setting.

  A metrics scraper needs the credential too — Prometheus takes it as
  `bearer_token_file` or `basic_auth`.

* The transport is plain HTTP inside the cluster, so the credential is only
  as private as the pod network. Put the Service behind an ingress or proxy
  that terminates TLS, and prefer one that authenticates in its own right
  (OIDC, mTLS, VPN) before exposing it outside the cluster. The catalog DSN,
  if given, is a plain database credential and should be a read-only role.
* Every response carries a strict `Content-Security-Policy`
  (`default-src 'none'; script-src 'self'; style-src 'self'; ...`) plus
  `X-Content-Type-Options`, `X-Frame-Options: DENY` and a no-referrer policy.
  No asset is loaded from outside the binary: htmx (2.0.8, Zero-Clause BSD,
  `internal/admin/static/LICENSE.htmx`), the stylesheet and the SSE client
  script are embedded.
* Requests are logged as JSON lines to stderr; shutdown drains in-flight
  requests for up to 10 seconds.
