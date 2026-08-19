# pgshard-admin

`pgshard-admin serve` is a read-only web UI over the Kubernetes API. It shows
every `PgShardCluster` the service account can see and, per cluster, the
replication groups, their members, the cluster conditions and the shard map,
plus a backups panel: the bound backup policy, recent `PgShardBackup` runs,
`PgShardRestore` runs and the certified restore points of the catalog.

## Running

```
pgshard-admin serve [--listen :8081] [--kubeconfig PATH] [--namespace NS] [--catalog-dsn DSN]
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--listen` | `:8081` | HTTP listen address. |
| `--kubeconfig` | in-cluster | Kubeconfig to use outside a pod. |
| `--namespace` | all namespaces | Restrict the watch and the clusters list to one namespace. |
| `--catalog-dsn` | none | PostgreSQL DSN of a catalog database; adds the catalog's `pgshard.shard_status` snapshot to the topology page and the certified restore points (`pgshard.restore_points`) to the backups page. |

`--help` and `--version` behave as in every pgshard command.

## Pages and endpoints

| Path | Content |
|------|---------|
| `/` | Clusters list with backup overview cards: age of the last successful backup per cluster, failed backups count, restores in progress. |
| `/clusters/{ns}/{name}` | Topology: conditions, groups (members: name, role, ready, epoch, replay lag, pod phase, node), shard map, catalog snapshot. The header shows `status.shardMapGeneration`. |
| `/clusters/{ns}/{name}/topology` | The topology fragment htmx swaps in on refresh. |
| `/backups` | Backups panel. Per cluster: the policy summary (store kind, bucket/container, prefix, endpoint, the *names* of the credential and encryption Secrets, schedules, retention, barrier schedule, health), recent `PgShardBackup` objects (kind, phase, started/finished, duration, per-group stanza, backup label, size, error) and `PgShardRestore` objects (target kind: time, lsn, name, xid, immediate, barrier or latest; phase incl. `Reconciling`; per-group progress; reconciliation summary: fence state, decisions, contradictions). A failed restore shows a `role="alert"` banner with its reason. Below the clusters, the certified restore points (name, created, shard map generation, per-group LSN and timeline; newest 50). |
| `/backups/panel` | The backups fragment htmx swaps in on refresh. |
| `/backups/{ns}/{name}` | One `PgShardBackup`: phase, label, times, per-group results. |
| `/restores/{ns}/{name}` | One `PgShardRestore`: phase, target, per-group progress, reconciliation (decisions, committed, rolled back, contradictions list), failure banner. |
| `/api/v1/backups` | JSON list of backups, newest first. |
| `/api/v1/restores` | JSON list of restores, newest first. |
| `/api/v1/restore-points` | JSON list of certified restore points (empty without `--catalog-dsn`). |
| `/api/v1/clusters` | JSON clusters list. |
| `/api/v1/clusters/{ns}/{name}` | JSON topology document the page is rendered from. |
| `/events` | Server-sent events. A `topology` event is sent on every PgShardCluster, PgShardGroup, PgShardBackupPolicy, PgShardBackup, PgShardRestore or member Pod change seen by the informers, and at least every 2 seconds. |
| `/healthz` | Liveness and readiness. |

The topology and backups pages open `/events` and reload their fragment on
each event, so the view follows the cluster without a full page reload.

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
   namespaces: `make deploy-admin` (override `ADMIN_IMG`).

Build the image with `make admin-image` or
`docker build -f Dockerfile.control --build-arg CMD=pgshard-admin .`.

## Security

* The UI is **read-only**: its RBAC grants only `get`, `list` and `watch` on
  the pgshard CRDs (clusters, groups, backup policies, backups, restores),
  Pods, PVCs, Services and Leases (`config/admin/rbac.yaml`; a unit test
  enforces this). Secrets are not readable.
* There is **no authentication**. Do not expose the Service outside the
  cluster directly; put it behind an ingress or proxy that authenticates
  (OIDC, mTLS, VPN). The catalog DSN, if given, is a plain database credential
  and should be a read-only role.
* Every response carries a strict `Content-Security-Policy`
  (`default-src 'none'; script-src 'self'; style-src 'self'; ...`) plus
  `X-Content-Type-Options`, `X-Frame-Options: DENY` and a no-referrer policy.
  No asset is loaded from outside the binary: htmx (2.0.8, Zero-Clause BSD,
  `internal/admin/static/LICENSE.htmx`), the stylesheet and the SSE client
  script are embedded.
* Requests are logged as JSON lines to stderr; shutdown drains in-flight
  requests for up to 10 seconds.
