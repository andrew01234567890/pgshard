# Admin UI tour

`pgshard-admin` is a read-only web UI. The operator deploys one per cluster
(`<cluster>-admin:8081`) unless `spec.admin.enabled: false`; a standalone
all-namespaces instance is `make deploy-admin`. Reference:
[admin.md](../admin.md).

```sh
kubectl port-forward svc/demo-admin 8081:8081
```

## Pages

- **`/` Clusters** — every visible `PgShardCluster` with backup overview
  cards: age of the last successful backup, failed backups, restores in
  progress, and a streams card when a catalog DSN is configured.
- **`/clusters/{ns}/{name}` Topology** — conditions, every replication
  group with its members (role, ready, epoch, replay lag, pod phase,
  node), the shard map and the catalog's `shard_status` snapshot. The
  header shows the shard map generation. Live: the page follows `/events`
  (server-sent events) and refreshes itself.
- **`/backups`** — per cluster: the bound policy (store, schedules,
  retention, barrier schedule, health; Secret *names* only, never
  contents), recent `PgShardBackup` runs with per-group labels and sizes,
  `PgShardRestore` runs with reconciliation summaries, and the newest
  certified restore points.
- **`/migrations`** — the DDL/DCL queue, newest first, filterable by
  database and state; per row a per-shard progress bar and the current
  step of a multistep migration. `/migrations/{id}` shows the statement,
  its steps and the per-shard detail; a DEGRADED banner flags a statement
  applied on some shards and failed on another.
- **`/streams`** — change streams with per-shard slot health: active,
  restart/confirmed LSNs, WAL retained behind the slot, `wal_status`,
  synced-on-standby. A red banner names streams with a lost
  (invalidated) slot.
- **JSON** — everything the pages render from is under `/api/v1/...`
  (`clusters`, `backups`, `restores`, `restore-points`, `migrations`,
  `streams`).

The migrations, streams and restore-point panels need `--catalog-dsn` (the
operator wires it); without it the UI is Kubernetes-only.

## Security

The UI authenticates nobody and its RBAC is get/list/watch only, with no
Secret access. Keep the Service internal and front it with an
authenticating ingress or proxy. Give `--catalog-dsn` a read-only role.
