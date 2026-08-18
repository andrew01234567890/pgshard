# pgshard.io/v1alpha1 custom resources

Generated CRDs live in `config/crd/bases/`. Regenerate with `make generate manifests`;
run the API-server validation tests with `make envtest` (downloads envtest binaries into `bin/k8s`).

## PgShardCluster (`psc`, namespaced, status subresource)

Print columns: Shards, Ready, Age.

| Field | Type | Default / constraint |
|---|---|---|
| `spec.postgresql.major` | int | required; `18` or `19` |
| `spec.postgresql.image` | string | optional override |
| `spec.postgresql.profile` | enum | `oltp` (default), `mixed`, `analytics` |
| `spec.postgresql.parameters` | map[string]string | must not contain `fsync`, `full_page_writes`, `wal_level`, `max_prepared_transactions`, `ssl`, `synchronous_commit` (CEL, message names the key) |
| `spec.resources` | corev1.ResourceRequirements | per PostgreSQL pod |
| `spec.catalog.replicas` | int | default 3, min 1; CEL: `>= 3` ("catalog.replicas must be >= 3 for HA") |
| `spec.catalog.storage.size` / `.storageClassName` | Quantity / *string | size required |
| `spec.shards` | *int | optional, min 1 |
| `spec.replicasPerShard` | int | default 3, min 1; CEL: `>= 3` ("replicasPerShard must be >= 3") |
| `spec.storage.size` / `.storageClassName` | Quantity / *string | size required |
| `spec.durability.synchronousCommit` | enum | `on` (default), `remote_apply` |
| `spec.durability.minSyncStandbys` | int | default 1 |
| `spec.router.minReplicas` / `.maxReplicas` | int | defaults 2 / 10; CEL: `maxReplicas >= minReplicas` |
| `spec.router.hpa.cpuUtilization` | int | default 70 (1..100) |
| `spec.router.tls.secretRef` | LocalObjectReference | optional |
| `spec.admin.enabled` | *bool | default true |
| `spec.backup.policyRef` | string | name of a PgShardBackupPolicy |
| `spec.resharding.retireOldGroupsAfter` | Duration | default `24h` |
| `spec.resharding.pauseBefore` | enum | `none` (default), `switchWrites`, `complete` |
| `spec.upgrade.strategy` | enum | `online` (default), `offline` |
| `spec.upgrade.maxParallelGroups` | int | default 1, min 1 |

Status: `conditions` (types Ready, Progressing, Degraded, PrimaryHealthy, ReplicationHealthy,
Fenced, BackupHealthy, Resharding, ServingWrites, RouterReady, TuningApplied), `observedGeneration`,
`shardMapGeneration`, `shards[]{id, rangeStart, rangeEnd, primary, epoch, members[]{name, role, ready, replayLagBytes}}`,
`tuning.derived[]{name, value, reason}`.

## PgShardGroup (status subresource)

Status-only mirror of one replication group. `spec{clusterRef, kind: catalog|shard, shardId}`;
`status{primary, epoch, members[]}`.

## PgShardBackupPolicy

`spec.objectStore{type: s3|azure|gcs|posix|sftp, bucket, container, endpoint, region, prefix, credentials.secretRef, encryption.secretRef}`,
`spec.schedules{full, differential, incremental}` (cron), `spec.retention{full, differential}`.

## PgShardBackup (status subresource)

`spec{clusterName, type: full|differential|incremental (default full)}`;
`status{phase, backupId, startedAt, completedAt, conditions}`.

## PgShardRestore (status subresource)

`spec{clusterName, backupId, target{time|lsn|name|xid|immediate}, targetTLI, exclusive}`.
CEL: at most one recovery target may be set. `status{phase, conditions}`.

## PgShardReshard (status subresource)

`spec{clusterName, targetShards (min 1)}`; `status{phase, journalIds[], progress{rowsCopied, rowsTotal, replicationLagBytes}, conditions}`.
