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
`effectiveShards` (shard count of the serving catalog shard set), `reshard{name, shardSet, generation, shards, phase}`
(the run in flight), `placementWorkflows[]{workflowId, table, from, to, state, phase, message, pauseMs}`
(table placement workflows active or ended within a day), `tuning.derived[]{name, value, reason}`. See
[resharding.md](resharding.md) for how `spec.shards` becomes a reshard and how a `pgshard.tables` edit
becomes a placement workflow.

## PgShardGroup (status subresource)

Status-only mirror of one replication group. `spec{clusterRef, kind: catalog|shard, shardId, shardSet, nonServing}`;
`status{primary, epoch, members[]}`. `nonServing` marks a reshard target group: routers never see it and
its members keep `archive_mode=off` until its shard set becomes serving.

## PgShardBackupPolicy (status subresource)

`spec.objectStore{type: s3|azure|gcs|posix|sftp, bucket, container, endpoint, region, prefix, uriStyle: host|path, verifyTLS, credentialType, credentials.secretRef, encryption.secretRef, sftp{host, user, port, hostKeyCheck}}`,
`spec.schedules{full, differential, incremental}` (five-field cron), `spec.retention{full, differential}`, `spec.logLevel`, `spec.processMax`.
Clusters bind through `spec.backup.policyRef`. `status{observedGeneration, clusters[]{name, lastFullTime, lastDifferentialTime, lastIncrementalTime, healthy, message}, conditions: Valid, BackupHealthy}`.
See [backup.md](backup.md).

## PgShardBackup (status subresource)

`spec{clusterName, type: full|differential|incremental (default full)}`;
`status{phase: Pending|Running|Completed|Failed, backupId, startedAt, completedAt, groups[]{group, stanza, backupId, startLSN, stopLSN, walStart, walStop, sizeBytes, repoSizeBytes, startedAt, completedAt, duration, error}, error, conditions: Progressing, RetentionApplied}`.

## PgShardRestore (status subresource)

`spec{clusterName, newClusterName, clusterSpec (optional PgShardCluster spec; defaults to the source's), backupId, target{time|lsn|name|xid|immediate}, targetTLI, exclusive}`.
CEL: at most one recovery target may be set; `newClusterName` differs from
`clusterName`; `target.name`, `target.xid` and `target.immediate` require
`backupId`. `status{phase: Pending|Restoring|Recovered|Failed, startedAt, completedAt, groups[]{group, sourceStanza, backupId, timeline, reachedTarget, message}, error, conditions: Progressing}`.
See [backup.md](backup.md#restore).

## PgShardReshard (status subresource)

Generated record of one resharding run; the catalog table `pgshard.shard_ranges` is the source of truth.
`spec{clusterName, fromGeneration (min 1), targetGeneration (min 2), targetShardSet, targetShards (min 1), targetRanges[]{shardId, rangeStart, rangeEnd}}`.
CEL: `targetRanges` has exactly `targetShards` entries; `targetGeneration > fromGeneration`.
`status{phase: Pending|Provisioning|Copying|Verifying|Switching|Completed|Cancelled|Failed, workflowId, targets[]{shardId, group, ready, primary}, journalIds[], message, conditions: TargetsReady, WorkflowCreated}`.
Label `pgshard.io/reshard-source` is `spec` or `catalog`. See [resharding.md](resharding.md).
