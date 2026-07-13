---
title: Backup and restore
description: Coordinated multi-shard backup sets with pgBackRest and restore across shard counts.
---

# Backup and restore

:::info Milestone 1 design contract
This page specifies the required behavior. Coordinated backup and restore are not
implemented in the foundation release; see [implementation status](../project/status.md).
:::

The Milestone 1 design uses pgBackRest with a separate stanza and repository
prefix for each shard. One physical backup per shard protects its primary and
replicas because they share physical history.

The preferred source is the healthiest, most caught-up secondary. pgBackRest's standby backup still coordinates with the primary. If no safe secondary exists, pgshard falls back to the primary and records that decision in status, events, and metrics.

## Coordinated backup set

```mermaid
flowchart LR
  B[Back up every shard concurrently] --> V[Verify base backups]
  V --> G[Cluster mutation barrier]
  G --> D[Drain distributed transactions]
  D --> L[Record restore LSN and timeline per shard]
  L --> W[Verify required WAL archived]
  W --> M[Publish immutable cluster manifest]
```

The barrier is acquired through a monotonically fenced `shardschema` operation.
Poolers stop new application writes and drain 2PC. The DDL, role/grant, reshard,
backup/restore, topology and operator reconcilers stop starting or activating
mutations at the same barrier. A safety failover invalidates the attempt and
forces a fresh backup set rather than silently changing its timeline.

While the barrier is held, the coordinator records frozen catalog, routing,
schema, authorization, topology and fencing epochs before capturing each
shard's restore LSN and timeline. Those epoch values and the complete mutation
barrier identity are part of the immutable manifest.

The manifest also identifies the cluster, PostgreSQL major, source topology,
every pgBackRest backup ID, checksums, and backup source role. A backup is usable
only when every shard backup, required WAL object, and the final manifest exist.
Retention treats the complete set as one unit.

:::caution Recovery point boundary
Milestone 1 restores coordinated backup-set points. Arbitrary wall-clock cross-shard PITR is not supported because a distributed commit can straddle a timestamp.
:::

## Restore

Restore accepts an empty target only:

1. Validate the manifest, PostgreSQL 18 compatibility, backup objects, checksums, and required WAL before changing the target.
2. Restore the original source topology to the recorded per-shard positions.
3. Restore `shard-0000`, including `shardschema`, before validating the catalog.
4. Keep application Services non-serving until all shards, roles, grants, and epochs validate.
5. If the requested shard count differs, provision non-serving targets and run the normal reshard workflow.
6. Publish services only after validation succeeds.

The required KIND suite will use an S3-compatible MinIO deployment and cover
standby selection, primary fallback, interrupted uploads, missing objects, and
same/different-count restore. It is not present in the foundation release.
