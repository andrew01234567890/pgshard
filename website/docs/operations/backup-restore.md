---
title: Backup, restore, and database recovery
description: Database-targeted coordinated backups, exact-topology restores, and online return moves.
---

# Backup, restore, and database recovery

:::info Milestone 1 design contract
This page specifies the required behavior. Coordinated backup, restore, and
database moves are not implemented in the foundation release; see
[implementation status](../project/status.md).
:::

Milestone 1 backups target one logical database. That database may have a shard
count and placement entirely different from every other database in the same
fleet. pgBackRest still operates at physical PostgreSQL-cell granularity: the
backup set references one physical backup for every cell containing a shard of
the selected database.

The preferred source is the healthiest, most caught-up secondary. pgBackRest's
standby backup still coordinates with the primary. If no safe secondary exists,
pgshard falls back to the primary and records that decision in status, events,
metrics, and the immutable manifest.

## Coordinated database backup set

```mermaid
flowchart LR
  T[Select database and active topology] --> B[Back up every referenced cell concurrently]
  B --> V[Verify base backups]
  V --> G[Database mutation barrier]
  G --> D[Drain that database's distributed transactions]
  D --> L[Record restore LSN and timeline per database shard]
  L --> W[Verify required WAL archived]
  W --> M[Publish immutable database manifest]
```

The monotonically fenced operation lives in `shardschema`. Poolers stop new
writes only for the selected logical database and drain its 2PC transactions.
Its DDL, role/grant, reshard, restore, move, and topology reconcilers stop
starting or activating mutations at the same barrier. Other databases may keep
serving, including databases colocated in the same physical cells. A safety
failover of any referenced cell invalidates the attempt and forces a fresh
backup set rather than silently changing timeline.

While the barrier is held, the coordinator records the database's frozen
catalog, routing, schema, authorization, topology, placement, and fencing epochs
before capturing each shard's restore LSN and timeline.

The manifest includes:

- source fleet and logical-database UUID, source name, PostgreSQL major, schema
  epoch, role/grant epoch, and backup barrier identity;
- a topology fingerprint covering hash algorithm/version and seed, shard count,
  stable database-shard identities and ordinals, and every range boundary;
- the physical cell and database OID for each database shard, its source restore
  incarnation, PostgreSQL system identifier, timeline, and selected backup
  source role;
- every pgBackRest stanza, repository object and backup ID, plus checksums and
  the required WAL interval.

A backup is usable only when every referenced cell backup, WAL object, and the
final manifest exist. Retention treats the complete set as one unit.

:::caution Physical backup scope
If `A` and `B` share a PostgreSQL cell, a physical backup needed by `A`
necessarily contains `B`'s bytes too. Repository encryption and authorization
remain cell-scoped. Restore quarantines those bytes and exposes only the
requested logical database. Dedicated placement avoids this coupling.
:::

:::caution Recovery point boundary
Milestone 1 restores coordinated backup-set points. Arbitrary wall-clock
cross-shard PITR is not supported because a distributed commit can straddle a
timestamp.
:::

## Database-targeted restore

A restore names one source database from the manifest and one destination
database name. For example, source `A` may restore as `B` while the current `A`
continues serving. If `A` does not exist at restore time, the destination may be
`A`.

The target API records, at minimum:

```yaml
spec:
  backupSetRef: backup-a-2026-07-16
  sourceDatabase: A
  destinationDatabase: B
  placement:
    mode: Dedicated # SharedCell, SharedNode, Dedicated, or Explicit
```

The destination name must either be absent or reserved by an empty, non-serving
`PgShardDatabase`. Restore does not overwrite an active database. Replacing an
existing name is a later, explicit online database move.

### Exact-topology preflight

Restore never performs implicit resharding. Before creating Secrets, PVCs,
PostgreSQL cells, Jobs, or repository writes, the controller compares the
manifest topology fingerprint with the destination topology:

- An absent destination is created with the exact fingerprint from the backup.
- A pre-created destination must match PostgreSQL major, hash
  algorithm/version and seed, shard count, stable shard ordinals/identities,
  and every range boundary.
- Any mismatch returns the permanent error code `RestoreTopologyMismatch`,
  identifies the manifest fingerprint and each differing topology field, and
  leaves the restore `Ready=False` with the same reason. There is no target
  mutation. A five-shard backup cannot restore into a three-shard destination,
  even if the destination is empty.

Physical cell IDs and Kubernetes Node names are placement rather than logical
topology. The exact five database shards may be placed on replacement cells,
on the first five compatible shared cells, or on five dedicated cells, but they
remain the same five shard identities and ranges.

### Restore execution

1. Validate the manifest, exact destination topology, PostgreSQL 18
   compatibility, backup objects, checksums, and required WAL before target
   mutation.
2. Create a private, non-serving staging cluster with the exact source database
   shard configuration and one restored cell for every referenced physical
   backup.
3. Restore the catalog-bearing backup and every data cell to the recorded
   per-shard positions. No staging Service, pooler endpoint, or application
   credential is published.
4. Validate the restored `shardschema` snapshot, then install a fresh
   restore-incarnation UUID for every restored database shard, advance affected
   consumer checkpoint generations, and require new snapshots. An old resume
   token must fail even when system identifier and database OID are unchanged.
5. Materialize only the selected source database, ordinal for ordinal and range
   for range, into the absent or empty destination. Colocated databases present
   in physical backups are neither registered nor copied.
6. Recreate and validate only that database's declarative roles, grants, schema,
   topology, row counts and chunk checksums. Keep it non-serving until every
   shard and epoch agrees with the manifest.
7. Atomically publish the destination database generation, then destroy the
   quarantined staging cluster and its unselected physical bytes.

Interruption is resumable from durable per-cell restore and per-database copy
receipts. A retry with the same operation ID and canonical request resumes; a
changed destination, topology, placement, backup ID, or source database is a
conflict rather than a new interpretation of the old receipt.

## Returning `B` to `A` online

After validation, `B` can move back to the user-facing name `A` using the
[online database move](./online-resharding.md#online-database-move). The move
keeps the source generation serving during snapshot and `pgoutput` catch-up.
At the final barrier, poolers buffer eligible queries, drain old-generation
transactions, apply the final LSN vector, atomically activate the new
name/topology/placement generation in `shardschema`, and release buffered
traffic.

If `A` still exists, replacement must be explicit. The old `A` generation is
read-fenced and quarantined rather than destroyed at cutover. Selecting an
older restored history intentionally does not merge post-backup writes from the
replaced generation; the operation records that discard boundary for audit.

A topology change is allowed in this separate online move. It is never folded
into restore. For example, restore five-shard `A` as exact five-shard `B`, then
move/reshard `B` into a new three-shard `A` generation while traffic continues.

## Required end-to-end coverage

The KIND and Docker Desktop suites use MinIO and must cover:

- standby backup selection, explicit primary fallback, interrupted uploads,
  missing or corrupt objects, incomplete WAL, and retry receipts;
- `A` with five shards restored as absent `B` with five shards on shared cells,
  shared Nodes, and dedicated cells;
- `A` restored as `A` only when that name is absent;
- rejection of five-to-three restore before any Secret, PVC, Pod, Job, or MinIO
  mutation, including count-equal but range/hash-seed mismatches, with the
  stable `RestoreTopologyMismatch` error and condition reason;
- a physical backup from a cell shared by `A` and `B`, proving only `A` is
  registered or queryable after restore;
- continuous load while restored `B` moves back to `A`, including process,
  primary, pooler, and operator failures before and after the atomic cutover;
- a separate online move that changes shard count and placement after exact
  restore, with old cells quarantined and no failed client request.

This coverage is not present in the foundation release.
