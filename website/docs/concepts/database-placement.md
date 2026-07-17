---
title: Database topology and placement
description: Independent per-database shard maps with shared or isolated PostgreSQL placement.
---

# Database topology and placement

:::info Milestone 1 design contract
The per-database CRDs, placement scheduler, database move runtime, and restore
workflow described here are not implemented in the foundation release. The
current operator still provisions one cluster-wide set of single-member
PostgreSQL cells; see [implementation status](../project/status.md).
:::

Milestone 1 treats a `PgShardCluster` as one routing and control-plane fleet,
not as one mandatory shard map. Every logical database owns an independent
topology and may use a different number of data shards. The fleet owns a pool
of physical PostgreSQL cells. A cell is one PostgreSQL HA group with its own
volumes, primary, standbys, pgBackRest stanza, and failure identity; it is not
the same thing as a Kubernetes Node.

The target catalog separates these identities:

- A logical database has a stable UUID, user-facing name, topology generation,
  hash algorithm/version and seed.
- A database shard has a stable identity scoped by its logical-database UUID
  and owns one range in that database's active routing epoch. The same
  human-readable shard identifier may therefore exist in both `A` and `B`.
- A placement maps a database shard to a physical PostgreSQL cell.
- A physical cell can host shards from multiple logical databases, or it can
  be reserved for one database.

For example:

| Database | Logical shards | Shared-cell placement | Dedicated placement |
|---|---:|---|---|
| `A` | 5 | `A0..A4 -> cell-0000..cell-0004` | same mapping on five A-only cells |
| `B` | 3 | `B0..B2 -> cell-0000..cell-0002` | `B0..B2 -> cell-0005..cell-0007` |

The first mapping puts `B` on the first three cells used by `A`. The second
gives `B` separate PostgreSQL processes, volumes, nodes, backup stanzas, and
failure domains while retaining the fleet's shared catalog, operator, and
pooler control plane. `shardschema` remains on the fleet's bootstrap anchor,
`cell-0000`, for Milestone 1; that cell is a physical catalog placement and is
not a promise that every database routes application shard zero there.

## Placement policies

The target `PgShardDatabase` API supports four explicit policies:

- `SharedCell`: place database shards in existing compatible PostgreSQL cells.
  This gives the highest density, but CPU, memory, I/O, failover, physical
  backup, and restore blast radius are shared.
- `SharedNode`: use separate PostgreSQL cells and PVCs but schedule them on the
  same Kubernetes Nodes as selected cells. PostgreSQL-process, volume, and
  backup isolation are retained; node capacity, kernel, and node failure remain
  shared.
- `Dedicated`: create database-only cells with required pod and node
  anti-affinity. No other database is scheduled into those cells.
- `Explicit`: map each database-shard ordinal to stable cell IDs. The operator
  validates capacity, PostgreSQL compatibility, failure-domain constraints,
  and duplicate or missing mappings before creating workloads.

Placement is never inferred from matching ordinals. A request to share the
first three cells used by `A` records those exact stable cell IDs so later
scaling or resharding cannot silently move `B`. Resource-derived PostgreSQL
tuning uses the cell's total limits and reservations for every colocated
database. Per-database connection, worker, memory, I/O, WAL-retention, and
admission budgets bound operator-managed work, but they are not hard isolation:
databases in one PostgreSQL process still share CPU scheduling, memory, WAL,
checkpoint, autovacuum, I/O, failover, and superuser trust domains. Workloads
that require hard process, volume, or failure isolation must use `Dedicated`.

## Routing and management scope

The PostgreSQL startup database name selects a logical database before SQL is
routed. Poolers cache a separate routing epoch for each database, then map its
range to a physical cell. DDL, role/grant propagation, backup barriers,
resharding, change streams, and distributed-transaction recovery operate only
on that database's active placements. Milestone 1 does not allow one client
transaction to span two logical databases.

Application roles are logical-database scoped. On shared cells the operator
materializes collision-free physical role names derived from the database UUID;
it does not expose one database's PostgreSQL roles to another database.

## Topology identity

A database topology fingerprint covers PostgreSQL major, hash
algorithm/version, hash seed, logical shard count, ordered shard ordinals, and
every half-open range boundary. It excludes logical-database, database-shard,
and restore-incarnation UUIDs, physical cell IDs, and Kubernetes Node names.
Restoring `A` as `B` allocates fresh database, database-shard, and restore
identities while preserving the same ordinals and ranges. Source identities are
recorded only as immutable provenance, so `A` and `B` can coexist without
sharing durable identities. Physical placement may move to replacement
hardware.

Restore has no topology override. An absent destination is created from the
backup fingerprint. A pre-created destination must be empty, non-serving, and
have the exact same fingerprint. Any mismatch is an error before Secrets,
PVCs, PostgreSQL cells, or backup objects are mutated. The restore reports the
permanent `RestoreTopologyMismatch` error and identifies the differing fields;
it does not retry an impossible topology as a transient controller failure.
Changing topology is a separate
[online database move or reshard](../operations/online-resharding.md) after the
exact restore completes.

The Milestone 1 restore engine initially materializes that exact topology on
new `Dedicated` cells only. It rejects direct restore to `SharedCell`,
`SharedNode`, or `Explicit` placement with `RestorePlacementUnsupported`.
Sharing or explicit placement is a later online move after the dedicated copy
has been validated and activated; restored physical bytes are never attached
to an already-serving shared cell.

## Backup consequences of sharing

pgBackRest is physical-cell scoped, not logical-database scoped. A backup of
`A` therefore references every cell that stores an `A` shard. If one of those
cells also stores `B`, the encrypted base backup necessarily contains `B`'s
physical bytes. Restore starts those cells only inside a quarantined staging
cluster, exposes no Services, logically imports only the requested database
into fresh dedicated destination cells, and destroys the staging volumes after
verification. Unselected databases are never registered, copied, or made
queryable in the destination.

Dedicated placement avoids that retention and restore coupling at the cost of
more PostgreSQL cells and resources.
