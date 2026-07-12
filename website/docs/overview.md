---
id: overview
title: pgshard documentation
slug: /
sidebar_position: 1
description: Learn what pgshard is, what Milestone 1 provides, and where its correctness boundaries lie.
---

# pgshard documentation

pgshard is the design and implementation of a PostgreSQL 18 sharding platform
with a latency-sensitive Rust data plane and a Kubernetes-native Go operator.
Milestone 1 targets PostgreSQL wire-compatible routing and pooling, shard
orchestration, coordinated backup/restore, online schema changes, online
resharding, and `pgoutput` change streams.

:::caution Alpha milestone
Milestone 1 is an alpha engineering target, not a production-readiness claim.
Most runtime features are not implemented yet. Check the [implementation
status](./project/status.md) before using any command or guarantee.
:::

## Choose a path

| If you want to… | Start here |
|---|---|
| Validate the current source | [Quickstart](./quickstart.md) |
| See what is actually implemented | [Implementation status](./project/status.md) |
| Understand component and trust boundaries | [Architecture](./concepts/architecture.md) |
| Evaluate transaction guarantees | [Distributed transactions](./concepts/distributed-transactions.md) |
| Check supported SQL | [SQL compatibility](./reference/sql-compatibility.md) |
| Plan recovery | [Backup and restore](./operations/backup-restore.md) |
| Move data without an outage | [Online resharding](./operations/online-resharding.md) |
| Consume a cluster change stream | [Change streams](./concepts/change-streams.md) |
| Contribute safely | [Development](./project/development.md) |

## Milestone 1 target invariants

- PostgreSQL 18 is the only supported major.
- Durable shard metadata lives in the `shardschema` database on `shard-0000`; etcd contains leases, not the authoritative shard map.
- Applications connect through pooler services and do not receive a direct PostgreSQL Service.
- Distributed transactions provide atomic final outcomes at `READ COMMITTED`, with documented phase-two visibility skew.
- Routing, schema, authorization, and reshard cutovers are activated through monotonic epochs.
- Restore operates on complete coordinated backup sets and an empty target.
