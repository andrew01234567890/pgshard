---
title: High availability
description: Replication, leases, fencing, promotion, restarts, and buffering.
---

# High availability

:::info Milestone 1 design contract
The Rust agent/orchestrator health surfaces and fail-closed in-memory fencing
models exist. They bound authenticated lease lifetimes and atomically reject an
operation unless its catalog epoch, fencing epoch and deadline match at
execution. An opt-in local lifecycle boundary can structurally preflight
required PostgreSQL 18 files, supervise one direct postmaster child with client
TCP and replication ingress disabled, propagate unexpected exit, and perform
bounded smart/fast/immediate signal escalation and HTTP drain. Final cleanup is
deliberately fail-closed rather than time-bounded: the PGDATA fence stays held
until the direct child is reaped and its process group has no live descendant,
including when supervision is cancelled. A filesystem-backed exclusive
supervisor lock prevents two pgshard agents that share PGDATA from starting
concurrently; it does not replace the future operator lease and storage fencing
needed before activation. The boundary rejects standby and archive-recovery
signal files and also runs the immutable PostgreSQL 18 `pg_controldata` sibling
to verify its CRC-backed report. Recovery control states are rejected even if a
signal file was lost, rather than silently turning former standby storage into
a writable primary. It does not bootstrap or activate a server.
PostgreSQL observation, replication, durable lease integration, promotion,
automated recovery and rolling restarts are not implemented; see
[implementation status](../project/status.md).
:::

The target default is one primary and two physical streaming replicas per shard,
spread across failure domains. PostgreSQL will use `synchronous_commit=on` with
`ANY 1` synchronous standby acknowledgement. An explicit asynchronous policy is
a durability downgrade and must be surfaced as such.

## Primary fencing

The primary must hold a renewable shard/term lease in the three-member etcd cluster. The local agent self-fences PostgreSQL before it can outlive an unsafe lease. Both the orchestrator authority and receiving agent reject expired or overlong leases; configured TTL bounds are enforced by the state machines, not merely logged. Poolers route writes only to the primary identity and term currently authorized by the lease.

Promotion requires a candidate whose WAL and prepared-transaction state prove that all acknowledged commits are present. If no candidate satisfies that condition, pgshard stops writes instead of risking split brain or acknowledged-data loss.

Managed logical consumers, including public change streams and reshard
materializers, prefer a healthy physical standby as their decoding source. The
primary retains a failover anchor at the last durable consumer checkpoint, and
PostgreSQL 18 automatically synchronizes that anchor to managed promotion
candidates. Because PostgreSQL does not allow a synchronized logical slot to be
decoded on a hot standby, normal standby decoding uses a distinct standby-local
slot. Promotion and source changes are catalog-fenced and may replay events,
but they must never skip an event. See
[change streams](../concepts/change-streams.md#standby-first-slot-topology) for
the slot roles, required settings, retention bounds, and failure tests.

## Planned maintenance

For a PostgreSQL restart, the orchestrator catches up and promotes a replica before restarting the old primary. It performs one member operation at a time and respects disruption budgets.

Pooler Deployments use multiple replicas, topology spread, readiness draining, and a pre-stop period. Existing TCP sessions can still receive a disconnect when their pooler exits; endpoint availability does not imply transparent session migration.

## Query buffering

During a short, recognized primary outage, poolers can buffer eligible new autocommit requests in a bounded per-shard FIFO. They never blindly replay a write whose execution outcome is unknown. Buffer time, requests, bytes, and per-client contribution are capped; exceeding a limit produces a clear transient error.
