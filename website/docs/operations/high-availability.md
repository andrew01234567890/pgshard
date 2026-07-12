---
title: High availability
description: Replication, leases, fencing, promotion, restarts, and buffering.
---

# High availability

:::info Milestone 1 design contract
The Rust agent/orchestrator health surfaces and fail-closed in-memory fencing
models exist, but PostgreSQL observation, durable lease integration, promotion,
recovery and rolling restarts are not implemented; see
[implementation status](../project/status.md).
:::

The target default is one primary and two physical streaming replicas per shard,
spread across failure domains. PostgreSQL will use `synchronous_commit=on` with
`ANY 1` synchronous standby acknowledgement. An explicit asynchronous policy is
a durability downgrade and must be surfaced as such.

## Primary fencing

The primary must hold a renewable shard/term lease in the three-member etcd cluster. The local agent self-fences PostgreSQL before it can outlive an unsafe lease. Poolers route writes only to the primary identity and term currently authorized by the lease.

Promotion requires a candidate whose WAL and prepared-transaction state prove that all acknowledged commits are present. If no candidate satisfies that condition, pgshard stops writes instead of risking split brain or acknowledged-data loss.

## Planned maintenance

For a PostgreSQL restart, the orchestrator catches up and promotes a replica before restarting the old primary. It performs one member operation at a time and respects disruption budgets.

Pooler Deployments use multiple replicas, topology spread, readiness draining, and a pre-stop period. Existing TCP sessions can still receive a disconnect when their pooler exits; endpoint availability does not imply transparent session migration.

## Query buffering

During a short, recognized primary outage, poolers can buffer eligible new autocommit requests in a bounded per-shard FIFO. They never blindly replay a write whose execution outcome is unknown. Buffer time, requests, bytes, and per-client contribution are capped; exceeding a limit produces a clear transient error.
