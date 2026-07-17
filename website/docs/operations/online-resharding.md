---
title: Online resharding
description: Change the number of shards while traffic continues.
---

# Online resharding

:::info Milestone 1 design contract
The copy, catch-up, journal and activation runtime are not implemented in the
foundation release; see [implementation status](../project/status.md).
:::

Online resharding will change equal ranges in pgshard's versioned 64-bit hash
space for one logical database while applications continue to use its old
routing epoch. Other databases sharing the same fleet or physical cells retain
their own routing epochs and continue serving.

```mermaid
flowchart LR
  P[Provision non-serving targets] --> S[Export source snapshots]
  S --> C[Bulk copy by target range]
  C --> A[Apply pgoutput changes]
  A --> V[Validate coverage and checksums]
  V --> F[Brief write fence and catch-up]
  F --> E[Publish routing epoch]
  E --> Q[Quarantine old shards]
```

Each source shard's snapshot and `pgoutput` catch-up slot are created on the
same eligible physical standby when one can retain the required point. This
keeps both the bulk-copy scan and logical materialization workload off the
primary. The materializer is a managed logical consumer with its own slot,
ownership fence, and durable checkpoint in `shardschema`; it never shares the
public change-stream slot. Its change reader and target applier are supervised
tasks in the `pgshard-pooler` stream-worker sidecar, not a separate stream
Deployment. A synchronized primary failover anchor protects promotion, while
independently created standby-local slots provide decoding before promotion. If
the selected standby or its snapshot holder is lost, pgshard resumes only from
a source that proves checkpoint coverage or restarts the affected snapshot. It
never combines a snapshot from one history with a slot that cannot prove the
same start point.

## Activation safety

- Key ranges must cover the complete hash space exactly once.
- Target rows are validated by counts and chunk checksums.
- Activation buffers eligible writes, drains old-epoch transactions, captures source fence LSNs, and waits for targets to catch up.
- Poolers that have not acknowledged the barrier lose readiness before the new epoch is published.
- No old-epoch request is accepted after activation.
- CDC consumers cross the topology through a durable reshard journal.

Pooler acknowledgement alone is not the fence. Every physical database stores
the accepted logical-database generation, and a PostgreSQL extension checks the
authenticated internal generation at transaction start, before every write,
and again before guarded `PREPARE TRANSACTION`. Application SQL cannot set that
generation. A stale or partitioned pooler therefore cannot continue writing to
an old source after the source fence advances, even if it retained an existing
socket and an old catalog snapshot.

Cutover first buffers eligible new requests and drains active old-generation
transactions. It then advances the source-side generation fence, captures the
final LSN vector, and catches up destinations. One `shardschema` CAS publishes
the new routing/name/placement generation. Destination cells arm exactly that
generation, and poolers release buffered work only after all destination and
pooler acknowledgements agree. The interval between the catalog CAS and the
last destination acknowledgement is deliberately non-serving, not partially
serving. A paused pooler can use neither side: sources reject its old generation
and destinations reject requests until it reloads the published generation.

Long-lived sessions stay pinned to the generation on which they authenticated.
The controller waits a bounded time for active transactions to finish, then
terminates remaining old-generation backends with the documented retry or
connection-failure result. An idle old socket is closed before its next
transaction; it is never silently rebound to the new history. Only requests
that have not begun a transaction and satisfy the pooler's replay policy may be
buffered and retried transparently.

Failures before activation leave serving routes untouched and allow target cleanup or resume. After activation, old shards are read-fenced and quarantined for 24 hours. Milestone 1 has no automatic reverse-replication rollback.

## Online database move

The same snapshot, `pgoutput`, validation, fencing, and activation engine moves
a logical database between shared cells, shared-node cells, and dedicated
placement pools. It can preserve the shard map or reshard while copying. The
source database remains writable during snapshot and catch-up. At the final
barrier, poolers buffer eligible requests, drain and fence old-generation
transactions, apply the final per-shard LSN vector, publish one new
name/topology/placement generation in `shardschema`, arm it on every
destination, and release buffered traffic only after all acknowledgements
agree. Physical PostgreSQL database names remain opaque UUID-derived names; the
move never attempts non-atomic `ALTER DATABASE ... RENAME` on every shard.

A restored database may therefore be validated as `B` and then moved online to
the user-facing name `A`. Replacing an existing `A` requires an explicit
replace policy and retains the old generation read-fenced in quarantine. The
cutover selects the restored history; writes committed only to the replaced
generation after the backup are not silently merged into that history.

Restore itself never invokes this engine implicitly. It recreates the exact
logical topology in the backup manifest. A request to restore a five-shard
backup into a three-shard destination fails before target mutation. The caller
must restore to an absent or empty exact five-shard database, then request a
separate online move/reshard.
