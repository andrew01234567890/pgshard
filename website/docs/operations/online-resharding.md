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
space while applications continue to use the old routing epoch.

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

Failures before activation leave serving routes untouched and allow target cleanup or resume. After activation, old shards are read-fenced and quarantined for 24 hours. Milestone 1 has no automatic reverse-replication rollback.

The same engine is used when a backup created with one shard count is restored into an empty cluster requesting another count: first restore the source topology privately, then reshard before publishing services.
