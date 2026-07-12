---
title: Online resharding
description: Change the number of shards while traffic continues.
---

# Online resharding

Online resharding changes equal ranges in pgshard's versioned 64-bit hash space while applications continue to use the old routing epoch.

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

## Activation safety

- Key ranges must cover the complete hash space exactly once.
- Target rows are validated by counts and chunk checksums.
- Activation buffers eligible writes, drains old-epoch transactions, captures source fence LSNs, and waits for targets to catch up.
- Poolers that have not acknowledged the barrier lose readiness before the new epoch is published.
- No old-epoch request is accepted after activation.
- CDC consumers cross the topology through a durable reshard journal.

Failures before activation leave serving routes untouched and allow target cleanup or resume. After activation, old shards are read-fenced and quarantined for 24 hours. Milestone 1 has no automatic reverse-replication rollback.

The same engine is used when a backup created with one shard count is restored into an empty cluster requesting another count: first restore the source topology privately, then reshard before publishing services.
