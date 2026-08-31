# 10. Making transitions automatic, with pauses as the opt-in

Status: accepted

## Context

Vitess resharding is a sequence of operator commands: `MoveTables`,
`SwitchTraffic`, `Complete`. Each step is a decision point where a human
looks at lag and errors and decides to go on. It is careful, and it is why
a Vitess reshard is a scheduled operation rather than a consequence of
editing a number.

pgshard's charter asks for the opposite: `spec.shards: 1 → 2` should do the
whole thing.

## Decision

Every transition is automatic. Changing `spec.shards`, editing
`pgshard.shard_ranges`, changing a table's `shard_key`, or changing
`spec.postgresql.major` starts and finishes the matching workflow —
provision, copy, verify, switch reads, switch writes, complete, retire.
Manual control exists only as **opt-in pauses**
(`spec.resharding.pauseBefore: switchWrites | complete`,
`pgshard.migrations.postpone`).

Automatic does not mean unguarded. Each transition has a gate before it —
copy complete, apply lag under threshold, no errors, no in-doubt two-phase
transactions on a source, verification passed — and an automatic rollback
path up to its point of no return. For a reshard that point is the journal
row written on the sources; before it, cancelling restores everything, and
after it the workflow is idempotent on re-run.

Restore is the exception and stays an explicit `PgShardRestore`. Everything
else reconciles desired state towards reality; a restore moves reality
backwards, and that is a decision, not drift.

## Consequences

- The gates are the safety argument, so they are tested as such: the e2e
  suites run reshards and upgrades under load with a correctness oracle,
  and the chaos suites kill the controller mid-transition.
- A user who wants Vitess's step-by-step control sets `pauseBefore`. A user
  who edits a shard count by accident gets a reshard, which is why the
  desired-state tables validate aggressively and the CRD is CEL-validated.
- Every workflow must be resumable after the controller dies, because
  nothing is waiting for a human to restart it.
