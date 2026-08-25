# Resharding and table re-keying

> **Status: partial.** The desired-state model, the detection of range and
> key changes, the workflow records and every building block (change
> streams with initial copy, failover slots, role materialization for new
> groups) are merged. The workflow *executor* that moves the data and
> switches traffic is under active development. Until it lands, a
> `reshard` or `table_rekey` workflow is created `pending` and stays there;
> nothing moves. See [capability-matrix.md](../capability-matrix.md).

## How you will trigger it

Resharding is declared, not scripted:

- **Change the shard count**: edit `spec.shards` on the `PgShardCluster`
  (or create a `PgShardReshard` object naming `targetShards`), or edit
  `pgshard.shard_ranges` directly — splits and merges as SQL in one
  transaction:

```sql
BEGIN;
UPDATE pgshard.shard_ranges SET range = int8range(NULL, 0)
 WHERE shard_set = 'default' AND shard_id = 0;
UPDATE pgshard.shard_ranges SET range = int8range(0, 4611686018427387904)
 WHERE shard_set = 'default' AND shard_id = 1;
INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range)
 VALUES ('default', 2, int8range(4611686018427387904, NULL));
COMMIT;
```

A shard set must number its shards `0..N-1` in key order: the shard owning the
lowest range is shard 0, the next is shard 1, and so on. Routing addresses a
shard by its range's position, so a set numbered any other way would be served
under different IDs than it declares; the catalog refuses it at `COMMIT`. The
checks are deferred, so a split or merge may renumber freely inside the
transaction as long as the committed state satisfies the rule.

Ranges belong to a reshard or upgrade once it is under way. While such a
workflow is provisioning, running or paused, the catalog refuses to change the
ranges of either the set it is building or the set it is copying from: the
workflow snapshots the target's ranges when it starts, and every cutover pass
rebuilds the source's shard IDs from live rows, so reshaping either would leave
the run addressing shards that no longer exist. The freeze outlasts the cutover,
because a set is already `serving` while its workflow still holds the reverse
subscription open for the rollback window.

Dropping a whole set stays allowed — that is how a cancelled reshard clears its
target — but only as a whole. The rules live on `pgshard.shard_ranges`, so
deleting a set's row on its own commits; what is refused is editing the ranges
it left behind.

A workflow that never started owns nothing, so a set with only such a workflow
against it stays editable. That covers one still `pending`, and one paused
before it started — pausing records the state to resume into, and a workflow
that would resume to `pending` has not begun.

- **Re-key a table**: change `placement` or `shard_key` in
  `pgshard.tables` for a table that already has an effective placement.

The controller validates the edit (ranges must stay contiguous and cover
the key space) and creates one workflow (`reshard` carrying the desired
ranges, or `table_rekey`) in `pgshard.workflows`; the effective state stays
untouched until the workflow completes.

## The intended flow

The executor follows the Vitess model on the merged streaming machinery
([streams.md](../streams.md)):

1. provision the target groups (the operator creates them; the role
   verifier materializes every role on them);
2. initial copy of each moving table through an exported snapshot with
   per-table checkpoints, then continuous apply from the per-shard change
   streams (failover slots survive promotions);
3. reverse replication, verification (row-set equality oracles under
   `test/e2e/oracle`), then switch reads and writes by bumping the shard
   map generation — routers follow the generation, in-flight statements are
   fenced with `55000` and retried;
4. retire the old groups after `spec.resharding.retireOldGroupsAfter`
   (default 24h). `spec.resharding.pauseBefore: switchWrites` holds the
   workflow before the write switch for manual confirmation.

## Observing and controlling workflows

```sql
SELECT id, kind, state, spec, status, error FROM pgshard.workflows
 ORDER BY created_at DESC;
```

The `pgshard.v1.Controller` gRPC service exposes `ListWorkflows`,
`GetWorkflow`, `PauseWorkflow` and `ResumeWorkflow`; a paused workflow
remembers the state it was paused from. VStream consumers see a reshard as
an `Error{RESHARDED}` (or a `Journal` with `stop_on_reshard`) and resume
against the new shard map.

Operational guidance: [stuck-workflows runbook](../runbooks/stuck-workflows.md).
