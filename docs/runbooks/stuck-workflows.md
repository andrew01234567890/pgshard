# Runbook: stuck reshard, placement and upgrade workflows

> The executors are merged. The controller starts the copier and the placer
> whenever shard connectivity is configured, so a `reshard`, `table_rekey`
> or `upgrade` workflow moves data on its own. A workflow that is not
> `pending` has begun physical work, and backing it out is no longer a
> desired-state edit — see [Backing out](#backing-out).

## Inspect

```sql
SELECT id, kind, state, spec, status, error, updated_at
FROM pgshard.workflows ORDER BY created_at DESC;
SELECT database, schema_name, table_name, effective_placement, workflow_id, progress
FROM pgshard.table_status WHERE workflow_id IS NOT NULL;
```

Over gRPC: `Controller.ListWorkflows` / `GetWorkflow` (filter by kind and
state). `PgShardReshard.status` mirrors progress on the Kubernetes side.

## Pause and resume

`Controller.PauseWorkflow` moves a pending or running workflow to
`paused` (the prior state is kept in `status.paused_from`);
`ResumeWorkflow` puts it back. `spec.resharding.pauseBefore: switchWrites`
pauses automatically before the traffic switch.

## A workflow that never starts

Check that the controller leader is alive (it holds a catalog advisory lock; the log says who leads)
and that the desired edit was valid — invalid ranges (gaps, overlap,
missing coverage) are rejected by the catalog triggers at commit, and an
invalid `pgshard.tables` row (e.g. `sharded` without `shard_key`) is
reported by the reconcile pass rather than acted on.

## Backing out

**Check what has already moved before editing anything.** `pending` is the
only state in which nothing has happened. A `paused` workflow has whatever
its `status.cutover.step` and journal say it has, and a run that reached the
journal has passed its point of no return: reversing it is a rollback, not
an edit. Read the durable state first:

```sql
SELECT id, kind, state,
       status->'cutover'->>'step'       AS step,
       status->'cutover'->>'journal_id' AS journal,
       status->>'message'               AS message
FROM pgshard.workflows ORDER BY created_at DESC;
```

A non-empty `journal` means the switch is committed; use the workflow's own
rollback path rather than a desired-state edit.

While a workflow is still `pending` nothing has moved, so backing out is a
desired-state edit:

- **Reshard**: set `pgshard.shard_ranges` (or `spec.shards` /
  `PgShardReshard`) back to the effective layout — the one in
  `pgshard.serving` and the current `shard_status` rows.
- **Re-key**: set the table's `placement`/`shard_key` in `pgshard.tables`
  back to the effective values shown in `pgshard.table_status`.

The reconciler sees desired equal to effective and the workflow becomes
moot; routers never saw a generation change, so no client impact.

## Once the executor lands

The dangerous window is after the write switch: cancelling then means
switching back with reverse replication, not discarding. Rely on
`pauseBefore: switchWrites`, verify (row-set equality, lag near zero) and
only then resume. Old groups are retired after
`spec.resharding.retireOldGroupsAfter` (24h default) precisely so a
just-switched reshard can be reversed.

VStream consumers: a completed reshard ends streams with
`Error{RESHARDED}` (or a `Journal` with `stop_on_reshard`); they resume
against the new shard map ([streams.md](../streams.md)).

## Sources refusing writes with 25006 after a workflow row was deleted

Every write to the old shards fails with `25006 cannot execute ... in a
read-only transaction`, with no hint and no workflow in
`pgshard.workflows` to explain it.

A cutover past its swap step pauses writes on its source set with
`ALTER SYSTEM SET default_transaction_read_only = on`. Every ordinary exit
gives it back: the swap lifts it on success and on each retry, the fatal and
abort paths lift it, an unwind lifts it, and a controller that dies mid-swap
resumes the step and reaches one of those. **Deleting the workflow row
directly does not** — nothing is left to run the release, and `ALTER SYSTEM`
survives a restart.

The controller sweeps for this on every resolve tick and lifts it, logging
`lifted a write pause whose workflow no longer exists`. Wait a tick before
doing anything by hand. What it sweeps:

```sql
SELECT shard_set, shard_id, write_paused_by
FROM pgshard.shard_status s
WHERE write_paused_by IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM pgshard.workflows w
                  WHERE w.id = s.write_paused_by
                    AND w.state NOT IN ('completed', 'failed', 'cancelled'));
```

Rows here are pauses with no live owner — the workflow row was deleted, or
it is in a terminal state, which it cannot be while still inside its swap
step. If the sweep cannot reach a shard it
says so and leaves the claim in place, which is deliberate: the claim is the
only record that the shard is paused, so it is dropped only after the shard
is writable again. Fix the connectivity and the next tick finishes.

To lift one by hand — on that shard's **primary**, not through the router:

```sql
ALTER SYSTEM RESET default_transaction_read_only;
SELECT pg_reload_conf();
```

then clear the claim so the sweep stops revisiting it:

```sql
UPDATE pgshard.shard_status SET write_paused_by = NULL, updated_at = now()
WHERE shard_set = '<set>' AND shard_id = <id>;
```

Do this in that order. A claim left on a writable shard costs one wasted
`ALTER SYSTEM RESET` per sweep; a paused shard with no claim is invisible
again.

Note that a shard paused by an operator for their own maintenance is never
touched: the sweep keys on `write_paused_by`, which only a cutover writes,
and not on `default_transaction_read_only` itself.

The range fence is a separate thing with the same shape — `shard_status.migrating`
and `migrating_by`. A stuck fence makes routers buffer and then refuse with a
retry hint, which is much easier to recognise than this.

## A cluster left write-fenced by a barrier whose controller died

Every write fails with `57P03` and the routers say the cluster is fenced,
with no barrier running.

A certified barrier raises the catalog write fence, and the running
controller lowers it — including on its failure paths. If that process dies
between the two, the fence stays up. Recovery on the next leader lifts it
automatically once the barrier lock is free and the fence is older than the
longest a run can take — but **only when the fence was written on this
server**. A cluster restored to a barrier also comes back fenced, carrying the
same reason and owner, and lowering that one would admit writes before
two-phase reconciliation has finished; what tells them apart is that a
restored fence came out of the backup and so predates this postmaster.

So this section is for the case recovery leaves alone: a fence older than the
catalog primary's last start, which is a restore in progress or a barrier that
died before a restart. Confirm which — a restore has a `PgShardRestore` that
has not reported `Unfenced`:

```sql
SELECT write_fence, write_fence_reason, write_fence_owner, write_fenced_at
  FROM pgshard.shard_map_generation;
```

```
kubectl get pgshardrestore -A
```

If no restore is running and the fence predates the catalog primary's last
start, recovery will not lift it: the next barrier takes it over and releases
it. A policy with a `barrierSchedule` runs one
on its own; otherwise ask the controller for one directly
(`Controller.CreateBarrier`, port 15500 on the controller Service):

```
grpcurl -plaintext -d '{"name":"unfence"}' \
  <cluster>-controller.<namespace>.svc:15500 pgshard.v1.Controller/CreateBarrier
```

If that cannot run either — the controller is the thing that died — clear it by
hand on the catalog primary, as the superuser:

```sql
UPDATE pgshard.shard_map_generation
   SET write_fence = false, write_fence_reason = '', write_fence_owner = '',
       write_fenced_at = NULL;
```

Do not do this while a restore is reconciling: that is the case the automatic
recovery refuses to guess at.
