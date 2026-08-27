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
