# Resharding

Resharding changes the number of shards (or the ranges they own) without
touching the serving groups until the new ones are ready. This document
covers the model, naming, the phases implemented so far and cancellation.
The copy, verify and switch stages are later milestones; their extension
points are named below.

## Model

The shard map lives in the catalog as **shard sets**
(`pgshard.shard_sets`, `pgshard.shard_ranges`). Every set is one
generation of the map:

| State | Meaning |
|-------|---------|
| `serving` | Routers route by it. `default` is generation 1 and is serving from the moment the catalog exists. |
| `desired` | A pending map: ranges are written, nothing is provisioned yet. |
| `provisioning` | The controller opened the reshard workflow; the operator is bringing the target groups up. |
| `retired` | A former serving set kept until `spec.resharding.retireOldGroupsAfter` elapses. |

The catalog table is the source of truth. Two things write pending sets:

1. `PgShardCluster.spec.shards`. When it differs from the serving shard
   count (`status.effectiveShards`) the operator splits the whole `int8`
   key space into N equal ranges (`placement.Split`) and writes them as
   shards `0..N-1` of a new set in state `desired`.
2. SQL. Inserting ranges with a new `shard_set` name (a split or a merge of
   the serving ranges, written in one transaction) registers the set as
   `desired` through a trigger; the operator adopts it the same way.

On first contact with a new cluster the operator materializes the serving
set itself: the `default` set gets `ServingShards` equal ranges (the
`spec.shards` the cluster was created with, default 1) and
`status.effectiveShards` records the count. From then on
`spec.shards` is a reshard request, not the group count.

## Naming

| Object | Generation 1 | Generation n >= 2 |
|--------|--------------|-------------------|
| shard set | `default` | `g<n>` |
| group | `shard-<id>` | `shard-<id>-g<n>` |
| pods, PVCs, Services, ConfigMaps, PDBs, Lease | `<cluster>-shard-<id>-…` | `<cluster>-shard-<id>-g<n>-…` |
| record | — | `PgShardReshard/<cluster>-reshard-g<n>` |

Generations never collide: shard 0 of the old map and shard 0 of the new
one are different groups with different names, Leases and slots. Every
object of a shard group carries the label `pgshard.io/shard-set`.

## The record: PgShardReshard

The operator creates one `PgShardReshard` per pending set, owned by the
cluster, with `spec{clusterName, fromGeneration, targetGeneration,
targetShardSet, targetShards, targetRanges[]}` copied from the catalog and
the label `pgshard.io/reshard-source: spec|catalog`. It is a record, not a
request: editing it changes nothing. Its status carries the phase, the
workflow id, per-target readiness (`targets[]{shardId, group, ready,
primary}`), the `TargetsReady` and `WorkflowCreated` conditions and a
message. `PgShardCluster.status.reshard` points at the active record and
the `Resharding` condition summarises it.

Only one reshard is active per cluster. While one is active a different
`spec.shards` is refused: the `Resharding` condition reports reason
`ReshardActive` and nothing is materialized until the run completes or is
cancelled.

## Target groups

The operator provisions the target groups exactly like serving ones
(`replicasPerShard` members each, same storage and tuning, failover and
rollouts apply) with these differences:

- `PgShardGroup.spec.nonServing=true`, `spec.shardSet=g<n>`.
- The agents run with `nonServing: true`: `archive_mode=off` and no
  pgBackRest stanza until the set becomes serving.
- `pgshard.shard_status` rows for the target set are published with
  `serving_state='provisioning'`; the controller never flips them to
  `serving` and never publishes the set in `pgshard.serving`. Routers
  route by the `default` set only, so targets are invisible to them.
- `PgShardCluster.status.shards` lists serving shards only.

## Phases

| Phase | Set by | Meaning |
|-------|--------|---------|
| `Pending` | operator | Pending set and record exist; no workflow yet. |
| `Provisioning` | controller | The desired-state reconciler created a `pgshard.workflows` row (`kind=reshard`, `state=provisioning`, `status.stage=provisioning`, `spec{shard_set, generation, ranges}`) and moved the set to `provisioning`. The operator reports target readiness on the record. |
| `Copying` | controller | Every target shard has a `shard_status` row with a primary endpoint; the workflow moved to `state=running`, `status.stage=ready_for_copy`. The copy stage is the first extension point (later milestone): today it is a no-op marker. |
| `Verifying`, `Switching`, `Completed` | later milestones | Verification, the write switch (`spec.resharding.pauseBefore` holds before `switchWrites` or `complete`), stanza creation for the new groups and retirement of the old ones. |
| `Cancelled` | operator | See below. |
| `Failed` | controller | The workflow failed. |

The controller state machine is in `internal/controller/reshard.go`; the
operator side in `internal/operator/reshard.go`.

## Cancel

Reverting `spec.shards` to the serving count while a **spec-sourced** run
is `Pending` or `Provisioning` cancels it: the operator drops the pending
set (ranges, `shard_sets` row and its `shard_status` rows), deletes every
object of the target groups (pods, PVCs, Services, ConfigMaps, PDBs,
Leases, `PgShardGroup`s) and marks the record `Cancelled`; the controller
sees the set vanish and cancels the workflow. Catalog-sourced runs are
cancelled by deleting the set's rows in SQL; the operator then tears the
targets down the same way. Once a run reached `Copying` a revert is
refused (`ReshardActive`) until the later stages can roll it back safely.

The `Cancelled` record stays for the audit trail; the next reshard gets the
next generation and a new record.
