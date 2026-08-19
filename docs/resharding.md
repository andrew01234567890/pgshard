# Resharding

Resharding changes the number of shards (or the ranges they own) without
touching the serving groups until the new ones are ready. This document
covers the model, naming, the phases implemented so far (provisioning and
the copy phase) and cancellation. The verify and switch stages are later
milestones; their extension points are named below.

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
| `Copying` | controller | Every target shard has a `shard_status` row with a primary endpoint; the workflow moved to `state=running`, `status.stage=ready_for_copy`. The copier takes over: `copying` while schemas, publications and subscriptions are set up and the initial copy streams, `catch_up_done` once every table is ready and the apply lag is under `--copy-lag-bytes`. The record stays `Copying` through both stages; the verify step (later milestone) starts from `catch_up_done`. |
| `Verifying`, `Switching`, `Completed` | later milestones | Verification, the write switch (`spec.resharding.pauseBefore` holds before `switchWrites` or `complete`), stanza creation for the new groups and retirement of the old ones. |
| `Cancelled` | operator | See below. |
| `Failed` | controller | The workflow failed. |

The controller state machine is in `internal/controller/reshard.go`; the
operator side in `internal/operator/reshard.go`.

## Copy phase

The copier (`internal/controller/copy.go`, one pass every
`--copy-interval`) drives every `kind=reshard` workflow in a copy stage.
Each pass re-derives what is missing from the shards' own catalogs
(`pg_database`, `pg_publication`, `pg_subscription`), so a restarted
controller continues wherever the previous one stopped, and the per-step
record under `status.copy` is informational except for the schema flags.

1. **Schema materialization.** For every row of `pgshard.databases` and
   every target the controller creates the database on the target and asks
   the agent of the target primary (`Agent.MaterializeSchema`, address =
   `shard_status.primary_endpoint` host + `--agent-port`) to run
   `pg_dump --schema-only --no-publications --no-subscriptions` against the
   database's home shard piped into `psql -v ON_ERROR_STOP=1`. Tables,
   indexes, constraints, sequences, types, views and grants come across;
   roles already exist on every group. `status.copy.schema[db][target]`
   records success; a database without the flag is dropped and recreated
   before the next attempt so a half-applied dump never survives. With
   `--pg-bin` (or `PGSHARD_PG_BIN`) the controller runs the binaries
   itself instead of calling agents.
2. **Publications on every source**, per database, publishing
   `insert, update, delete` (never TRUNCATE: the router refuses `TRUNCATE`
   on every table while a shard set is provisioning):
   - `pgshard_reshard_g<gen>_t<target>` for every target: every sharded
     table `FOR TABLE t WHERE (<hash>(key) >= lo AND <hash>(key) <= hi)`
     with the target's range. The hash expression is the one the router's
     placement port mirrors: `hashint8extended(key::int8, seed)` for
     integer keys, `hashtextextended(key::text, seed)` for character keys,
     `uuid_hash_extended(key, seed)` for uuid, seed =
     `HASH_PARTITION_SEED`. Other key types fail the workflow.
   - `pgshard_reshard_g<gen>_ref` on the database's home shard only:
     every reference table, unfiltered; every target subscribes to it.
   - `pgshard_reshard_g<gen>_home` on the home shard only: every other
     table of the database (registered unsharded tables and unregistered
     ones), unfiltered; only the **home target** subscribes to it. The
     home target is the target whose range contains keyspace id 0: the
     successor of the home shard for unsharded data.

   Publishing UPDATE and DELETE requires a replica identity that covers
   the row-filter columns. Tables without a primary key, and sharded
   tables whose primary key does not include the shard key, get
   `REPLICA IDENTITY FULL` on the source before the publication is
   created; `status.copy.replica_identity_full` lists them so a later
   stage can revert it.
3. **Subscriptions on every target**, per database, one per
   (target, source) pair named `pgshard_reshard_g<gen>_t<target>_s<source>`
   (also the slot name on the source) with
   `copy_data=true, create_slot=true, streaming=parallel, two_phase=false,
   origin=any` and the source's direct conninfo
   (`--subscription-dsn-template`, placeholders `{set} {id} {group} {db}`).
   Slot creation waits for every running transaction on the source, so
   before creating one the controller lists `pg_prepared_xacts` there,
   runs the resolver once, and retries next pass while any remain
   (`status.copy.blocked_by`); after `--copy-prepared-wait` the workflow
   fails.
4. **Progress.** `pg_subscription_rel` states and
   `pg_stat_subscription.latest_end_lsn` against the source's
   `pg_current_wal_lsn()` are aggregated into `status.progress`
   (`subscriptions, tables_total, tables_ready, lag_bytes, paused`) and
   `status.message`; the operator copies the message onto the record.
5. **Throttling.** The largest physical standby lag over the sources
   (`pg_stat_replication` minus the reshard walsenders) pauses every
   subscription (`ALTER SUBSCRIPTION ... DISABLE`) above
   `--copy-throttle-high-bytes` and resumes them below
   `--copy-throttle-low-bytes` (hysteresis). Target bloat is not measured
   yet.

Tables registered or created after the publications exist are not picked
up (the M4 DDL applier runs its own migrations; resharding under DDL is a
later step). Sequences come across with their definitions only; values
are not replicated.

## Cancel

Reverting `spec.shards` to the serving count while a **spec-sourced** run
is `Pending` or `Provisioning` cancels it: the operator drops the pending
set (ranges, `shard_sets` row and its `shard_status` rows), deletes every
object of the target groups (pods, PVCs, Services, ConfigMaps, PDBs,
Leases, `PgShardGroup`s) and marks the record `Cancelled`; the controller
sees the set vanish and cancels the workflow. Catalog-sourced runs are
cancelled by deleting the set's rows in SQL; the operator then tears the
targets down the same way. A run in `Copying` cancels the same way: the
reconciler moves the workflow to `state=cancelled, stage=cancelling`
and the copier drops the subscriptions on the targets it can still
reach, then every `pgshard_reshard_g<gen>_*` slot (terminating its
walsender) and publication on the sources, and ends at
`stage=cancelled`. Targets the operator already deleted are skipped.
Runs past `Copying` cannot be cancelled yet.

The `Cancelled` record stays for the audit trail; the next reshard gets the
next generation and a new record.
