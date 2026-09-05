# Resharding

> Major-version upgrades reuse this machinery with a 1:1 range map and
> new-major target groups; see [upgrade](upgrade.md).

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

### One cluster is one keyspace

A shard set is a **generation of one cluster-wide map**, not an independent
routing domain. `Snapshot.ServingShardSet()` returns a single value for the
whole snapshot, so every database in a cluster is routed by the same serving
set and moves to a new one together.

That is a deliberate model, and it has a consequence worth stating plainly:
a reshard's write fence is **cluster-wide for its flip window**.
`Snapshot.Migrating()` reports whether *any* shard of the serving set is
fenced, and `internal/router/fence.go` holds new writes on that answer, so
databases that are not being resharded are held for the same window as the
one that is. The hold is bounded by the cutover pause (measured in
[the cutover section](#cutover)), not by the copy, which is why the model is
workable — but it is not isolation.

Isolation between unrelated workloads is therefore **a separate
`PgShardCluster`**, not a boundary inside one. Vitess models several
keyspaces per installation, each with its own sharding, traffic policy and
workflow scope; pgshard does not, and one installation is the unit that
reshards, upgrades and fences together. If you need one database to reshard
without touching another's write path, run two clusters.

Placement workflows are the exception that shows the rule: `TableMigrating`
fences only the tables under a placement change, so a re-key holds writes for
its own tables rather than the cluster (PGS-485 tracks whether the keyspace
boundary should become first class).

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
| `Verifying` | controller | `stage=awaiting_switch_writes`: the switch gate is evaluated every pass (see below). |
| `Switching` | controller | `stage=switching`: the write switch steps run; `status.cutover.step` names the one in flight. |
| `Completing` | controller | `stage=switched` then `completing`: the new map serves, the old groups stay up with reverse replication until `retireOldGroupsAfter` elapses. |
| `Completed` | controller | Replication objects dropped, old set retired, old groups deleted by the operator. |
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
     integer keys, `hashtextextended(key::text, seed)` for text/varchar keys (blank-padded character types are not accepted as shard keys),
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
Runs past `Copying` cannot be cancelled: once the journal row exists the
switch is the point of no return (see Cutover).

The `Cancelled` record stays for the audit trail; the next reshard gets the
next generation and a new record.

## Cutover

After `catch_up_done` the copier (`internal/controller/cutover.go`, the SQL
side in `cutoverpg.go`) moves the workflow to `awaiting_switch_writes`.
There is no separate read switch: the router has no read-replica routing,
so reads stay on the sources until the flip.

### Gate

The stage waits until, on every target subscription, the apply lag is
under `--copy-lag-bytes`, every relation in `pg_subscription_rel` is `r`,
no subscription worker is stalled, and — when
`spec.resharding.pauseBefore` is `switchWrites` — the record carries
`pgshard.io/proceed: switchWrites` (comma-separated; the operator mirrors
`spec.resharding` and the annotation into the workflow spec every pass).

### Switch steps

`status.cutover.step` records the step in flight; every step is
idempotent, so a controller crash anywhere repeats at most one step:

1. `fence` — `shard_status.migrating=true` on the source shards and a
   `pgshard.workflow_locks(kind=reshard)` row. Routers see the flag in the
   next snapshot and hold new writes to those shards in the failover
   buffer; poolers refuse new `PREPARE TRANSACTION` on fenced shards with
   `57P03` and a retry hint. In-flight transactions finish.
2. `drain` — waits until `pg_prepared_xacts` is empty on every source (the
   resolver drives the outcome).
3. `sweep` — `LOCK TABLE ... IN SHARE MODE` per sharded table on each
   source in a short transaction under `lock_timeout`.
4. `positions` — `pg_current_wal_lsn()` per source, kept in the record.
5. `catch_up` — every forward subscription's `latest_end_lsn` reaches its
   source position.
6. `verify` — per table, range and target: `count(*)`, `sum(h)` and
   `bit_xor(h)` where `h = hashtextextended(row::text, 0)`, under the source
   position vs the target. A mismatch aborts the switch (fence released,
   workflow `failed`) before anything irreversible.

   Two combinations rather than one because a sum is commutative and
   additive: any two rows swapped for two others of the same total pass a
   sum unnoticed, and rows that sum alike generally do not XOR alike. The
   hash is 64-bit for the same reason — at a few million rows a 32-bit
   collision is unremarkable. It remains a digest, not a row-by-row
   comparison: an online, resumable per-key diff before the fence is
   PGS-478.
7. `reverse` — publications on the targets (`pgshard_reshard_g<gen>_rev_s<src>`,
   same row filters, target -> source direction) and disabled subscriptions
   on the sources (`origin=none, copy_data=false, create_slot=true`),
   created while the targets are still non-serving.
8. `journal` — the point of no return. A row keyed by a uuid allocated
   before the first attempt goes into
   `pgshard_journal.resharding_journal` in every user database of every
   source (the replicated stream) and into the catalog's
   `pgshard.resharding_journal`; `workflows.journal_ids` records it.
   Stream consumers following JOURNAL rows are wired in a follow-up task.
9. `flip` — one catalog transaction: targets `serving`, sources `retired`,
   `pgshard.serving` published for the new set, database home shards
   moved, `shard_map_generation` bumped so poolers reject the old
   generation.
10. `swap_replication` — the sources stop taking new writing transactions
    (`default_transaction_read_only`), **the writers already open are waited
    out**, the final positions are sampled and confirmed applied on the
    targets, forward subscriptions are disabled (dropped on complete), the
    sources are made writable again and reverse subscriptions are enabled.

    The drain is not belt and braces. `default_transaction_read_only` is
    read when a transaction *starts*, so the pause stops new writers and
    nothing else — and the router deliberately lets a transaction that
    opened before the fence carry on writing. Without the wait, such a
    commit lands after the positions were sampled and after forward
    replication is gone, acknowledged on a source that is about to be
    retired and replicated nowhere. A drain that does not finish within
    the writer-drain timeout retries the step with the sources writable
    again, rather than failing the workflow or holding writes down.
11. `release` — `migrating=false`, lock row removed. Routers replay the
    buffered writes against the new map.

The pause (`fence` raised to `flip` committed) is written to
`status.cutover.pause_ms` and mirrored to
`PgShardReshard.status.cutoverPause`. A switch that has not reached the
journal within `--cutover-timeout` (default 60s) is undone (fence
released) and retried; after `--cutover-attempts` (default 3) the
workflow fails. Steps after the journal retry until they succeed.

Global sequences live in the catalog, so nothing moves with the data.
The M4 DDL applier must honor `pgshard.workflow_locks` (kind `reshard`)
and defer migrations of affected tables while a row exists.

## Complete

`switched` holds for `spec.resharding.retireOldGroupsAfter` (default 24h)
and, when `pauseBefore` is `complete`, for `pgshard.io/proceed: complete`.
`completing` drops the forward and reverse subscriptions, slots and
publications on both sides and ends the workflow at `completed`. The
operator then deletes the retired groups' pods, PVCs and Services; their
stanzas stay under the backup retention. `PgShardCluster.status.reshard`
reports `retiredShardSet` during the window.

## Merges

A merge is a reshard whose pending set has fewer shards than the serving
one: `spec.shards` decreased, or ranges of the serving set written as one
new set with fewer, wider ranges. Nothing in the workflow is specific to
the direction. Every source publishes one `pgshard_reshard_g<gen>_t<target>`
publication per target with the target's range as the row filter, so a
target whose range spans several old shards receives the union of their
rows; every target subscribes once per (target, source) pair, so a merge
2 -> 1 runs two subscriptions on the single target. The verify step digests
per (table, range, target) over every source. `TestReshardMergeOnPostgres`
drives a 2 -> 1 merge through copy and cutover under a ledger of transfers.

## Table placement workflows

Editing a `pgshard.tables` row of a table that already has an effective
placement (`shard_key` change, `unsharded` <-> `sharded`, either ->
`reference`, `reference` -> `sharded`) creates one `kind=table_placement`
workflow (state `pending`, `spec{database, schema_name, table_name, from,
to, desired_generation}`); `table_status.workflow_id` points at it. The
placer (`internal/controller/placement.go`, one pass every
`--placement-interval`) moves the rows within the serving shard set; no
group is provisioned. Reshards and placement workflows never run together:
a placement waits at `preparing` while a reshard is active, and a reshard
waits at `ready_for_copy` while a placement is active. Concurrent
placements of different tables are serialized per table through
`pgshard.workflow_locks(kind=table, key=database.schema.table)`.

Where a placement holds a table: `sharded` on every shard, by the hash of
its key over the serving ranges; `unsharded` on the database's home
shard; `reference` on every shard, copied from the home shard.

| Stage | Meaning |
|-------|---------|
| `preparing` | Waits for reshards; reads the table on the first source: it needs a primary key (rows are applied by it), the new shard key must exist, hash as a row-filter type and be part of the primary key or a unique constraint. A violation fails the workflow with the reason in `error`. Takes the lock. |
| `shadow` | `<table>__pgshard_new` on every shard of the new placement: `CREATE TABLE ... (LIKE <table> INCLUDING ALL)` where the shard has the table, else built from the source's columns, defaults (sequences created as needed), identity, constraints and indexes. |
| `copying` | Per source: `REPLICA IDENTITY FULL`, a `pgshard_place_<id8>` publication of the table and a `pgshard_place_<id8>_s<source>` pgoutput slot (`pg_create_logical_replication_slot`), then a keyset walk by primary key under one `REPEATABLE READ` snapshot, every row upserted into the shadow of the shard the new placement assigns. The snapshot is taken after the slot exists, so changes in between are replayed by the catch-up; the upserts make the overlap harmless. |
| `catch_up` | The slots are read with `pg_logical_slot_peek_binary_changes` (pgoutput protocol 1, text tuples, decoded by `internal/controller/pgoutput.go`), each transaction applied to the shadows by the new placement (`routeChange`: insert -> upsert; delete -> delete by key; an update whose row moves shard, or whose key changed -> delete on the old shard + upsert on the new), then the slot is advanced past the commit. The stage ends when the slot lag is under `--copy-lag-bytes`. |
| `buffering` | `table_status.migrating=true`: routers hold new writes to the table (statements resolving it) in the failover buffer for at most the buffering window, then refuse with `57P03`. The placer drains the slots until two consecutive passes (200ms apart) applied nothing with no lag; a drain longer than `--placement-buffer-timeout` releases the fence and returns to `catch_up` (three times, then the workflow fails). |
| `swapping` | One transaction per shard: `<table>` -> `<table>__pgshard_old` where it existed, `<table>__pgshard_new` -> `<table>` where the new placement holds it; sequences owned by the old table's columns move to the new one. Then one catalog transaction publishes the placement (`table_status.effective_placement/effective_shard_key/effective_generation`, `migrating=false`, lock removed, `shard_map_generation` bumped); routers reload and release the buffer. `status.placement.pause_ms` is fence to publish. Slots and publications are dropped. |
| `retiring` | After `--placement-drop-old-after` (or `spec.drop_old_after_seconds`) the old tables drop and the new table's indexes and constraints lose the `__pgshard_new` infix. |
| `completed` / `failed` / `cancelled` | Terminal. |

Every stage re-derives its work from the shards (shadow present, slot
present, source copied, table renamed), so a restarted controller resumes
where the previous one stopped and a step that ran twice changes nothing.
Reverting the `pgshard.tables` row before `swapping` cancels the run: the
placer drops the shadows, slots and publications, restores the replica
identity, releases fence and lock (`cancelled`). After the swap the run
cannot be cancelled; edit the row again for a new run. A failed change is
not retried until the row is edited again.

The swapped table keeps its name but gets a new relation OID. Publications
`FOR ALL TABLES` keep publishing it; logical consumers that follow the
stream see a new Relation message under the same name and continue. The
cluster status reports the runs as `status.placementWorkflows[]{workflowId,
table, from, to, state, phase, message, pauseMs}` (active runs and those
that ended within a day).

## Limitations

- No cancel after the journal; a reverse flip (`switch_back`) is not
  implemented.
- Archiving and stanza creation for the new groups are not switched on
  by the cutover; the operator's backup reconciliation covers groups it
  considers serving on its next pass.
- Reads have no separate switch (no read-replica routing).
- Writes to fenced ranges wait in the router's failover buffer for at
  most the buffering window; a pause longer than that surfaces as
  `57P03` to clients.
- Placement workflows need a primary key and refuse TRUNCATE in the
  stream; DDL on the table during a run fails it (the columns changed).
  Rows with a NULL new shard key fail the run.
- A move is refused at preflight when the table carries something the
  shadow build cannot reproduce, because a swap that dropped it would leave
  the table looking correct and enforcing nothing: **inbound** foreign keys
  (a constraint on another table points at this one by OID, and the swap
  leaves it aimed at the retired table), rules, a non-default replica
  identity, inheritance or partition membership, and publication membership.
- **A table's own foreign keys are reproduced when they can hold**, which is
  when the referenced table is a **reference** table: every shard has all of
  its rows, so the key holds wherever the moved table lands. A key pointing
  at an unsharded table (home shard only) or a sharded one (split, so a row
  and the row it references can land apart) is refused, naming the table and
  its placement. They are added while the shadow is still empty, so the
  constraint validates at no cost and every copied row is checked as it
  arrives.
- **The owner and table/column privileges are reproduced.** The shadow is
  built by the controller, so without this the swap would hand the
  application's table to the controller's role with every grant gone. Both
  are applied at the swap, after the rename and in that order — the grants
  while the controller still owns the table, the owner change last — so it
  keeps the rights it needs while it is still building. Statements are
  rendered from `aclexplode`, including column grants and `WITH GRANT
  OPTION`; their grantor is the controller's role rather than the source's
  owner, which a later `REVOKE` by the owner is unaffected by.
- **Row-level security is reproduced**, not refused: the policies are
  recreated on the shadow (from `pg_policy`, since PostgreSQL has no
  `pg_get_policydef()`) while RLS is still off there, so the copy is not
  filtered by the policies it is copying, and the swap enables `ROW LEVEL
  SECURITY` and `FORCE ROW LEVEL SECURITY` in the transaction that renames
  the table.
- **User triggers are reproduced** from `pg_get_triggerdef`, retargeted onto
  the shadow, and then **disabled for the copy**: a `BEFORE` trigger would
  rewrite every copied row and an `AFTER` trigger would fire for a row the
  source already fired on. The swap restores each to the state the source
  had, disabled and `REPLICA`/`ALWAYS` triggers included. A move is still
  refused when a trigger calls a function the target shards do not have —
  pgshard does not fan out function DDL, so the refusal names the function
  and where to create it, rather than failing part-way through building the
  shadow.
