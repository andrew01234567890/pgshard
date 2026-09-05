# Change streams

Change streams are the per-shard logical decoding feeds that resharding,
VStream consumers and external sinks read. This document covers the pgoutput
decoder, the replication client, slot lifecycle in the agent and controller,
the pooler `Stream` RPC, the catalog rows and the router's `VStream` API
that fans the shard streams into one.

## Decoder (`internal/pgoutput`)

`pgoutput.Decoder` decodes pgoutput protocol version 4 as started with
`proto_version '4', publication_names '…', streaming 'parallel', messages
'on'` and optionally `two_phase 'on'` (binary transfer stays off): Begin,
Commit, Origin, Relation, Type, Insert, Update (key/old/new images), Delete
(key/old), Truncate, logical decoding messages (transactional and not),
Stream Start/Stop/Commit/Abort (parallel mode carries the abort LSN and
time), Begin Prepare, Prepare, Commit Prepared, Rollback Prepared and Stream
Prepare. Tuple columns are null (`n`), unchanged TOAST (`u`), text (`t`) or
binary (`b`). The decoder keeps the relation cache by relation id (column
names, type OIDs, typmods, replica identity flags) and tracks whether it is
inside a streamed segment, where every message carries the transaction id.
`Format` renders a message as one stable line.

Golden captures under `internal/pgoutput/testdata/pg18` and `pg19` are real
frames recorded from both majors (DML with unchanged TOAST and replica
identity full, truncate, messages, a streamed transaction larger than
`logical_decoding_work_mem=64kB` with a rolled-back subtransaction,
two-phase prepare/commit/rollback and a streamed prepare, relation changes
mid-stream). `go test ./internal/pgrepl -run TestCaptures -update` rewrites
them against the local PostgreSQL images; `FuzzDecode` seeds from the same
frames.

## Replication client (`internal/pgrepl`)

A small client over `pgconn` (no pglogrepl): `Connect` forces
`replication=database`; `IdentifySystem`, `CreateLogicalSlot` (PG17+
syntax `CREATE_REPLICATION_SLOT name LOGICAL pgoutput (TWO_PHASE, FAILOVER,
SNAPSHOT 'export'|'use'|'nothing')`), `DropSlot`, `StartReplication SLOT …
LOGICAL lsn (options)` entering CopyBoth, `Receive` returning `XLogData` or
`PrimaryKeepalive` (a context deadline surfaces as a timeout that leaves the
connection usable), and `SendStandbyStatus`.

## Slot lifecycle (agent)

- `CreateStreamSlot(stream, database, two_phase)` creates the publication
  `pgshard_all FOR ALL TABLES` in the database if missing and the slot
  `pgshard_<stream>_<shard>` with `pgoutput`, `failover = true` and the
  stream's `two_phase`. `DropStreamSlot(stream)` drops it (idempotent).
  `ListSlots` reports `active`, `restart_lsn`, `confirmed_flush_lsn`,
  `wal_status`, `invalidation_reason`, `synced`, `failover`, `temporary`,
  `two_phase` and `database`.
- Standbys run with `primary_slot_name`, `hot_standby_feedback = on`,
  `dbname` in `primary_conninfo` and `sync_replication_slots = on`, so
  failover slots are synchronized and survive a promotion.
- `SetSynchronizedStandbySlots(slots)` writes `synchronized_standby_slots`
  (into `pgshard.slots.conf`, included by `postgresql.conf`, then reload)
  keeping only the listed physical slots that exist and are active: a listed
  slot that is missing or inactive stalls every failover-slot walsender. The
  operator calls it wherever it applies `synchronous_standby_names`, with
  the slots of the streaming standbys (`SynchronizedStandbySlots`), and
  with no slots when replication is asynchronous.
- Invalidation: `wal_status = 'lost'` is visible in `ListSlots`; the
  controller's `StreamMonitor` copies every slot's state (including the WAL
  retained behind `restart_lsn` and the `synced`/`failover` flags) into
  `pgshard.stream_status` and marks the stream `lost`. The admin UI shows
  this on `/streams` (see [admin.md](admin.md)).

## Pooler `Stream` RPC

See [pooler.md](pooler.md): batched `ChangeBatch`es per transaction or per
`batch_bytes`, `Ack` advancing `confirmed_flush_lsn`, keepalive batches when
idle, resume from the confirmed position with `start_lsn = 0`, and one
reader per slot.

## Pooler `CopyTables` RPC

`CopyTables(stream, database, publication, two_phase, batch_rows,
done_tables, resume_schema/table/lastpk)` is the per-shard copy phase of an
initial copy (see below). The pooler opens a replication connection and
creates the stream's slot `pgshard_<stream>_<shard>` with `SNAPSHOT
'export'` (`failover`, `two_phase` as requested) when it does not exist yet;
when it exists, a temporary slot `pgshard_copy_<random>` exports the
snapshot instead and disappears with the connection. The first message is
`Snapshot{slot, stream_slot, consistent_point, snapshot_name}`. A second
connection runs `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET
TRANSACTION SNAPSHOT '<name>'` while the replication connection stays open,
lists the tables of the publication (`pg_publication_tables`, so `FOR ALL
TABLES` is expanded inside the snapshot) and, for every table not in
`done_tables`, sends `TableBegin{relation, by_ctid}`, `Rows{rows, lastpk}`
batches of `batch_rows` (default 1000) in primary-key order, then
`TableDone`; `Done` closes the copy. Pagination is keyset on the primary key
(`WHERE (k1, k2) > ($1::t1, $2::t2) ORDER BY k1, k2 LIMIT n`, composite keys
included); a table without a primary key is walked by `ctid` ranges in the
same way (`by_ctid`). `lastpk` is a JSON array of the key's text values
(`["3","k1"]`, `["(0,7)"]` for ctid) and `resume_*` continues a table after
it; rows at or below the checkpoint are never sent again. Rows committed
after the snapshot are invisible to the copy and reach the consumer through
the change stream.

## Catalog

`pgshard.streams` (`name`, `database`, `two_phase`, `state`, `created_at`)
and `pgshard.stream_status` per `(stream, shard)` — see
[catalog.md](catalog.md).

A `Stream` request that names no `shard_set` streams **whichever set is
serving**, not the literal `default`. A reshard or a blue/green upgrade
makes another set serving and retires the old one, and a consumer pinned to
`default` would go on reading shards nothing writes to any more -- without
an error, because those shards and their slots are still there. Naming a set
explicitly still wins, so a consumer draining a retired set on purpose can
say so.

## Creating a stream (controller)

`Controller.CreateStream(stream, database, two_phase, shard_set)` inserts
the `pgshard.streams` row (`creating`), then on every shard of the set
(default `default`) opens a superuser connection to the primary in the
stream's database, ensures `pgshard_all FOR ALL TABLES` and creates
`pgshard_<stream>_<group>` with `pgoutput`, `failover = true` and the
stream's `two_phase` (an existing slot is reused), and finally marks the
stream `active`. A shard that cannot be reached leaves the stream
`creating`; the call can be repeated once it is back. `DropStream` drops the
slot on every shard (missing is fine) and deletes the rows. The controller
needs `--shard-dsn-template` or `--shard-dsn` for this, the same access the
resolver and `StreamMonitor` use; the router has pooler addresses only, so
it forwards `VStream.Create`/`Drop` to the controller (`--controller`).

## VStream API (router)

`pgshard-router serve --vstream-listen host:port [--controller host:port]`

In an operator-deployed cluster both are set for you: the routers listen on
`9091`, published by the router Service as the `vstream` port, and point at
the cluster's controller Service. Consumers dial the router Service, which
serves `pgshard.v1.VStream` with the router↔pooler mTLS material
(`--pooler-tls-*`, or plaintext with `--insecure-dev`).

### What `pgshard.v1` does and does not promise

The wire contract is `pgshard.v1.VStream`, and a consumer reaches it the way
the `grpcurl` examples below do, or by generating stubs from `proto/` in its
own build. That path is supported.

What is **not** available is importing pgshard's own generated Go stubs: their
`go_package` is `internal/gen/pgshard/v1`, and the Go toolchain forbids another
module from importing an `internal` path. A Go consumer generates its own from
the `.proto` files.

The `v1` in the protobuf package is not a stability guarantee. The Kubernetes
API is `v1alpha1` deliberately, and this service is at the same maturity — the
package name predates the question rather than answering it. Whether these
services become a published consumer contract (a publicly importable package
and a compatibility policy) or are declared private with a separate versioned
consumer API is an open decision, tracked as PGS-394. Until it is made, treat
method paths and message shapes as able to change, and pin the commit you
generated from.

- `Create(stream, database, two_phase)`, `Drop(stream)`: forwarded to the
  controller (`UNIMPLEMENTED` without `--controller`). `List` reads
  `pgshard.streams` and `pgshard.stream_status`.
- `Stream` is bidirectional. The first request is a `start` (`stream`,
  optional `position`, `options`); later requests are `ack` positions. The
  router opens one pooler `Stream` per serving shard of the set from the
  shard's LSN in `position` (a shard missing from the vector, or an empty
  vector, resumes from the slot's `confirmed_flush_lsn`) and merges them:
  - every shard transaction is delivered whole and contiguous —
    `Begin{shard, xid, commit_ts}`, `Relation`s as needed, `Row`s,
    `Truncate`s, transactional `Message`s, then `Commit{shard, lsn, end_lsn}`
    (or `Prepare{gid}` with `two_phase`) — never interleaved with another
    shard's events; nothing is promised about the order of transactions of
    different shards, with or without alignment (round robin by default,
    oldest commit timestamp first with `best_effort_wall_clock_alignment`,
    which reorders the presentation without promising anything about it). In-progress (streamed) transactions are reassembled from
    their segments and aborted subtransactions dropped;
  - a `VGtid{position}` follows every transaction boundary
    (`Commit`, `Prepare`, `CommitPrepared`, `RollbackPrepared`); the vector
    holds each shard's end LSN of the last delivered transaction plus the
    shard map generation. Resuming from a `VGtid` replays nothing delivered
    before it and skips nothing after it;
  - `Relation` is keyed by `schema.table` (relation ids are not exposed):
    it is sent before the first row of a table and again when the columns
    change, once per stream across shards;
  - `Heartbeat{position}` every `heartbeat_interval_ms` (default 5000)
    while idle; keepalives from the poolers advance the vector silently
    between transactions;
  - `ack` positions are clamped to what was delivered and forwarded per
    shard to the pooler's `Ack`, advancing the slot's `confirmed_flush_lsn`.
    `VStream.Ack(stream, position)` does the same out of band (it needs an
    open reader on the shard's pooler);
  - a promotion (`primary_epoch` bump in the snapshot) or a dropped pooler
    stream makes the router reconnect the shard to the current primary's
    pooler at the last delivered LSN with backoff; the failover slot exists
    there because it was synchronized (see above). A shard that stays broken
    for longer than the reconnect window (30s) ends the stream with
    `Error{SHARD_UNAVAILABLE}`;
  - an invalidated slot, a slot that is gone, or WAL no longer retained ends
    the stream with `Error{POSITION_TOO_OLD}`, which tells a consumer its
    checkpoints are worthless and it must copy again. Nothing else earns it:
    a publication that does not exist, a permission failure or an option the
    server rejects ends the stream with `Error{INTERNAL}` and its message,
    because those are fixed without discarding anything; a shard map generation change ends it with
    `Error{RESHARDED}`, or with a `Journal` (participants, no targets yet)
    when `stop_on_reshard` is set — following a reshard arrives with the
    journal rows;
  - `best_effort_wall_clock_alignment` holds a shard whose next commit
    timestamp is ahead of every other shard's last delivered commit by more
    than `wall_clock_lead_ms` (default 1000) until they catch up or
    `wall_clock_hold_ms` (default 10000) passes. **It changes presentation
    only.** The timestamps are each shard host's own clock, and a hold is
    released on the timeout whether or not the slow shard caught up, so it
    establishes no causal, real-time or serialization order — the
    no-ordering contract above is unchanged by it. Do not use it to decide
    that one shard's transaction happened before another's; use the shard
    and LSN on every event, which are there either way;
  - buffering is bounded: each shard may run at most 16 assembled
    transactions ahead of the consumer; beyond that the pooler stream stalls
    (flow control back to the walsender). A transaction that has **not**
    committed cannot stall anything -- it is not a unit yet -- so it is
    bounded separately: 64 MiB of uncommitted events per shard and 256
    interleaved in-progress transactions. Exceeding either ends the stream
    with `TRANSACTION_TOO_LARGE`, and the last position delivered is still
    valid: resume from it, with a larger bound or after splitting the
    transaction. Reconnecting without changing either would reassemble the
    same transaction and reach the same limit.
- `start_from: COPY` runs an initial copy for every shard missing from the
  position (see "Initial copy"); `copy_batch_rows` sizes its batches.
- `two_phase` on `Stream` requires a stream created with `two_phase`; it
  adds `Prepare`, `CommitPrepared` and `RollbackPrepared` events and
  `Begin.gid` for prepared transactions. Without it, prepared transactions
  are decoded at commit time as ordinary transactions.

## Initial copy

`Stream` with `options.start_from = START_FROM_COPY` and an empty position
(or a position missing some shards) copies every table of the publication
on those shards before streaming changes, the way logical replication's
tablesync and Vitess VReplication do:

1. The router asks each shard's pooler for `CopyTables`. The pooler creates
   the stream slot with an exported snapshot if the slot does not exist yet
   (a stream created through `Create` already has it, so a temporary slot
   exports the snapshot); the snapshot's `consistent_point` becomes the
   shard's LSN in the vector and is where streaming starts once the copy is
   done. Transactions committed before it are in the copy; transactions
   committed after it arrive as ordinary stream events.
2. Per shard the events are `CopyBegin{shard, schema, table}`, the
   `Relation` (once per table across shards, as for streamed rows), `Row`
   batches with `copy = true` (always `KIND_INSERT`), a `VGtid` after every
   batch, `CopyCompleted{shard, schema, table}` per table and
   `CopyCompleted{shard}` once every table of the shard is copied. Copy
   events of a shard are never interleaved with that shard's streamed
   transactions (they all come first); other shards proceed independently,
   so shard 1 may already stream while shard 0 still copies. Once the last
   shard finishes, a `CopyCompleted` without shard closes the copy phase of
   the stream.
3. `VPosition.copy_state` carries, per shard still copying, the table in
   progress with the last key delivered (`lastpk`) and the tables done;
   `VGtid` and `Heartbeat` vectors include it. Resuming from such a vector
   (with `start_from = COPY`) continues the copy: the pooler exports a new
   snapshot from a temporary slot, skips the done tables and the rows at or
   below `lastpk` of the table in progress, and streaming later starts from
   the original consistent point kept in the vector. A shard with an LSN and
   no copy state simply streams; a shard without either starts its copy.
   Acks for shards still copying are held (there is nothing to confirm on
   the slot yet).

Delivery is at-least-once at batch boundaries and around a resume:

- rows at or below the checkpoint are never resent; rows above it (within
  the batch the consumer did not finish) are resent once;
- a resumed copy sees rows committed between the original consistent point
  and the new snapshot, and the stream from the original point delivers
  them again as transactions;
- `CopyBegin` repeats for a table a resume continues.

Consumers therefore apply copy rows with upsert semantics keyed by the
primary key (and treat stream inserts the same way during and right after
the copy). Tables without a primary key are copied by `ctid` and carry **no
checkpoint**: both the pooler and the router drop it, so a resume or a
reconnect re-sends such a table from its start. That is at-least-once and
never skips — an earlier design resumed from the ctid and could miss rows
after a heap rewrite, which is what this avoids — and the cost is that a
large keyless table is copied again on every reconnect.
The row-count oracle in `test/e2e/router` (`TestRouterVStreamInitialCopy`:
10k rows per shard, concurrent inserts, the consumer killed twice mid-copy)
checks that the upserted consumer state equals the tables on both shards.

Example with `grpcurl` against a development router (`--insecure-dev`):

```sh
grpcurl -plaintext -d '{"stream":"orders","database":"app","two_phase":true}' \
  127.0.0.1:15600 pgshard.v1.VStream/Create
grpcurl -plaintext -d '{"start":{"stream":"orders","options":{"heartbeat_interval_ms":1000}}}' \
  127.0.0.1:15600 pgshard.v1.VStream/Stream
# initial copy of every table, then changes from the copy's consistent point
grpcurl -plaintext -d '{"start":{"stream":"orders","options":{"start_from":"START_FROM_COPY","copy_batch_rows":500}}}' \
  127.0.0.1:15600 pgshard.v1.VStream/Stream
# resume from a VGtid and ack it (acks are later messages on the same call;
# the unary form works while a Stream is open)
grpcurl -plaintext -d '{"stream":"orders","position":{"shards":[{"shard":{"shard_set":"default","shard_id":0},"lsn":25165824}]}}' \
  127.0.0.1:15600 pgshard.v1.VStream/Ack
grpcurl -plaintext -d '{"stream":"orders"}' 127.0.0.1:15600 pgshard.v1.VStream/Drop
```
