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
  controller's `StreamMonitor` copies every slot's state into
  `pgshard.stream_status` and marks the stream `lost`.

## Pooler `Stream` RPC

See [pooler.md](pooler.md): batched `ChangeBatch`es per transaction or per
`batch_bytes`, `Ack` advancing `confirmed_flush_lsn`, keepalive batches when
idle, resume from the confirmed position with `start_lsn = 0`, and one
reader per slot.

## Catalog

`pgshard.streams` (`name`, `database`, `two_phase`, `state`, `created_at`)
and `pgshard.stream_status` per `(stream, shard)` — see
[catalog.md](catalog.md).

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
serves `pgshard.v1.VStream` with the router↔pooler mTLS material
(`--pooler-tls-*`, or plaintext with `--insecure-dev`).

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
    different shards (round robin by default, oldest commit first with
    `align_skew`). In-progress (streamed) transactions are reassembled from
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
  - an invalidated slot or an LSN no longer retained ends the stream with
    `Error{POSITION_TOO_OLD}`; a shard map generation change ends it with
    `Error{RESHARDED}`, or with a `Journal` (participants, no targets yet)
    when `stop_on_reshard` is set — following a reshard arrives with the
    journal rows;
  - `align_skew` holds a shard whose next commit timestamp is ahead of every
    other shard's last delivered commit by more than `align_skew_ms`
    (default 1000) until they catch up or `align_timeout_ms` (default 10000)
    passes;
  - buffering is bounded: each shard may run at most 16 assembled
    transactions ahead of the consumer; beyond that the pooler stream stalls
    (flow control back to the walsender).
- `two_phase` on `Stream` requires a stream created with `two_phase`; it
  adds `Prepare`, `CommitPrepared` and `RollbackPrepared` events and
  `Begin.gid` for prepared transactions. Without it, prepared transactions
  are decoded at commit time as ordinary transactions.

Example with `grpcurl` against a development router (`--insecure-dev`):

```sh
grpcurl -plaintext -d '{"stream":"orders","database":"app","two_phase":true}' \
  127.0.0.1:15600 pgshard.v1.VStream/Create
grpcurl -plaintext -d '{"start":{"stream":"orders","options":{"heartbeat_interval_ms":1000}}}' \
  127.0.0.1:15600 pgshard.v1.VStream/Stream
# resume from a VGtid and ack it (acks are later messages on the same call;
# the unary form works while a Stream is open)
grpcurl -plaintext -d '{"stream":"orders","position":{"shards":[{"shard":{"shard_set":"default","shard_id":0},"lsn":25165824}]}}' \
  127.0.0.1:15600 pgshard.v1.VStream/Ack
grpcurl -plaintext -d '{"stream":"orders"}' 127.0.0.1:15600 pgshard.v1.VStream/Drop
```
