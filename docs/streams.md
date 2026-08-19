# Change streams

Change streams are the per-shard logical decoding feeds that resharding,
VStream consumers and external sinks read. This document covers the pieces
that exist today: the pgoutput decoder, the replication client, slot
lifecycle in the agent, the pooler `Stream` RPC and the catalog rows.

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
