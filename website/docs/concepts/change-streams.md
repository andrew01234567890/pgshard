---
title: Change streams
description: VStream-like change data capture using PostgreSQL pgoutput.
---

# Change streams

:::info Milestone 1 design contract
This page specifies the required behavior. Source code can decode PostgreSQL 18
replication envelopes and the buffered, streamed, and two-phase `pgoutput`
transaction controls, Relation and Type schema metadata,
Insert/Update/Delete/Truncate row bodies, and—when the accepted command
explicitly enabled `messages`—custom logical Message records without
allocation. A segment-layout state machine derives streamed schema,
row, and custom-message XID prefixes from Stream Start/Stop rather than caller
selection. Stream Start identifies the top-level transaction, while each
schema or row prefix may identify a subtransaction. A custom Message inside a
stream must be transactional and repeat the active top-level XID. Live
PostgreSQL 18 coverage shows that a Message emitted inside a savepoint retains
that top-level XID while the Relation record carries the savepoint XID. It does
not yet implement a complete
transaction-order machine, relation cache, slots, acknowledgements, durable
replay, snapshots, cross-shard merge, or a stream service; see [implementation
status](../project/status.md).

The source also contains a fixed-size PostgreSQL 18 Standby Status Update
encoder. It validates that neither flush nor apply is ahead of write but does
not decide when progress is durable or safe to acknowledge. PostgreSQL permits
apply to be ahead of flush for locally written, unflushed work, so the actual
within-sample checks are `flush <= write` and `apply <= write`. The future
stream owner must advance its persisted checkpoint before reporting the
corresponding flush position. A state machine scoped to one COPY-BOTH session
rejects any write, flush, or apply regression across samples all-or-nothing.
After disconnect, the owner discards volatile write and apply progress and
starts a new tracker with all positions at the last durable checkpoint.
The live PostgreSQL 18 fixture sends the encoded frame in COPY-BOTH mode and
proves that the server records distinct write, flush, and apply positions and
honors its immediate-reply request after the initial catch-up keepalive is drained.
:::

Milestone 1 will expose a cluster change stream derived from PostgreSQL 18
`pgoutput`. It is similar in purpose to Vitess VStream: clients consume one
logical stream across shards while positions remain a vector rather than an
invented global WAL order.

## Guarantees

- Preserve WAL order and transaction boundaries within each shard.
- Deliver at least once from the last acknowledged vector checkpoint.
- Never claim strict order between independent shards.
- Carry distributed-transaction identifiers without pretending participant events are one globally ordered batch.
- Protect resume tokens with authenticated versioning plus stream, cluster,
  database, semantic configuration, epoch, each per-shard source-attachment
  identity, timeline and LSN, checkpoint generation and ordinal, and
  reshard-journal generation.

Only a `Checkpoint` carries an acknowledgeable resume token. Tokens are opaque,
server-issued, and authenticated. The server rejects altered, cross-stream,
cross-configuration, and future tokens. It accepts only checkpoints delivered
on the current RPC; duplicate or stale authenticated acknowledgements are
idempotent no-ops, so durable acknowledgement and slot feedback never regress.
Heartbeats expose non-acknowledgeable source progress and the last fully
delivered position, so a consumer cannot acknowledge past buffered WAL it has
never received.

The token contains a canonical authenticated hash of every shard's restore
incarnation, PostgreSQL system identifier, and database OID independently of the
stream's semantic-configuration hash. Resume and acknowledgement validate that
attachment vector before reading or advancing any checkpoint or replication
slot. During restore, one non-serving `shardschema` transaction installs the new
shard incarnations, advances the affected checkpoint generations, and marks the
streams as requiring a snapshot. A token from the restored history therefore
returns `SOURCE_INCARNATION_CHANGED` without changing durable checkpoint or slot
state, even if its cluster, database, timeline, and LSN fields still match.

Milestone 1 buffers or spills PostgreSQL streaming-transaction chunks until the
terminal outcome is known. Aborted transactions expose no row events. A committed
transaction's begin, row events and terminal commit are emitted contiguously in
source order. Prepared rows remain buffered until `COMMIT PREPARED`; no checkpoint
can advance beyond an unresolved prepared transaction.

Each durable stream config sets maximum events and canonical payload bytes for
one transaction, a maximum for any individual data event, and a separate bound
for control events. Canonical payload bytes encode only the selected event
message; per-connection sequence/timestamp fields and transport framing are
excluded, so a replay cannot change whether data fits. Clients cannot request an
unacknowledged window below the transaction or individual-event maxima.

`Checkpoint` and the other bounded control events do not consume the data
window. A transaction exactly at the limit can therefore still emit its sole
acknowledgeable checkpoint. Oversized transactions emit no part and terminate
with `TRANSACTION_TOO_LARGE`; an oversized snapshot row, relation, schema, or
reshard journal terminates with `EVENT_TOO_LARGE`. Both stop at the last
acknowledged token and require a larger durable limit before resuming. Delivery
limits are excluded from the token's semantic configuration hash, so monotonic
limit increases remain compatible with an existing token.

`StreamError` and `ResnapshotRequired` are terminal responses followed by a
non-OK gRPC status. A normal retry uses only the explicitly returned last
acknowledged token; a resnapshot response cannot be treated as a checkpoint.

Consumers must durably apply a checkpoint before acknowledging it. Reconnection can replay changes after the last acknowledgement; consumers therefore need idempotency or deduplication. Exactly-once delivery is not claimed.

Snapshots emit bounded checkpoints throughout row copy. Their opaque token and
durable server state bind the retained snapshot-session ID, copy phase, and each
shard's relation plus deterministic chunk cursor; the WAL vector alone is not a
snapshot cursor. Snapshot-holder sessions run independently of the client RPC,
so a gateway or client reconnect can continue the same exported snapshots with
at-least-once replay after the last acknowledgement. If a holder, slot, or
exported snapshot is lost before copy completes, the service returns
`ResnapshotRequired` instead of combining a new snapshot with the old WAL vector.

## Standby-first slot topology

All pgshard-managed `pgoutput` consumers use one placement policy. This includes
public change streams, online-reshard catch-up and its target materializers, and
future internal materializations. Normal decoding runs on an eligible direct
physical standby to keep logical decoding work off the shard primary. This is a
placement preference, not permission to lose or skip data: if no standby can
prove that it retains the durable per-shard checkpoint, pgshard either uses the
primary's anchor slot or fences the consumer and requires a new snapshot.

`shardschema` is the authority for every managed logical consumer. Each
per-shard record is keyed by consumer, `logical_database_id`, and shard. Its
source-attachment key adds the shard restore incarnation, PostgreSQL system
identifier from `pg_control_system()`, and database OID; the database name is
descriptive metadata, not identity. It also records the bounded purpose,
primary anchor, selected source server and timeline, standby-local slot and
consistent point, durable checkpoint and generation, and ownership fence. A
consumer cannot attach to a slot until those fields match its current catalog
epoch and lease.
Physical replicas share the system identifier and database OID, so an ordinary
promotion can retain the attachment after its timeline checks. A reinitialized
shard has a different system identifier. A restore can reuse both the system
identifier and database OID, so every initial bootstrap and coordinated restore
installs a fresh immutable shard restore-incarnation UUID before slot
reconciliation or application service. Any mismatch is fenced and requires a
compatible snapshot instead of rebinding the record. This prevents workers,
databases, restored histories, or different uses such as a public stream and a
reshard materializer from sharing a slot or advancing each other's checkpoint.

PostgreSQL's synchronized logical slots and standby-local decoding slots have
different jobs. A synchronized slot copied from the primary cannot be consumed
on a hot standby before that standby is promoted, and a logical slot created
locally on a standby cannot be marked as a failover slot and synchronized to
its peers. Milestone 1 therefore keeps two explicit classes of slot per logical
consumer and shard:

- a persistent `failover = true` anchor on the current primary, advanced no
  further than the durable checkpoint stored in `shardschema`; and
- persistent, non-failover decoding slots created locally on eligible standbys,
  from which the active stream worker consumes `pgoutput`.

The operator automatically synchronizes each primary anchor to eligible direct
standbys for promotion safety. Standby-local slots are independent and
reconciled separately; pgshard never describes them as synchronized and never
treats a PostgreSQL-synchronized slot as usable on a server that is still in
recovery. `shardschema` records each local slot's consistent point.
A new local slot is ineligible while that point is ahead of the durable
checkpoint because PostgreSQL cannot decode the missing older WAL through that
slot. The old source or primary anchor remains active until the checkpoint
reaches the new consistent point; if neither retains the gap, the stream
requires a new snapshot. Source selection is fenced by shard term, restore
incarnation, system identifier, database OID, timeline, catalog epoch, slot
identity, consistent point, and the durable checkpoint. A safe source change
starts from that checkpoint and can replay already acknowledged WAL, so the
public contract remains at-least-once.

For every shard that can host a decoder or receive a synchronized anchor, the
operator enforces the PostgreSQL 18 prerequisites as one configuration unit:

- `wal_level = logical`, sufficient `max_replication_slots`, and sufficient
  `max_wal_senders` on the primary and every eligible standby;
- `hot_standby = on`, `hot_standby_feedback = on`, a bounded positive
  `wal_receiver_status_interval`, and `sync_replication_slots = on` on eligible
  standbys;
- one durable physical slot per standby, named by its `primary_slot_name`, plus
  a valid database name in `primary_conninfo`; and
- a primary `synchronized_standby_slots` policy containing the physical slots
  whose receipt must gate failover-anchor progress.

`hot_standby_feedback` is mandatory for these managed standbys, not merely a
tuning default. It carries the standby logical slots' catalog horizon upstream;
turning it off, setting its reporting interval to zero, or accepting stale
feedback can let primary vacuum invalidate standby decoding or synchronized
slots. Decoder eligibility therefore requires recently observed upstream
feedback, not configuration alone. Operator reconciliation rejects an override
that disables feedback and fences an assigned decoder if the observed setting
or feedback becomes unhealthy. The physical slot is also mandatory because
feedback alone disappears across a standby disconnect or restart. Replicas that
are explicitly excluded from both decoding and promotion-slot synchronization
need not enable feedback; with the default topology, every managed promotion
candidate is eligible and therefore has it enabled.

Since both facilities can retain WAL and dead catalog tuples, the operator
exposes retained-byte, retained-age, `catalog_xmin`, synchronization-lag,
invalidation, and feedback-health metrics. Retention caps continue to prefer
database availability. The operator first durably fences the stream and records
that a new snapshot is required, then stops and drops or safely advances the
offending logical slots. It verifies that every upstream physical slot's
`catalog_xmin` and retained WAL clear or advance. If a disconnected standby
cannot send clearing feedback within the bound, the operator removes it from
eligibility, drops its primary-side physical slot, and requires a full standby
rebuild before recreating that slot. Merely fencing a consumer is never treated
as proof that retained storage was released.

Milestone 1 KIND and Docker Desktop end-to-end suites must cover public streams
and reshard materializers under steady standby decoding, primary write-load
offload, slot synchronization, standby restart, consumer restart, promotion,
decoder-source replacement, lag and invalidation, feedback loss, timeline
change, and resumption from the last durable checkpoint without gaps. They must
also prove that independent consumers cannot share or advance each other's
slots, a consumer cannot attach to a slot for another logical or source
database, same-name and same-OID databases with a different system identifier
are rejected, and restoring the same system identifier requires a new restore
incarnation. A synchronized slot is never consumed while its server is still a
standby, and loss of every safe source fails closed. These suites are planned
and not yet present.

## Snapshot plus changes

```mermaid
sequenceDiagram
  participant G as Stream service
  participant P as Poolers
  participant S as Shards
  G->>P: Acquire snapshot-init barrier
  Note over P,G: Block writes, DDL, reshard activation,<br/>and semantic stream-config changes
  G->>S: Create slots and exported snapshots
  G->>S: Lock selected relations for copy lifetime
  S-->>G: Start LSN vector
  G->>P: Release snapshot-init barrier
  G->>S: Copy rows from snapshots
  G-->>G: Emit bounded copy checkpoints
  G-->>G: Emit SnapshotComplete
  G->>S: Consume pgoutput from start vector
```

The short barrier coordinates snapshot initialization; it does not manufacture
a global PostgreSQL snapshot. Application writes, DDL activation, routing or
topology activation, reshard cutover, and semantic stream-configuration changes
cannot cross this window. In-flight distributed transactions and recovery are
drained or held at the same barrier before any per-shard slot is created, so the
assembled shard set, semantic configuration hash, and start-position vector are
one coherent catalog epoch.

Before that barrier is released, each retained snapshot transaction acquires
`ACCESS SHARE` on every selected relation in deterministic OID order and holds
the locks through `SnapshotComplete`. Normal DML can resume after initialization,
but managed DDL activation that would alter, swap, or drop copied storage waits
or fails its bounded activation deadline. This pins relation identity and
storage for all later chunks rather than relying on the MVCC snapshot alone.

Completing row copy is not itself permission to discard replay state. Before a
holder releases its snapshot transaction and relation locks, every snapshot
event not yet covered by an acknowledgement—including `SnapshotComplete`—is
written to the durable, bounded stream spool. That spool survives client and
gateway reconnects and remains until the checkpoint covering `SnapshotComplete`
is durably acknowledged. If the spool cannot be persisted, the holder and locks
remain; timeout or retention exhaustion terminates the snapshot and requires a
new one rather than releasing unreplayable state.

## Resharding and schema

Managed DDL produces a `Schema` event only after every shard activates the new schema epoch. Reshard activation emits a durable journal mapping old range positions to the target topology. Old tokens follow this journal chain or terminate with `ResnapshotRequired`; topology changes must never silently create a gap.

## WAL retention safety

Slow consumers retain WAL. Each stream has acknowledgement deadlines, inactivity limits, warning thresholds, and a hard retained-WAL cap. At the cap, database availability wins: pgshard fences the stream, removes its slots, and requires a fresh snapshot. A restored cluster also requires external consumers to resnapshot because timelines can fork.
