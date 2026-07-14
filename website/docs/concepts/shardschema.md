---
title: Shard metadata and shardschema
description: How pgshard stores and distributes its authoritative topology.
---

# Shard metadata and `shardschema`

:::info Current implementation boundary
The PostgreSQL 18 migration, validated Rust snapshot model, canonical checksum,
multi-epoch lock-free cache, repeatable-read snapshot loader,
LISTEN-before-initial-load primitive, bounded notification and polling driver,
bounded reconnect and stale-readiness supervisor, metrics-ready state, and live
database contract test exist in source. A Linux control executable composes
the supervisor with pooler HTTP/readiness/status and Prometheus publication,
bounded runtime settings, a file-backed DSN, and coordinated shutdown. Its
control HTTP resources and drain are bounded, and its temporary plaintext
connector rejects non-local endpoints. An explicit credential-free bootstrap
mode exposes liveness while reporting the catalog unconfigured and making no
connection attempt. The operator selects that mode until it can provision a
safe catalog transport. Overall application readiness stays false because
there is no SQL data plane. Authenticated TLS, remote catalog transport, and
operator-provisioned credentials are not wired yet; see
[implementation status](../project/status.md).
:::

`shardschema` is a dedicated PostgreSQL database on stable `shard-0000`.
Physical streaming replication will protect it with the rest of that shard. It
is authoritative for logical topology; etcd is used only for ephemeral
leases and fencing.

## Catalog contents

The current internal `pgshard_catalog` migration records:

- Databases, registered tables, shard-key types and hash versions.
- Shard identities and non-overlapping half-open key ranges.
- Routing, schema, authorization, and catalog epochs.
- Permanent fixed-size operation tombstones for idempotency.

Durable DDL, reshard, backup/restore and change-stream journals remain planned
extensions; the current schema does not claim to store them. The planned
logical-consumer registry keys each per-shard record by consumer,
`logical_database_id`, and shard. Its source-attachment key adds an immutable
shard restore-incarnation UUID, PostgreSQL system identifier, and database OID;
the database name remains metadata. It records a bounded purpose, ownership
fence, primary anchor, selected source identity and timeline, standby-local slot
and consistent point, each never-reused slot generation and
generation-encoded name, the exact two-phase activation boundary, durable
checkpoint and checkpoint generation, and whether a new snapshot is required.
The planned live-health record also binds slot-sync success to the current
direct-primary connection generation; it does not mistake that worker's SQL
connection database for the database OID of every slot it synchronizes. Public
streams and reshard materializers receive separate records and slots. Bootstrap
and every coordinated restore install fresh shard restore-incarnation UUIDs and
atomically advance affected checkpoint generations before slot reconciliation
or serving; restoring the catalog's old incarnation value never authorizes
attachment to restored WAL or an old resume token. Retired managed slot names
and generations are permanent tombstones and are never allocated again.

Password material is never stored in `shardschema`.

Each cluster has one routing-hash record containing algorithm version `1` and a
creation-time seed. Both columns are insert-only. Changing either requires an
explicit online reshard into a new hash space; ordinary catalog updates cannot
rewrite them. Rust golden vectors cover every supported key type, numeric and
empty boundaries, XXH3 length boundaries, and seed extremes.

Catalog registration records and validates text-key encoding and collation.
Milestone 1 accepts only `UTF8` plus PostgreSQL's built-in `C` collation so
database equality and byte hashing cannot disagree across shards.

Epochs, WAL positions and 64-bit range bounds use decimal strings in JSON-facing
interfaces because JavaScript numbers cannot represent them exactly. The Rust
core therefore does not derive generic Serde encodings for `u64`/`u128` catalog
types. Protobuf's standard JSON mapping likewise encodes 64-bit integers as
strings; the exclusive keyspace end `2^64` is represented by an absent optional
range end.

## Cache protocol

1. A reader takes ownership of a dedicated idle connection, clears inherited
   session state, and commits `LISTEN pgshard_catalog_changed` before its first
   read. An existing manual transaction fails closed.
2. It reads a complete catalog snapshot and its epoch in one read-only,
   repeatable-read transaction.
3. It validates range coverage, references, epochs, identities and the canonical checksum.
4. It swaps the immutable cache state atomically.
5. PostgreSQL `NOTIFY` sends only the committed positive decimal epoch.
6. A notification is a wake-up hint, never authoritative data; duplicate,
   stale, and malformed hints need not trigger a read, while a burst retains
   only its latest valid epoch.
7. The driver polls every bounded 1 to 300 seconds. Each subscription/initial
   load and refresh has a bounded 100-millisecond-to-five-minute deadline that
   covers SQL, validation, and cache publication. Deadline-first selection and
   a timed cache write lock reject publication after expiry. A deadline closes
   that session without replacing the last validated cache;
   a slightly later PostgreSQL 18 `transaction_timeout` interrupts server-side
   lock waits and guarantees rollback. Connection loss is also terminal to that
   driver instance.
8. A single-owner supervisor creates a fresh session with bounded, jittered
   exponential backoff and a 100-millisecond-to-30-second connection deadline.
   A new process is unready until its first validated load. An existing process
   may serve its last validated snapshot during a bounded 2-to-900-second grace
   that must be longer than its poll interval.
9. Readiness fails exactly when cache age reaches the grace deadline, whenever
   an epoch fence makes the current snapshot unusable, and after graceful
   shutdown. Reconnection does not restore readiness until a new authoritative
   load succeeds.

The default policy polls every 30 seconds, allows 90 seconds of cache age, uses
a five-second connection deadline and a 30-second catalog-operation deadline,
and grows reconnect-window ceilings from 100 milliseconds to five seconds.
Each process waits within the upper half of its current window so replicas do
not reconnect in lockstep. The status handle reports connection phase, catalog
epoch, monotonic cache age, attempts, connections completing their initial
authoritative load, and credential-safe failure categories including separate
connection and operation timeouts. The pooler control executable publishes
that catalog usability independently in exact JSON status and bounded-label
Prometheus metrics. Its overall readiness remains false with reason
`data_plane_unavailable`, even when the catalog is ready. It opens one regular
DSN file nonblockingly, performs a bounded read, and accepts only loopback IP
literals or Unix sockets with `sslmode=disable`, the exact `shardschema`
database, `target_session_attrs=read-write`, and no startup options. That
development bridge is not a substitute for authenticated TLS or operator
credential distribution.

`bootstrap-unavailable` is a separate fail-closed installation state, not an
empty catalog or a stale-cache policy. It accepts no DSN, reports phase
`not_configured` and readiness reason `catalog_not_configured`, keeps all
connection counters at zero, and requires a process rollout to enter supervised
catalog mode.

The empty installed catalog begins at epoch zero. A reader fails closed before
publishing metadata above the current process limits: 1,024 logical databases,
4,096 ranges or 16,384 registered tables per database, and 65,536 ranges or
tables across one snapshot. Queries fetch only the limit plus one row so a
runaway catalog cannot force an unbounded materialization before rejection.

A request retains the exact immutable snapshot with which it was planned. The
cache retains installed snapshots across newer publications and removes old
ones only when an explicit monotonic fence retires them. Components reject a
request if its epoch is unknown, future, or fenced before execution or
activation.

The migration is transactional and idempotent, requires PostgreSQL 18 or newer,
and must run in a pre-created UTF8 database named exactly `shardschema`. It
creates NOLOGIN reader/admin group roles, revokes public access and exposes
activation through a dual compare-and-swap over the global catalog epoch and
the prior active routing epoch. Activated routes and identity history are
immutable. Every staged range mutation also versions its parent routing epoch,
so an activation using an older `REPEATABLE READ` snapshot fails with a
serialization error rather than publishing stale or incomplete coverage.

## Stable catalog host

`shard-0000` remains the catalog host when data ranges are resharded. The identifier denotes the control-plane placement, not permanent ownership of a particular application key range. Moving `shardschema` is outside Milestone 1.
