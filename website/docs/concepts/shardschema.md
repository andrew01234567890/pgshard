---
title: Shard metadata and shardschema
description: How pgshard stores and distributes its authoritative topology.
---

# Shard metadata and `shardschema`

:::info Current implementation boundary
The PostgreSQL 18 migration, validated Rust snapshot model, canonical checksum,
multi-epoch lock-free cache, and live database contract test exist in source.
The pooler snapshot loader and LISTEN/reconnect task are not wired yet; see
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
extensions; the current schema does not claim to store them.

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

1. A listener commits `LISTEN pgshard_catalog_changed` before its first read.
2. It transactionally reads a complete catalog snapshot and its epoch.
3. It validates range coverage, references, epochs, identities and the canonical checksum.
4. It swaps the immutable cache state atomically.
5. PostgreSQL `NOTIFY` sends only the committed positive decimal epoch.
6. A notification is a wake-up hint, never authoritative data; duplicate and stale hints are ignored.
7. Polling and reconnect recover lost notifications by rereading a complete snapshot.

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
immutable.

## Stable catalog host

`shard-0000` remains the catalog host when data ranges are resharded. The identifier denotes the control-plane placement, not permanent ownership of a particular application key range. Moving `shardschema` is outside Milestone 1.
