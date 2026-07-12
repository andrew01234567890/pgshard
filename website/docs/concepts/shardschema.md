---
title: Shard metadata and shardschema
description: How pgshard stores and distributes its authoritative topology.
---

# Shard metadata and `shardschema`

:::info Milestone 1 design contract
The catalog schema and cache listener are not implemented in the foundation
release; see [implementation status](../project/status.md).
:::

`shardschema` will be a dedicated PostgreSQL database on stable `shard-0000`.
Physical streaming replication will protect it with the rest of that shard. It
will be authoritative for logical topology; etcd is used only for ephemeral
leases and fencing.

## Catalog contents

The internal `pgshard_catalog` schema records:

- Databases, registered tables, shard-key types and hash versions.
- Shard identities and non-overlapping half-open key ranges.
- Routing, schema, authorization, and catalog epochs.
- Durable DDL, reshard, backup, restore, role, grant, and change-stream operations.
- Backup-set manifests, CDC vector acknowledgements, and reshard journals.

Password material is never stored in `shardschema`.

Each cluster has one routing-hash record containing algorithm version `1` and a
creation-time seed. Both columns are insert-only. Changing either requires an
explicit online reshard into a new hash space; ordinary catalog updates cannot
rewrite them. Rust golden vectors cover every supported key type, numeric and
empty boundaries, XXH3 length boundaries, and seed extremes.

Epochs, WAL positions and 64-bit range bounds use decimal strings in JSON-facing
interfaces because JavaScript numbers cannot represent them exactly. The Rust
core therefore does not derive generic Serde encodings for `u64`/`u128` catalog
types. Protobuf's standard JSON mapping likewise encodes 64-bit integers as
strings; the exclusive keyspace end `2^64` is represented by an absent optional
range end.

## Cache protocol

1. A component transactionally reads a complete catalog snapshot and its epoch.
2. It validates range coverage, references, and the snapshot checksum.
3. It swaps the cache atomically.
4. PostgreSQL `NOTIFY` wakes listeners after later commits.
5. A notification never carries authoritative data; listeners re-read the newer snapshot.
6. Polling and reconnect handle lost notifications.

A request retains the epoch with which it was planned. Components reject a request if that epoch is fenced before execution or activation.

## Stable catalog host

`shard-0000` remains the catalog host when data ranges are resharded. The identifier denotes the control-plane placement, not permanent ownership of a particular application key range. Moving `shardschema` is outside Milestone 1.
