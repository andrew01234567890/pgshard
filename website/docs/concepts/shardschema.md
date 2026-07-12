---
title: Shard metadata and shardschema
description: How pgshard stores and distributes its authoritative topology.
---

# Shard metadata and `shardschema`

`shardschema` is a dedicated PostgreSQL database on stable `shard-0000`. Physical streaming replication protects it with the rest of that shard. It is authoritative for logical topology; etcd is used only for ephemeral leases and fencing.

## Catalog contents

The internal `pgshard_catalog` schema records:

- Databases, registered tables, shard-key types and hash versions.
- Shard identities and non-overlapping half-open key ranges.
- Routing, schema, authorization, and catalog epochs.
- Durable DDL, reshard, backup, restore, role, grant, and change-stream operations.
- Backup-set manifests, CDC vector acknowledgements, and reshard journals.

Password material is never stored in `shardschema`.

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
