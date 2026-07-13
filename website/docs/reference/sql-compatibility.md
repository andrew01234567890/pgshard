---
title: SQL compatibility
description: Supported and rejected PostgreSQL behavior in Milestone 1.
---

# SQL compatibility

:::warning Planned compatibility, not current support
No pooler endpoint, SQL parser, or statement planner exists yet. The source has
only a fail-closed core that routes an already-resolved, non-NULL shard-key bind
parameter against one immutable catalog snapshot. The table below is the
Milestone 1 acceptance contract; see [implementation status](../project/status.md).
:::

The planned pooler will speak PostgreSQL simple and extended query protocols and
support parameter-aware routing. Compatibility depends on whether a statement
can be proven to target one shard.

| Behavior | Milestone 1 target |
|---|---|
| Single-shard PostgreSQL statement | Supported, subject to managed DDL/session limits |
| Multi-row insert across shards | Supported through 2PC |
| Multi-shard update/delete with routable predicates | Supported through 2PC |
| Scatter `SELECT` whose results can be concatenated exactly | Supported |
| Cross-shard join or aggregate | Rejected |
| Global `ORDER BY`, `LIMIT`, `DISTINCT`, window, set operation | Rejected |
| Cross-shard foreign key | Rejected |
| Unique constraint without shard key | Rejected |
| Shard-key update | Rejected |
| `COPY` on a sharded table | Rejected |
| Managed online DDL | Supported subset |
| Distributed `READ COMMITTED` | Supported |
| Distributed `REPEATABLE READ` or `SERIALIZABLE` | Rejected |

## Transaction pooling limits

Safe session settings are replayed when a transaction receives a backend. Temporary objects, `LISTEN`, session advisory locks, holdable cursors, and backend-bound state are rejected because they cannot move safely between pooled connections or enter PostgreSQL prepared transactions.

The pooler pins `client_encoding` to canonical `UTF8` and rejects attempts to
change it. PostgreSQL converts both text-format and binary `text` binds from the
session encoding before storage; routing raw bytes from any other encoding can
disagree with the stored value and is therefore not allowed. Both formats also
reject the zero byte exactly as PostgreSQL does.

Named prepared statements are virtualized at the pooler. Their routing plan is invalidated by relevant schema or routing epoch changes.

## Keys and constraints

A registered sharded table has one immutable shard-key column. Supported key
types are integer, UUID, text, and bytea. Text keys require database encoding
`UTF8` and the built-in deterministic, byte-distinguishing `C` collation on the
key column. Registration rejects ICU, nondeterministic, case-insensitive and
other collations, and a later collation change is managed DDL that fails before
activation. This ensures values equal under PostgreSQL equality cannot hash to
different shards. Primary and unique constraints include the shard key so
PostgreSQL can enforce them locally. Sequences are shard-local and do not promise
a gapless or globally ordered value stream.

## Roles and grants

Application users, role membership, grants, and default privileges are managed declaratively across all shards. Direct role/grant SQL through the application endpoint is rejected to prevent drift. Milestone 1 application roles cannot be superusers or receive replication, `BYPASSRLS`, `CREATEDB`, or `CREATEROLE` capabilities.
