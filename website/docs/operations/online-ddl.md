---
title: Online DDL
description: Non-blocking schema migration and client-visible atomic activation.
---

# Online DDL

:::info Milestone 1 design contract
The DDL job, shadow-copy and activation runtime are not implemented in the
foundation release; see [implementation status](../project/status.md).
:::

Schema changes will be submitted as durable `PgShardDDL` jobs. The controller
will apply safe native operations directly and convert supported blocking
rewrites into online shadow-table migrations.

## Workflow

1. Parse and classify the statement before any shard changes.
2. Create a shadow table on every shard.
3. Take exported snapshots and bulk-copy primary-key chunks.
4. Apply concurrent writes through the shared PostgreSQL `pgoutput` decoder.
5. Validate schema, row counts, and chunk checksums.
6. Enter `ReadyForActivation`, automatically or awaiting an explicit command.
7. Gate the affected table and drain old-epoch transactions.
8. Swap old and shadow tables in a transaction on every shard.
9. Compensate completed swaps if another shard fails while traffic remains gated.
10. Publish one schema epoch and release traffic only after all shards agree.

This is client-visible atomic activation. It is not one physical PostgreSQL transaction spanning all catalogs.

## Milestone 1 boundaries

Tables need a primary key. Supported changes cover selected column type/default/nullability operations and indexes. Primary-key changes, foreign-key changes, partition-layout changes, unsupported extensions, and complex dependent-object graphs fail validation instead of running a blocking migration.
