# Catalog schema

The pgshard control plane lives in one ordinary PostgreSQL database. Every
object sits in the `pgshard` schema and is created by the embedded migrations
in `internal/catalog/schema`, applied by `catalog.Migrate`. Operators change
the cluster by editing the desired-state tables with plain SQL; components
publish what they observe into the status tables.

## Roles

| Role | Purpose |
|------|---------|
| `pgshard_system` | Owns every catalog object. Used by pgshard components. Writes status tables. |
| `pgshard_admin` | Reads and writes desired-state tables; reads status tables. |
| `pgshard_reader` | Reads every table. |

Roles are `NOLOGIN` and created only if missing. Grant them to login roles.

## Migrations

`pgshard.schema_migrations(version, applied_at, checksum)` records each
applied migration with the SHA-256 of its file. `Migrate` runs pending
migrations in version order, each in its own transaction, and refuses to
continue if the embedded text of an already applied migration no longer
matches its recorded checksum.

`pgshard.hash_versions(version, description)` lists the hash functions a
sharded table may use. Version 1 is `postgresql-extended-hash-seed-8816678312871386365`.

## Desired-state tables

Every desired-state table has `desired_generation` and `updated_at`. A
`BEFORE INSERT OR UPDATE` trigger stamps `desired_generation` from
`pgshard.desired_generation_seq` and refreshes `updated_at`. After each
statement a trigger sends `NOTIFY pgshard_desired` with payload
`<table>:<generation>` (deletes take a fresh generation).

### `pgshard.databases`

| Column | Meaning |
|--------|---------|
| `name` | Logical database name (primary key). |
| `default_placement` | `unsharded`, `sharded` or `reference`; placement of tables not listed in `pgshard.tables`. |
| `home_shard` | Shard that holds unsharded tables. |
| `created_at`, `updated_at` | Timestamps. |

### `pgshard.tables`

| Column | Meaning |
|--------|---------|
| `database`, `schema_name`, `table_name` | Primary key; `database` references `pgshard.databases`. |
| `placement` | `sharded`, `reference` or `unsharded`. |
| `shard_key` | Column the hash is computed over; required when `placement = 'sharded'`. |
| `hash_version` | References `pgshard.hash_versions`. |
| `sequence_columns` | Columns of a sharded table the router fills from `pgshard.sequences` on `INSERT` (migration `0005`); `NULL` for none. See *Sequences* in `docs/router.md`. |

### `pgshard.shard_ranges`

| Column | Meaning |
|--------|---------|
| `shard_set` | Name of the shard map; `default` unless a database uses another. |
| `shard_id` | Shard within the set. Primary key with `shard_set`. |
| `range` | `int8range` of hash values owned by the shard, lower bound inclusive, upper bound exclusive; unbounded ends allowed. |

An exclusion constraint rejects overlapping ranges within a shard set. A
deferred constraint trigger checks at commit that the ranges of a shard set
are contiguous and cover the whole `int8` key space, so splits and merges can
be written as several statements in one transaction.

### `pgshard.roles`

| Column | Meaning |
|--------|---------|
| `rolname` | Role name (primary key). |
| `verifier` | SCRAM verifier string. |
| `login`, `createdb`, `createrole`, `inherit`, `connection_limit`, `valid_until` | Role attributes (`docs/roles.md`). |
| `attributes` | Reserved JSON for further attributes. |

### `pgshard.role_members`

`(rolname, member, admin_option)` — role memberships; both sides reference
`pgshard.roles`.

### `pgshard.grants`

| Column | Meaning |
|--------|---------|
| `id` | Identity primary key. |
| `rolname` | Grantee; references `pgshard.roles`. |
| `database` | References `pgshard.databases`. |
| `object_kind`, `object_schema`, `object_name`, `column_name` | What the privileges apply to (unique with `rolname`, `database`). |
| `privileges` | Privilege names, `ALL` expanded per kind. |
| `grant_option` | `WITH GRANT OPTION`. |

### `pgshard.role_settings`

`(rolname, database, name, value)` — `ALTER ROLE … [IN DATABASE] SET`.

## Status tables

Status tables are written by `pgshard_system`; `pgshard_admin` and
`pgshard_reader` may only `SELECT`.

| Table | Columns |
|-------|---------|
| `database_status` | `database`, `state`, `effective_generation`, `updated_at` |
| `table_status` | `database`, `schema_name`, `table_name`, `effective_placement`, `effective_shard_key`, `effective_generation`, `workflow_id`, `progress` (jsonb), `updated_at` |
| `shard_status` | `shard_set`, `shard_id`, `group_name`, `serving_state`, `primary_epoch`, `primary_endpoint`, `replay_lag_bytes`, `updated_at` |
| `role_status` | per `(rolname, group_name)`: `state` (`in_sync`, `drifted`, `missing`, `unmanaged`, `unmanaged_superuser`), `details` (jsonb), `roles_generation`, `checked_at` (`docs/roles.md`) |
| `role_group_status` | `group_name`, `roles_generation` the group was last materialized at, `materialized_at` |
| `workflows` | `id` (uuid), `kind`, `state`, `spec`, `status`, `journal_ids`, `created_at`, `updated_at`, `error` |
| `migrations` | DDL/DCL migrations (`docs/ddl.md`): `id` (uuid), `database`, `statement`, `kind`, `strategy` (`direct`, `concurrent`), `scope` (`all`, `home`, `existing`), `home_shard`, `state` (`queued`, `running`, `complete`, `failed`), `meta` (jsonb: run_as, object, role/verifier, roles delta, database), `per_shard` (jsonb per shard: state, attempts, error, sqlstate), `error`, `created_at`, `updated_at`, `finished_at` |
| `xact_decisions` | Two-phase commit outcomes: `gid`, `state` (`preparing`, `commit`, `abort`), `participants`, `participant_xids` (since `0006`: the participants' `pg_current_xact_id()` in the same order, so a restore can tell a committed transaction from one that never happened), `created_at`, `decided_at` |
| `streams` | `name`, `spec`, `position`, `state` |
| `sequences` | Global sequences: `name`, `next_value`, `block_size`. Since migration `0005` `pgshard_admin` may also insert, update and delete rows (declaring a sequence or changing its block size), and routers allocate through `pgshard.allocate_sequence_block(name, n)`, a `SECURITY DEFINER` function (executable by `pgshard_admin`) that creates the row on first use, moves `next_value` past a block of `n` values (`block_size` when `n` is `NULL`) in one `UPDATE … RETURNING` and returns `(block_start, block_end)`. |
| `restore_points` | `id` (uuid), `name` (unique), `shard_map_generation`, `per_group` (jsonb: `{group: {lsn, timeline, wal_segment}}`), `certified` (since `0006`: writes were paused and two-phase commits drained when the points were taken), `created_at` |
| `serving` | `shard_set`, `generation`, `published_at` |
| `shard_map_generation` | Single row: `generation`, `updated_at`, and since `0006` the cluster write fence: `write_fence`, `write_fence_reason`, `write_fenced_at`. Routers hold new writes while `write_fence` is set (see [router.md](router.md#write-fence)); the barrier workflow raises it and a restored catalog carries it until the restore is reconciled. |

### Serving notifications

Migration `0004_status_notify` adds a statement-level trigger on
`shard_status`, `table_status` and `shard_map_generation` that sends
`NOTIFY pgshard_serving` with the table name as payload, so routers learn of
effective-map changes without polling.

## Router snapshots

`internal/catalog/snapshot` gives the router an immutable view of the catalog.

- `snapshot.Load` reads one `Snapshot` in a single `REPEATABLE READ` read-only
  transaction: shard-map and desired generations, every shard set's ranges
  (inclusive `int64` bounds, unbounded ends mapped to `MinInt64`/`MaxInt64`),
  the serving primary of every shard from `shard_status`, the databases and
  the effective table placement. Placement comes from `table_status`; a table
  with no status row is only visible when its desired placement is
  `unsharded`.
- `snapshot.LoadRoles` reads SCRAM verifiers into a separate `Roles` value
  whose `String`/`GoString` print only a count. Verifiers never enter a
  `Snapshot`.
- `Snapshot.Locate(shardSet, keyspaceID)` binary-searches the ranges.
- `snapshot.CheckGeneration(routed, observed)` returns `*StaleGeneration`
  when a pooler reports a different shard-map generation than the snapshot
  the request was routed with.
- `snapshot.Watcher` LISTENs on `pgshard_desired` and `pgshard_serving`,
  reloads on every notification, every 30 s (configurable) and after each
  reconnect; `Current()` returns the latest snapshot and `Subscribe()` a
  channel of generation changes.
- `snapshot.ConsistencyWatcher.Observe` reports `Consistent`/`Inconsistent`
  transitions per shard set; a shard blocks consistency while its
  `serving_state` is `migrating` or `fenced` or it has no status row.

`pgshard-router catalog-watch DSN` follows a catalog and prints each
generation change and consistency transition.

## Go API

`internal/catalog` exposes `Migrate`, `Migrations`, the constants
`Schema`, `DesiredChannel`, `RoleSystem`, `RoleAdmin`, `RoleReader`, and
`ServingChannel`, typed readers `ListDatabases`, `ListTables`,
`ListShardRanges`, `ListTableStatus`, `ListShardStatus`, their cluster-wide
variants `ListAllTables`, `ListAllShardRanges`, `ListAllTableStatus`,
`ListAllShardStatus`, and `Generations`.
