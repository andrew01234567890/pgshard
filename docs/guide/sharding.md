# Defining sharding

The shard layout is not configuration: it is rows in the `pgshard` catalog
database, edited with ordinary SQL. Connect to the router with
`dbname=pgshard` (or directly to the catalog primary) as a role granted
`pgshard_admin`, and every component follows your edits through
LISTEN/NOTIFY. The full schema is in [catalog.md](../catalog.md).

On a cluster nobody has logged into yet, that role is the superuser whose
password the operator generated; see
[the first login](getting-started.md#the-first-login) for how to read it
out of the Secret.

## What one cluster is

A `PgShardCluster` is the unit of isolation. Everything inside one shares a
single shard set and a single serving generation, and the workflows that
change either act on all of it at once:

- **Reshard and major upgrade.** A cutover flips the serving set for every
  database in the cluster, not per database. Unrelated databases move
  together.
- **The write fence.** A certified barrier and a cutover pause writes
  cluster-wide for their window ([resharding.md](resharding.md),
  [backup-restore.md](backup-restore.md)).
- **Failover.** A group's promotion is scoped to that group, and which
  databases notice depends on where their data is: a sharded or reference
  table has rows on every group, so any database with one is affected by any
  group; a database whose tables are all unsharded is affected only by the
  failover of its home shard.

Databases are a naming and permission boundary, not a blast-radius boundary.
`CREATE DATABASE` fans out to every shard group rather than provisioning
anything of its own, which is what makes adding a database cheap and what
makes it share everything above.

**To isolate two workloads from each other, run two clusters.** That is the
supported answer, and it is the only one: there is no per-database shard set
and no per-database serving generation to keep them apart inside one
cluster.

## Declare a database

```sql
INSERT INTO pgshard.databases (name, default_placement, home_shard)
VALUES ('app', 'unsharded', 0);
```

- `default_placement` is the placement of tables *not* listed in
  `pgshard.tables`: `unsharded` (they live on the home shard), `sharded`
  (undeclared tables are refused until declared) or `reference`.
- `home_shard` is where unsharded tables live.

Create the database and roles through the router with plain DDL — it fans
out to every shard and records the result ([queries.md](queries.md)):

```sql
CREATE DATABASE app;
CREATE ROLE app_rw LOGIN PASSWORD 'change-me';
GRANT CONNECT ON DATABASE app TO app_rw;
```

## Declare sharded and reference tables

```sql
INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key, hash_version)
VALUES ('app', 'public', 'orders', 'sharded', 'customer_id', 1);

INSERT INTO pgshard.tables (database, schema_name, table_name, placement)
VALUES ('app', 'public', 'currencies', 'reference');
```

- `sharded` tables place each row by hashing `shard_key` with the
  PostgreSQL extended hash (hash version 1,
  [placement.md](../placement.md)) and looking the value up in the shard
  set's ranges. Supported key types hash bit-identically to PostgreSQL's own
  hash partitioning: `int2/int4/int8`, `text` (deterministic collation),
  `uuid`, `bytea`. Numeric and floating-point keys are refused.
- `reference` tables exist on every shard; the router replicates writes to
  all of them ([queries.md](queries.md#reference-tables)).
- Then create the table through the router; a sharded table must define its
  shard key column, and every PRIMARY KEY or UNIQUE constraint must include
  it:

```sql
CREATE TABLE orders (
  customer_id bigint NOT NULL,
  id          bigint NOT NULL,
  body        text,
  PRIMARY KEY (customer_id, id)
);
```

A table with no effective placement yet becomes effective immediately (no
data to move). Changing the placement or shard key of a table that already
has one requires data movement and creates a `table_rekey` workflow instead
of taking effect — see [resharding.md](resharding.md) for the current
status of that workflow.

## Shard ranges

`pgshard.shard_ranges` maps hash values to shards, one `int8range` per
shard, contiguous and covering the whole signed 64-bit space. The operator
populates the `default` set from `spec.shards`; inspect it with:

```sql
SELECT shard_set, shard_id, range FROM pgshard.shard_ranges ORDER BY 1, 2;
```

Splits and merges can be written as several statements in one transaction —
a deferred trigger checks contiguity and coverage at commit. A change to
the ranges of a live shard set creates a `reshard` workflow rather than
taking effect directly ([resharding.md](resharding.md)).

## Global sequences

A `serial` or identity column on a sharded table is per-shard and not
unique across shards. Declare the columns the router should fill from the
catalog-backed global sequence instead:

```sql
UPDATE pgshard.tables SET sequence_columns = '{id}'
 WHERE database = 'app' AND schema_name = 'public' AND table_name = 'orders';
```

Routers allocate disjoint blocks through
`pgshard.allocate_sequence_block(name, n)` and fill the column on `INSERT`
(also answering `SELECT nextval('orders.id')` locally). Block size is
editable per sequence:

```sql
UPDATE pgshard.sequences SET block_size = 100
 WHERE name = 'app.public.orders.id';
```

Values are gap-free within a block but interleave across routers, exactly
like block-cached PostgreSQL sequences. Details in
[router.md](../router.md#sequences).

## Roles and grants

Roles are cluster-wide: `CREATE ROLE`, `GRANT` and `ALTER ROLE` through the
router apply to every shard and to the catalog, and the controller's role
verifier repairs drift ([roles.md](../roles.md)). `SUPERUSER`,
`REPLICATION` and `BYPASSRLS` roles cannot be managed through the router.

## Observing the layout

```sql
SELECT * FROM pgshard.table_status;   -- effective placement per table
SELECT * FROM pgshard.shard_status;   -- serving state, primary endpoint, epoch
SELECT * FROM pgshard.shard_map_generation;
```

Desired-state edits bump `desired_generation`; the controller validates
them and publishes the effective state, which routers follow live.

## Editing safely when something else may be editing too

`desired_generation` is stamped by a `BEFORE` trigger on every insert and
update, from one cluster-wide sequence. Because the stamp happens on the
new row, the value a statement reads is the value of the row it is about to
replace -- so it can be used as an expected-version predicate:

```sql
-- read the row and its generation
SELECT placement, shard_key, desired_generation FROM pgshard.tables
 WHERE database = 'app' AND schema_name = 'public' AND table_name = 'orders';

-- write it back only if nothing else has changed it since
UPDATE pgshard.tables SET shard_key = 'customer_id'
 WHERE database = 'app' AND schema_name = 'public' AND table_name = 'orders'
   AND desired_generation = 41;    -- the value just read
```

If the update reports zero rows, someone else changed the row first: re-read
and decide again rather than retrying blindly. A shard-key change starts a
re-key workflow, so a lost update here is not a lost edit -- it is an
unintended workflow.

This is a convention, not an enforcement. Raw `UPDATE` without the predicate
still succeeds, and `pgshard_admin` holds direct DML on every desired-state
table. **Automation should always carry the predicate**; two processes
working from stale observations can otherwise silently overwrite each
other's topology. Guarded mutation procedures that require the expected
generation and refuse on mismatch are tracked separately.
