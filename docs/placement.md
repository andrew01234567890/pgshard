# Placement and the controller

Sharded tables place each row by hashing its shard key into the signed 64-bit
key space and looking the result up in the shard set's ranges. Both halves
are deliberately identical to what PostgreSQL itself does for hash
partitioning, so a shard key hashes to the same position inside PostgreSQL
(`hashint8extended`, `hashtextextended`, ...) and in the router.

## Hash (`internal/placement`)

`placement.KeyspaceID(v)` returns the key space position of a shard key.
It is a bit-exact port of PostgreSQL's extended hash functions:

| Go value | PostgreSQL function |
|----------|---------------------|
| `int64`, `int` | `hashint8extended` (also `timestamp_hash_extended` on the raw microsecond count) |
| `int32` | `hashint4extended` |
| `int16` | `hashint2extended` |
| `int8` | `hashcharextended` |
| `string` | `hashtextextended` under a deterministic collation |
| `[16]byte` | `uuid_hash_extended` |
| `placement.RawBytes` | `hash_any_extended` over the given bytes (`bytea`) |

Every function uses the fixed seed `placement.PartitionSeed`
(8816678312871386365, PostgreSQL's `HASH_PARTITION_SEED`), which is hash
version 1 in `pgshard.hash_versions`. `int8` values are folded to 32 bits
exactly like PostgreSQL does (`lohalf ^= hihalf` for non-negative values,
`lohalf ^= ~hihalf` otherwise), so an `int8`, `int4` and `int2` shard key with
the same value land on the same shard. Numeric and floating-point shard keys
are refused: their PostgreSQL hash depends on the internal representation and
is not ported yet.

Verified goldens on PostgreSQL 18:

```
hashint8extended(42, 8816678312871386365) = 7363975540656877951
hashint8extended(42, 0)                   = 8010225493015854792
hashint8extended(-1, 0)                   = -1888257769727981238
```

`go test ./internal/placement` also runs a differential test: 10 000 random
values per type and three seeds are hashed by a live PostgreSQL 18 and 19
(Docker) and compared with the Go result.

## Key ranges

`placement.Range{Start, End}` is an inclusive interval; a `placement.RangeSet`
must be sorted, contiguous and cover `MinInt64..MaxInt64` (`Validate`).
`Locate` finds the range for a key by binary search, `Split(n)` divides the
key space into `n` ranges of equal size (to within one key), and `Merge(i, j)`
joins adjacent ranges. In the catalog the same ranges are stored as
`int8range` (lower inclusive, upper exclusive, unbounded ends) in
`pgshard.shard_ranges`, whose triggers reject gaps and overlaps.

## Uniqueness on a sharded table

Every global uniqueness key must contain the shard key, and must compare it
the way pgshard distributes it: a deterministic collation and the default
operator class, so index equality and hash equality agree.

An exclusion constraint — including a PostgreSQL 18 temporal `PRIMARY KEY`
or `UNIQUE ... WITHOUT OVERLAPS`, whose index is an exclusion index — needs
one condition more. Its elements are compared with operators of its own,
and it is enforceable one shard at a time only when the shard key's element
is compared with **btree equality**: rows with different keys are then never
in conflict, wherever they live. An element compared with `&&` or another
non-equality operator can conflict across shards, and no shard can see the
other's rows, so sharding by that element is refused.

A table whose *primary key* is temporal is refused for a different reason:
placement applies rows by the primary key and PostgreSQL cannot match an
exclusion constraint from an `ON CONFLICT` column list. Declare the temporal
key as a `UNIQUE` constraint alongside an ordinary primary key.

## Controller (`internal/controller`, `pgshard-controller run`)

The controller turns desired state into status and workflows. Exactly one
instance is active: each `pgshard-controller run` process takes
`pg_try_advisory_lock` on `controller.LeaderLockKey` in the catalog database;
the holder is the leader, the others retry every `--election-retry`. Leadership
needs no Kubernetes API. The leader `LISTEN`s on `pgshard_desired` and
`pgshard_serving` and runs a pass after every notification and at least every
`--reconcile-interval`. Losing the catalog connection loses the lock and the
process campaigns again.

One pass runs in a single `REPEATABLE READ` transaction:

* **Tables.** A `pgshard.tables` row is validated (`sharded` needs a
  `shard_key` and a known `hash_version`; other placements must not have one).
  A table with no effective placement yet becomes effective immediately: its
  `pgshard.table_status` row is written with the desired placement, shard key
  and generation, because a table that was never materialised needs no data
  movement. A change of placement or shard key on a table that already has an
  effective placement needs movement: the effective values stay as they are and
  one `table_placement` workflow is created in state `pending`; `table_status.workflow_id`
  points at it and no second workflow is created while it is pending, running
  or paused, nor after it failed for the same desired generation. Reverting
  the row cancels the workflow before its swap. See
  [resharding.md](resharding.md#table-placement-workflows) for the run.
* **Shard sets.** The ranges of each shard set are validated as a
  `RangeSet`. A shard set with no `pgshard.shard_status` rows is populated:
  every shard gets a status row in state `provisioning`, `pgshard.serving`
  records the desired generation that became effective, and
  `pgshard.shard_map_generation` is bumped. When another component publishes a
  `primary_endpoint` for a `provisioning` shard the controller flips it to
  `serving` and bumps the generation again. A change to the ranges of a shard
  set that already has status rows needs movement: one `reshard` workflow is
  created in state `pending` carrying the desired ranges; nothing else changes.

Workflows are exposed through the `pgshard.v1.Controller` gRPC service:
`ListWorkflows` (filter by kind and state), `GetWorkflow`, `PauseWorkflow`
(pending or running to paused; the previous state is kept in
`status.paused_from`) and `ResumeWorkflow` (back to that state).
`ResolveTransactions` and `ApplyDDL` are not implemented yet.

```
pgshard-controller run --catalog-dsn postgres://... \
    --listen 127.0.0.1:15500 --tls-cert ... --tls-key ... --tls-ca ...
```

`--insecure-dev` serves plaintext gRPC for development; `--listen ""` runs
the reconciler without a gRPC listener.
