# Major-version upgrades

pgshard upgrades PostgreSQL majors (18 → 19) with a blue/green group
replacement: the reshard machinery (see [resharding](resharding.md)) with a
1:1 range map, where the target groups run the new major. Writes pause only
for the reshard cutover fence; nothing is upgraded in place, and the old
groups stay current over reverse replication until retirement, so the flip
can be undone.

## Preconditions

Both sides gate the upgrade before anything physical happens, and a failure
names every check that failed rather than the first.

Operator-side (`UpgradeBlockers`), checked before the pending set is
materialized:

- `spec.postgresql.image`, when set, must name the target major — an image
  built for the old major cannot serve the new one.
- Backups must be healthy: a fresh full backup has to be possible before and
  after, and the stanza changes with the major.
- No shard set may already be pending — a reshard in flight must finish
  first.
- No table placement workflow may be running.

Controller-side (`upgradePreconditions`), checked before the copy starts:

- Every extension present on the source must exist on the target major, at a
  version the target can carry (see below).
- No large objects in any database: logical replication does not carry
  `pg_largeobject`, so those need the offline strategy.

This list is the specification. Earlier comments in the code cited a plan
section that is not tracked in the repository; if a check is added or
removed, change it here.

## Online strategy (default)

Setting `spec.postgresql.major` from 18 to 19 (with an image built for 19,
or the default image) triggers the upgrade once the operator preconditions
pass. It materializes a pending shard set with exactly the serving ranges,
stamped `pg_major = 19` (`pgshard.shard_sets.pg_major`), and the controller
opens a `kind = upgrade` workflow — the reshard workflow in everything but
name:

1. **Provisioning** — the operator brings up one non-serving group per
   shard on the new major's image. Groups of the old set keep their own
   major's image (`ImageFor`): the spec change never restarts them.
2. **Copy** — schema materialization with `pg_dump | psql` from the old
   primaries (forward-compatible 18 → 19), `FOR ALL TABLES`-equivalent
   publications, native logical subscriptions (`copy_data`,
   `streaming = parallel`), catch-up under the same throttles as a reshard.
3. **Cutover** — identical to the reshard switch: range fence, in-doubt
   2PC drain, table sweep, LSN catch-up, VDiff-lite verify, **sequence
   handoff** (`max(last_value)` across the sources, `setval` with
   `is_called` on every target — logical replication does not carry
   sequence positions), reverse publications and subscriptions for
   rollback, journal rows, the serving-map flip, replication swap and
   fence release. DDL is frozen for the window through `workflow_locks`.
4. **Retirement** — the old groups stay for
   `spec.resharding.retireOldGroupsAfter` with reverse replication
   flowing, then the run completes and they are deleted.

`spec.upgrade.maxParallelGroups` bounds provisioning parallelism: at most
that many new-major target groups are brought up at a time, the next one
starting as earlier ones become ready. The cutover itself is not staged:
the implementation replaces the whole shard set through one workflow (one
fence, one flip), the same shape as a reshard, so the flip is
all-groups-at-once regardless of the setting. Bounding provisioning keeps
the pod and I/O surge of a wide cluster in check; a per-group rolling flip
is not available.

### Preconditions

Checked automatically; the run does not start (operator) or fails with a
clear message (controller) otherwise:

- the target image names the new major (or the default image is used);
- backups are healthy when a backup policy is bound;
- no reshard or table placement workflow is in flight;
- every extension installed in **every database on every source shard**
  appears in `pg_available_extensions` on **every** target shard. Extensions
  are per-database objects and availability is per installation, so one
  shard's default database says nothing about the rest — and an extension
  available on only some target shards would install on those and fail on
  the others, which is the same outcome as missing, found after the target
  groups are provisioned;
- **the target's default version can carry the source's.** `pg_dump` emits
  `CREATE EXTENSION` with no version, so the restored schema gets the
  *target's* default whatever the source had. That is accepted when the two
  are equal, or when PostgreSQL declares an update path from the source's
  version to the target's default (`pg_extension_update_paths`). Nothing here
  compares the two version strings: `extversion` is opaque and PostgreSQL does
  not order it — `1.11` sorts before `1.9` as text and after it as anybody
  reading it means — which is precisely why update paths exist. Measured on
  this project's own images, five of the 46 extensions PostgreSQL 18.6 offers
  have a different default on 19beta3 (`btree_gin`, `btree_gist`,
  `pg_buffercache`, `pg_stat_statements`, `postgres_fdw`), and PostgreSQL
  declares a path for all five, so the ordinary upgrade passes;
- target shards must **agree** on an extension's default version. One that
  differs between them would restore a different schema per shard, so it is
  refused under its own message rather than reported as missing;
- no large objects (`pg_largeobject_metadata` must be empty): logical
  replication does not carry `pg_largeobject` — use the offline strategy.

A serving set without a stamped `pg_major` (a catalog from before this
feature) never triggers; stamp it once with
`UPDATE pgshard.shard_sets SET pg_major = 18 WHERE state = 'serving'`.

### Rollback

Before retirement completes, annotate the run's PgShardReshard with
`pgshard.io/upgrade=rollback` (or merge `{"rollback": true}` into the
workflow spec). The controller fences, waits until every reverse
subscription passed the targets' LSNs, carries the sequences back, flips
the serving map to the old set and drops the run's replication objects.
The workflow ends `cancelled` / `rolled_back`; the new-major groups are
retired for the operator to delete. After retirement the old groups are
gone and rollback is a restore, not a flip.

### Catalog group

The catalog group is not part of a shard set, so the reshard machinery
does not cover it; it goes **last** in the upgrade's group iteration. Once
every shard set runs the new major (`status.servingPGMajor` equals
`spec.postgresql.major`, no reshard in flight), the operator drives its
own blue/green replacement, tracked in `status.catalogUpgrade`:

1. **provisioning** — a new-major catalog group (`catalog-g<n>`) comes up
   next to the old one and gets the catalog schema migrations.
2. **copying / catching_up** — the `pgshard` catalog is copied over native
   logical replication (`FOR TABLES IN SCHEMA pgshard` publication,
   subscription with the initial copy) until the subscription drained the
   source's WAL position.
3. **cutover** — the old primary is fenced (`default_transaction_read_only`
   plus backend termination), the subscription drains to the fence LSN,
   sequence positions are carried over with `setval`, the subscription is
   dropped and the **stable catalog Service** (`<cluster>-catalog-rw`) is
   repointed at the new group's primary. Routers and the controller dial
   that Service name, so the flip re-points them without a redeploy; a
   severed connection reconnects to the new primary. Writes that land in
   the fence window fail read-only and are retried by their callers.
4. **retiring** — the old group stays (fenced) for
   `spec.resharding.retireOldGroupsAfter`, then it is deleted. Annotating
   the cluster with `pgshard.io/catalog-upgrade=rollback` inside that
   window repoints the Service back, lifts the fence and deletes the
   new-major group. A reverse stream keeps the old catalog current in the
   meantime: the new group publishes `pgshard_catalog_rollback` for the
   `pgshard` schema and the old one subscribes, so a rollback replays what
   the new catalog accepted before it serves again, and refuses rather than
   serve a catalog that is missing writes. Roll back only from a quiet
   cluster all the same — the trigger conditions (no reshard or placement
   in flight) enforce it at the start.

   Catalog **schema** migrations wait for this window to close. Logical
   replication carries no DDL, so a migration applied after the flip would
   send its `pgshard.schema_migrations` row back to the old catalog while
   the DDL it describes stayed behind, and rows in a table it created would
   not replicate at all until the subscription refreshed — the old catalog
   would be structurally behind and believe otherwise. While the previous
   catalog is kept, `CatalogReady` says `MigrationDeferred`, and the
   migrations run once it is retired. An operator upgraded *inside* this
   window therefore runs against the schema it found until the window
   closes: shorten `retireOldGroupsAfter` or finish the catalog upgrade
   before rolling the operator.

A serving catalog whose major was never probed is stamped on the first
reconcile (`status.catalogPGMajor`).

## Offline strategy

`spec.upgrade.strategy: offline` runs `pg_upgrade --link` on the primary
PVC per group instead: `pgshard-agent upgrade` is the Job entrypoint
(`UpgradeJob` renders the manifest), the group's pods scaled down first and
the replicas re-cloned afterwards. It requires an image carrying **both**
majors' binaries under `/usr/lib/postgresql/<major>/bin`; the per-major
`pgshard-postgres` image carries one, so the agent refuses with a message
naming the requirement. Until a combined image exists the offline strategy
is scaffolding only — the online strategy is the supported path.

## Mixed majors and the router

While old- and new-major groups both serve, the router must parse with the
grammar of the **lowest still-present major** so no statement is accepted
that an old-major group would refuse. `pgparser.EffectiveMajor` states that
rule, and nothing calls it yet: the router binds one grammar at build time
and cannot swap it per statement, so this is the intended behaviour rather
than today's.

The per-set major is `pgshard.shard_sets.pg_major`, and per-run majors
appear on `PgShardCluster.status` (`servingPGMajor`, `reshard.pgMajor`,
`reshard.retiredPGMajor`). The bound grammar today is
PostgreSQL 18 (`internal/pgparser/pg18`); PG19-only syntax is refused
until a libpg_query 19 binding lands, at which point the effective major
flips once every group runs 19.

## Continuous integration

`.github/workflows/e2e-kind.yml` carries an `upgrade` suite
(`test/e2e/upgrade`): it builds **both** the pg18 and pg19 images, loads
them into kind and runs `TestUpgrade18To19UnderLoad` — a one-shard cluster
upgraded 18 → 19 under a ledger workload, asserting no acknowledged write
is lost or duplicated, the cutover pause is recorded, rollback before
retirement returns serving to the old groups, the re-run completes, and
the catalog group follows onto 19 behind the stable Service — plus
`TestUpgrade18To19ChaosControllerAndPrimaryKill`, which kills the
controller mid-copy and the promoted primary after the switch and asserts
convergence. The `reshard-split` and `reshard-merge` suites each run one
reshard under the same ledger oracle — growing a cluster and shrinking one
— in separate jobs, so each transition gets a budget it can finish in.
All run on small clusters to stay inside the runner budget.

## After the upgrade

- pgBackRest: the new-major groups archive into their **own stanza** (the
  stanza is derived per group); take a fresh full backup immediately.
- **PITR does not cross majors**: restore points and WAL from the 18 era
  restore onto 18 only. Keep the old repository until its retention ends.
