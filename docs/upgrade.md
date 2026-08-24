# Major-version upgrades

pgshard upgrades PostgreSQL majors (18 → 19) with a blue/green group
replacement: the reshard machinery (see [resharding](resharding.md)) with a
1:1 range map, where the target groups run the new major. Writes pause only
for the reshard cutover fence; nothing is upgraded in place, and the old
groups stay current over reverse replication until retirement, so the flip
can be undone.

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
- every extension installed on the sources appears in
  `pg_available_extensions` on the target major;
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
   new-major group. The old catalog is frozen from the cutover on, so
   catalog changes made after the flip (workflow progress, serving-map
   bumps) are lost on rollback — roll back only from a quiet cluster,
   which the trigger conditions (no reshard or placement in flight)
   enforce at the start.

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
grammar of the **lowest still-present major**
(`pgparser.EffectiveMajor`), so no statement is accepted that an old-major
group would refuse; the per-set major is `pgshard.shard_sets.pg_major` and
per-run majors appear on `PgShardCluster.status` (`servingPGMajor`,
`reshard.pgMajor`, `reshard.retiredPGMajor`). The bound grammar today is
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
convergence. The `reshard-scale` suite runs the 1 → 2 → 4 → 2 reshard
under the same ledger oracle. Both run on a single small shard to stay
inside the runner budget.

## After the upgrade

- pgBackRest: the new-major groups archive into their **own stanza** (the
  stanza is derived per group); take a fresh full backup immediately.
- **PITR does not cross majors**: restore points and WAL from the 18 era
  restore onto 18 only. Keep the old repository until its retention ends.
