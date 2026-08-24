# Major upgrades: PostgreSQL 18 to 19

> **Status: planned.** The CRD carries the upgrade spec
> (`spec.upgrade.strategy: online | offline`, `spec.upgrade.maxParallelGroups`)
> and both majors are fully built, tested and released as images, but the
> operator does not yet act on a `spec.postgresql.major` change. Do not
> edit `major` on a live cluster. See
> [capability-matrix.md](../capability-matrix.md).

## What exists today

- **Both majors end to end.** Every image, the catalog schema, the pooler,
  the router and the e2e suites run against PostgreSQL 18 and 19; the
  pgoutput decoder has golden captures from both, and tuning knows the
  18/19 differences (`io_workers` vs `io_max_workers`).
- **Per-major stanzas.** Backups are keyed
  `<cluster>-<group>-pg<major>`, so an upgrade starts a fresh stanza and
  the old one ages out by retention — the repository layout is already
  upgrade-shaped.
- **Restore-based migration.** Today the supported path to 19 is
  side-by-side: create a new cluster with `major: 19`, move the data
  (dump/restore, or a VStream consumer), and switch the application.

## The intended flows

- **online** (default): one group at a time (`maxParallelGroups`), each
  group gets new-major members built beside the old ones via logical
  replication from the group primary, then a switchover onto the new-major
  member; the router keeps serving throughout. Requires the router to speak
  to mixed-major groups mid-upgrade; the PostgreSQL 19 grammar for the
  router's planner is pending.
- **offline**: per group, stop, `pg_upgrade --link`, restart — minutes of
  unavailability per group, no logical replication requirements.

## Minor updates

A PostgreSQL minor (or image) update is just a new image reference: the
operator classifies it as a rolling restart — standbys first, then a
switchover and the old primary rebuilt — with writes paused only for the
shutdown-to-promotion window per group.
See the [rolling-restart runbook](../runbooks/rolling-restarts.md) and
[pgbackrest-and-minor-upgrades](../runbooks/pgbackrest-and-minor-upgrades.md).
