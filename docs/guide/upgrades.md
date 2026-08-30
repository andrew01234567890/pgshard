# Major upgrades: PostgreSQL 18 to 19

> **Status: implemented for the online strategy.** Editing
> `spec.postgresql.major` is the trigger: the operator provisions groups on
> the new major, the controller replicates into them and cuts over with the
> reshard machinery, and the catalog group is upgraded the same way. The
> kind suite `upgrade` runs 18→19 end to end. `spec.upgrade.strategy:
> offline` (`pg_upgrade --link`) is still planned and is not acted on.
>
> [upgrade.md](../upgrade.md) is the reference for the stages and the
> rollback window; [capability-matrix.md](../capability-matrix.md) is the
> per-feature status of record.

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
