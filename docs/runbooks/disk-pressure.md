# Runbook: disk pressure and max_slot_wal_keep_size

Tuning caps WAL retained by replication slots at `max_slot_wal_keep_size`
(20% of the volume, clamped to [4GiB, 200GiB]) so a stalled consumer can
fill at most that much — it invalidates the slot before it fills the disk
([slot-invalidation](slot-invalidation.md)). This runbook is for a data
volume filling up anyway.

## Triage: what is eating the disk?

```sh
kubectl exec <primary-pod> -c postgres -- df -h /var/lib/postgresql
kubectl exec <primary-pod> -c postgres -- du -sh /var/lib/postgresql/*/pg_wal
```

```sql
-- WAL pinned by slots
SELECT slot_name, slot_type, active, wal_status,
       pg_size_pretty(pg_current_wal_lsn() - restart_lsn) AS retained
FROM pg_replication_slots ORDER BY restart_lsn;
-- archiving health: failing archive_command retains everything
SELECT * FROM pg_stat_archiver;
-- prepared transactions pin the horizon too
SELECT gid, prepared FROM pg_prepared_xacts;
-- table/bloat growth
SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
FROM pg_class ORDER BY pg_total_relation_size(oid) DESC LIMIT 10;
```

## By cause

- **Failing archiving** (`failed_count` climbing): WAL is never removable
  until archived. Fix the repository first
  ([backup-failures](backup-failures.md)); the backlog drains itself.
- **Stalled logical slot**: find the stream on the admin UI `/streams`
  page; either revive the consumer or drop the stream. Do not raise
  `max_slot_wal_keep_size` to paper over a dead consumer.
- **Stalled physical slot** (a standby down for long): fix or remove the
  member. The operator manages the physical slots around failovers; a slot
  belonging to a deleted member should disappear — if one lingers inactive
  with no matching pod, that is a bug worth reporting, and
  `pg_drop_replication_slot` on it is the mitigation.
- **Old prepared transactions**: resolve them
  ([in-doubt-transactions](in-doubt-transactions.md)); they block vacuum
  cluster-wide on that shard.
- **Genuine data growth**: grow the volume — `spec.storage.size` is an
  online expansion on an expandable StorageClass
  ([storage-changes](storage-changes.md)). Note `max_wal_size` and
  `max_slot_wal_keep_size` are derived from the volume size, so tuning
  recomputes them after a resize.

## If the volume is already full

PostgreSQL panics on a full `pg_wal`. Preferred order:

1. expand the PVC (works while the pod is down);
2. make archiving succeed so segments become removable, then start;
3. only with support-level care: drop an invalidated (`lost`) slot to free
   its retained WAL — it was unusable anyway.

Never delete files from `pg_wal` by hand.

## Prevention

Alert at 80% volume usage, on `pg_stat_archiver.failed_count`, and on
`pgshard.stream_status.retained_bytes` > half of `max_slot_wal_keep_size`.
The `Degraded`/`BackupHealthy` conditions surface the archiving half; slot
health is on the admin UI.
