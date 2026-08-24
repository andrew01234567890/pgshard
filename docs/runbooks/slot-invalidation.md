# Runbook: slot invalidation and POSITION_TOO_OLD

Mechanism: [streams.md](../streams.md). A logical slot that falls too far
behind is invalidated by PostgreSQL (`max_slot_wal_keep_size`,
`idle_replication_slot_timeout=24h`); its WAL is gone and no consumer can
resume from it.

## Symptoms

- A VStream consumer gets `Error{POSITION_TOO_OLD}`.
- Admin UI `/streams` shows a LOST banner; `pgshard.streams.state = 'lost'`.
- On the shard:

```sql
SELECT slot_name, active, wal_status, invalidation_reason,
       restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name LIKE 'pgshard_%';
```

`wal_status = 'lost'` is invalidated; the controller's stream monitor
copies this into `pgshard.stream_status` (with `retained_bytes`,
`synced`, `failover`).

## Why it happens

- The consumer stopped acking (crashed, or holds the stream open without
  `Ack`) — `confirmed_flush_lsn` stalls and `retained_bytes` grows until
  the cap.
- Sustained write volume above what the consumer drains.
- After a failover, a slot that was **not** synchronized to the promoted
  standby is missing there (`wal_status = 'missing'` in the status
  table). Slots are created with `failover = true` and standbys run
  `sync_replication_slots = on`, so this points at a standby that was not
  syncing — check `synced` in `pgshard.stream_status` *before* promotions.

## Recovery

An invalidated slot cannot be rewound. Recover the consumer, not the slot:

1. Drop and recreate the stream (`VStream.Drop` then `Create`, or
   `Controller.DropStream`/`CreateStream`) — this recreates the per-shard
   slots at the current position.
2. Re-baseline the consumer with an initial copy:
   `Stream` with `options.start_from: START_FROM_COPY` copies every table
   through an exported snapshot and then streams from the copy's
   consistent point. Consumers apply copy rows as upserts keyed by primary
   key, so a re-copy over existing downstream state converges.
3. Resume normal acking; watch `retained_bytes` on `/streams/{name}`.

## Prevention

- Alert on `pgshard.stream_status.retained_bytes` approaching
  `max_slot_wal_keep_size` (see [disk-pressure](disk-pressure.md)) and on
  `active = false` for more than a few minutes.
- Drop streams you no longer consume — an idle slot is invalidated after
  24h (`idle_replication_slot_timeout`) but retains WAL until then.
- Keep `synced = true` on every slot so failovers do not lose positions;
  the operator maintains `synchronized_standby_slots` so failover slots
  never confirm past a synchronous standby.
