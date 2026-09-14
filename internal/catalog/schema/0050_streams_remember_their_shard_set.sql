-- pgshard.streams records which shard set its slots were made on.
--
-- Controller.CreateStream has always taken a shard set, and uses it to pick
-- the shards it creates slots on -- and then discards it. Nothing else can
-- ask afterwards, which leaves the stream monitor sweeping EVERY shard set
-- (listShards(ctx, "")) because it has no way to narrow. A reshard
-- provisioning its targets therefore gives every stream a stream_status row
-- per new shard with no slot behind it, and those rows are indistinguishable
-- from slots that have gone: PGS-807 (PR #899) was closed for reading them
-- as exactly that, which would have condemned every stream on any cluster
-- that had ever resharded.
--
-- The backfill reads the answer out of pgshard.stream_status rather than
-- assuming it. That table is keyed (stream, shard_set, shard_id) and the
-- monitor has been writing it all along, so the set a stream last reported
-- a REAL slot on is on record even though pgshard.streams never kept it.
-- Only a stream that has never been swept falls back, and it falls back to
-- 'default' because that is the literal StreamAdmin.Create resolved an
-- empty set to -- not the router, which forwards whatever it was given and
-- says so.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.streams ADD COLUMN shard_set text NOT NULL DEFAULT '';

UPDATE pgshard.streams s SET shard_set = COALESCE(
    (SELECT ss.shard_set FROM pgshard.stream_status ss
      WHERE ss.stream = s.name AND ss.wal_status NOT IN ('missing', '')
      ORDER BY ss.updated_at DESC LIMIT 1),
    'default')
WHERE s.shard_set = '';

RESET ROLE;
