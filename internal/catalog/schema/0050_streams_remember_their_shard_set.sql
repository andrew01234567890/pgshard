-- pgshard.streams records which shard set its slots were made on.
--
-- Controller.CreateStream has always taken a shard set, and uses it to pick
-- the shards it creates slots on -- and then discards it. Nothing else can
-- ask afterwards, which leaves two things broken.
--
-- The stream monitor sweeps EVERY shard set (listShards(ctx, "")) because
-- it has no way to narrow, so a reshard provisioning its targets gives
-- every stream a stream_status row per new shard with no slot behind it.
-- Those rows read as a stream with idle slots, and anything that tried to
-- read them as a lost position would condemn every stream on the cluster
-- (PGS-807, PR #899 closed for exactly that).
--
-- And VStreamInfo cannot report where a stream's slots are, so a consumer
-- cannot discover the answer either.
--
-- Existing rows are backfilled to 'default'. That is what the router's
-- Create has always resolved an empty set to, so it is right for every
-- stream made through the VStream API; a stream made directly through
-- Controller.CreateStream on another set cannot be recovered, because the
-- set was never written down. That is the defect being fixed rather than a
-- shortcoming of the backfill.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.streams ADD COLUMN shard_set text NOT NULL DEFAULT '';
UPDATE pgshard.streams SET shard_set = 'default' WHERE shard_set = '';

RESET ROLE;
