-- Certified barrier points: the cluster-wide write fence routers observe, the
-- participant transaction ids the restore reconciler checks commit status
-- with, and the certified flag on restore points.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_map_generation
    ADD COLUMN write_fence boolean NOT NULL DEFAULT false,
    ADD COLUMN write_fence_reason text NOT NULL DEFAULT '',
    ADD COLUMN write_fenced_at timestamptz;

ALTER TABLE pgshard.xact_decisions
    ADD COLUMN participant_xids text[] NOT NULL DEFAULT '{}';

ALTER TABLE pgshard.restore_points
    ADD COLUMN certified boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT restore_points_name_key UNIQUE (name);

RESET ROLE;
