-- Major-version upgrades: a shard set carries the PostgreSQL major its
-- groups run, so a pending set with a higher major is a blue/green
-- replacement (an upgrade), not a topology change.
ALTER TABLE pgshard.shard_sets ADD COLUMN pg_major int;
