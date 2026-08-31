-- The catalog is a supported interface: pgshard_admin edits these tables
-- with ordinary SQL. Where the controller's Go validation was stricter than
-- the table's constraints, a row could be written that the controller then
-- rejects on every pass for ever, or accepted and acted on with a meaning
-- nobody intended.
--
-- These say in SQL what the controller already required.

-- validateTable refuses a sharded table whose shard key is empty, and any
-- shard key at all on a reference or unsharded one. The table only required
-- that a sharded row's key be non-NULL.
ALTER TABLE pgshard.tables
    DROP CONSTRAINT IF EXISTS sharded_tables_need_shard_key,
    ADD CONSTRAINT tables_shard_key_matches_placement CHECK (
        CASE placement
            WHEN 'sharded' THEN shard_key IS NOT NULL AND shard_key <> ''
            ELSE shard_key IS NULL
        END);

-- home_shard names a shard. A negative one routes unsharded work to a shard
-- that cannot exist, and nothing said so until the routing failed.
ALTER TABLE pgshard.databases
    ADD CONSTRAINT databases_home_shard_is_a_shard CHECK (home_shard >= 0);

-- A stream's name is embedded verbatim in the replication slot it creates,
-- and PostgreSQL refuses a slot name outside [a-z0-9_]. A stream named
-- 'a-b' was therefore stored happily and then failed to create its slot on
-- every shard, for ever. ValidStreamName has always refused it at the RPC;
-- the table did not.
ALTER TABLE pgshard.streams
    ADD CONSTRAINT streams_name_is_slot_safe CHECK (name ~ '^[a-z][a-z0-9_]{0,31}$'),
    ADD CONSTRAINT streams_state_is_known CHECK (state IN ('creating', 'active', 'lost')),
    ADD CONSTRAINT streams_database_is_named CHECK (database <> '');
