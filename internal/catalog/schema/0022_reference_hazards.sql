-- A reference table is replicated to every shard and a write to it runs on
-- every shard, so anything the shard evaluates for itself -- a column
-- default, a generated expression, an identity column, a trigger, a rule --
-- yields a different row on each one while 2PC still commits them all. None
-- of it appears in the statement, so the router cannot see it: the
-- controller inspects the table on the shards and records what it found.
--
-- NULL reference_checked_generation means the inspection has not run for the
-- table's current generation, which routers treat as unsafe. The default is
-- deliberately not 0: an existing row must read as unchecked after upgrade,
-- not as checked and clean.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status
    ADD COLUMN reference_checked_generation bigint,
    ADD COLUMN reference_hazards text[] NOT NULL DEFAULT '{}';

RESET ROLE;
