-- A sharded table's rows are placed by the router from the key in the
-- statement, and nothing on the shard checked that the stored row's key
-- belongs there: a BEFORE trigger that rewrote the key, or a write made on
-- a shard directly, left the row on a shard no lookup of it would reach
-- (PGS-878). Each shard now records the range it owns, and every sharded
-- table carries a check against it. owner_guard_generation is the
-- effective generation whose check is installed on every shard; NULL means
-- none is.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status
    ADD COLUMN owner_guard_generation bigint;

RESET ROLE;
