-- Declaring a pre-existing table sharded made it effective on the spot: the
-- reconciler only sees the catalog, and the catalog does not know what type
-- the key column has on the shards. That matters because the router hashes
-- the client's bytes while a row filter hashes the stored value, and for a
-- blank-padded character(n) those are different values for keys PostgreSQL
-- calls equal -- two "equal" keys on two shards. A table that arrives
-- through pgshard is refused at CREATE TABLE; one that was already there
-- was not refused anywhere.
--
-- So a sharded table is not routed until a shard has been asked what the
-- key column actually is. NULL shard_key_checked_generation means the
-- question has not been answered for the table's current generation.
-- Tables already effective keep routing: the gate is on activation, which
-- is where the unchecked table entered.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status
    ADD COLUMN shard_key_checked_generation bigint,
    ADD COLUMN shard_key_error text;

RESET ROLE;
