-- The shard key check already asks a shard what the key column is, and then
-- throws the answer away once it has decided the type is hashable. The
-- router needs the answer itself, because hashing the client's bytes is
-- only the same as hashing the stored value once the router applies the
-- same normalisation the column's type does.
--
-- character varying(n) is the case that bites: PostgreSQL silently drops
-- the excess when an overlength value's excess is all spaces, so it stores
-- and hashes 'abc' where the client sent 'abc   '. The router routed by the
-- untruncated bytes and so read and wrote that row on a different shard
-- from the one holding it.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status
    ADD COLUMN shard_key_type text;

RESET ROLE;
