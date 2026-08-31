-- pgshard.tables and the other per-database rows reference pgshard.databases
-- and go with it; streams only checked that their database was not the empty
-- string. So a stream could name a database that had never been declared, or
-- outlive one that was dropped, and it failed later -- on every shard, when
-- its slot could not be created against a database that was not there.
--
-- ON DELETE CASCADE, like the others: dropping a database takes the streams
-- of that database with it rather than leaving rows nothing can serve.
ALTER TABLE pgshard.streams
    DROP CONSTRAINT IF EXISTS streams_database_is_named,
    ADD CONSTRAINT streams_database_fkey
        FOREIGN KEY (database) REFERENCES pgshard.databases (name) ON DELETE CASCADE;
