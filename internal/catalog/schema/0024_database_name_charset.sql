-- A database name reaches libpq connection strings that carry a shard
-- superuser credential. libpq separates keywords on whitespace, so a name
-- carrying any would be an injection of host, sslmode and the rest; the
-- callers quote, and this refuses the shape outright as well. The quote
-- and the backslash go with it, so the catalog, the router and the agent
-- accept the same set of names.
ALTER TABLE pgshard.databases
    ADD CONSTRAINT database_name_is_connection_safe
    CHECK (name <> '' AND name !~ '[[:space:][:cntrl:]''\\]');
