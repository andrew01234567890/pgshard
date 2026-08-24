-- Table placement workflows: the table-scoped write fence routers observe
-- while a table's rows move to their new placement.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status
    ADD COLUMN migrating boolean NOT NULL DEFAULT false;

RESET ROLE;
