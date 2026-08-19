-- Slot health details for the admin streams panel: WAL retained behind the
-- slot and the failover-slot flags.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.stream_status
    ADD COLUMN retained_bytes bigint  NOT NULL DEFAULT 0,
    ADD COLUMN synced         boolean NOT NULL DEFAULT false,
    ADD COLUMN failover       boolean NOT NULL DEFAULT false;

RESET ROLE;
