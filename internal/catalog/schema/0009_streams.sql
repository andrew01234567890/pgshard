-- Change streams: per-stream options and per-(stream, shard) slot status.
-- Numbered 0009 because 0006-0008 are taken by migrations developed in
-- parallel; the loader accepts gaps, so the version sequence is strictly
-- increasing but not necessarily contiguous.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.streams
    ADD COLUMN database   text        NOT NULL DEFAULT '',
    ADD COLUMN two_phase  boolean     NOT NULL DEFAULT false,
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(),
    ALTER COLUMN state SET DEFAULT 'creating';

CREATE TABLE pgshard.stream_status (
    stream              text        NOT NULL REFERENCES pgshard.streams(name) ON DELETE CASCADE,
    shard_set           text        NOT NULL,
    shard_id            integer     NOT NULL,
    slot                text        NOT NULL,
    -- pg_replication_slots.wal_status of the slot; 'lost' means the slot
    -- was invalidated and the stream must be recreated.
    wal_status          text        NOT NULL DEFAULT '',
    invalidation_reason text        NOT NULL DEFAULT '',
    confirmed_flush_lsn bigint      NOT NULL DEFAULT 0,
    restart_lsn         bigint      NOT NULL DEFAULT 0,
    active              boolean     NOT NULL DEFAULT false,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, shard_set, shard_id)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.streams, pgshard.stream_status TO pgshard_admin;
GRANT SELECT ON pgshard.stream_status TO pgshard_reader;

RESET ROLE;
