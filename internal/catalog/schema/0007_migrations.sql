-- DDL/DCL migrations: the router queues every DDL statement here and the
-- controller's applier drives it across the shards it targets.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.migrations
    ADD COLUMN kind        text    NOT NULL DEFAULT '',
    ADD COLUMN scope       text    NOT NULL DEFAULT 'all'
                                   CHECK (scope IN ('all', 'home', 'existing')),
    ADD COLUMN home_shard  integer NOT NULL DEFAULT 0,
    ADD COLUMN meta        jsonb   NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN error       text,
    ADD COLUMN finished_at timestamptz,
    ADD CONSTRAINT migrations_state CHECK (state IN ('queued', 'running', 'complete', 'failed')),
    ADD CONSTRAINT migrations_strategy CHECK (strategy IN ('direct', 'concurrent'));

CREATE INDEX migrations_pending ON pgshard.migrations (created_at) WHERE state IN ('queued', 'running');

RESET ROLE;
