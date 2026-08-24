-- Reshard cutover: the range-scoped write fence on the source shards, the
-- control-plane copy of the resharding journal, and the lock table other
-- workflows (the DDL applier) honor while a cutover is in flight.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_status
    ADD COLUMN migrating boolean NOT NULL DEFAULT false;

CREATE TABLE pgshard.resharding_journal (
    id           uuid        PRIMARY KEY,
    generation   bigint      NOT NULL,
    shard_set    text        NOT NULL,
    participants integer[]   NOT NULL,
    targets      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.workflow_locks (
    kind        text        NOT NULL,
    key         text        NOT NULL,
    workflow_id uuid        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, key)
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['resharding_journal', 'workflow_locks'] LOOP
        EXECUTE format('REVOKE ALL ON pgshard.%I FROM pgshard_admin, pgshard_reader', t);
        EXECUTE format('GRANT SELECT ON pgshard.%I TO pgshard_admin, pgshard_reader', t);
    END LOOP;
END
$$;

RESET ROLE;
