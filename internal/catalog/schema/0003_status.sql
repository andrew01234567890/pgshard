-- Status tables: written only by pgshard_system; readable by everyone else.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.database_status (
    database             text        PRIMARY KEY,
    state                text        NOT NULL,
    effective_generation bigint      NOT NULL DEFAULT 0,
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.table_status (
    database             text        NOT NULL,
    schema_name          text        NOT NULL,
    table_name           text        NOT NULL,
    effective_placement  text,
    effective_shard_key  text,
    effective_generation bigint      NOT NULL DEFAULT 0,
    workflow_id          uuid,
    progress             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema_name, table_name)
);

CREATE TABLE pgshard.shard_status (
    shard_set        text        NOT NULL,
    shard_id         integer     NOT NULL,
    group_name       text        NOT NULL,
    serving_state    text        NOT NULL,
    primary_epoch    bigint      NOT NULL DEFAULT 0,
    primary_endpoint text,
    replay_lag_bytes bigint,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (shard_set, shard_id)
);

CREATE TABLE pgshard.role_status (
    rolname              text        PRIMARY KEY,
    effective_generation bigint      NOT NULL DEFAULT 0,
    per_shard            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.workflows (
    id          uuid        PRIMARY KEY,
    kind        text        NOT NULL,
    state       text        NOT NULL,
    spec        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    journal_ids text[]      NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    error       text
);

CREATE TABLE pgshard.migrations (
    id         uuid        PRIMARY KEY,
    database   text        NOT NULL,
    statement  text        NOT NULL,
    strategy   text        NOT NULL,
    state      text        NOT NULL,
    per_shard  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.xact_decisions (
    gid          text        PRIMARY KEY,
    state        text        NOT NULL CHECK (state IN ('preparing', 'commit', 'abort')),
    participants integer[]   NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    decided_at   timestamptz
);

CREATE TABLE pgshard.streams (
    name     text  PRIMARY KEY,
    spec     jsonb NOT NULL DEFAULT '{}'::jsonb,
    position jsonb NOT NULL DEFAULT '{}'::jsonb,
    state    text  NOT NULL
);

CREATE TABLE pgshard.sequences (
    name       text    PRIMARY KEY,
    next_value bigint  NOT NULL DEFAULT 1,
    block_size integer NOT NULL DEFAULT 1000 CHECK (block_size > 0)
);

CREATE TABLE pgshard.restore_points (
    id                   uuid        PRIMARY KEY,
    name                 text        NOT NULL,
    shard_map_generation bigint      NOT NULL,
    per_group            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.serving (
    shard_set    text        PRIMARY KEY,
    generation   bigint      NOT NULL,
    published_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.shard_map_generation (
    singleton  boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    generation bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO pgshard.shard_map_generation DEFAULT VALUES;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'database_status', 'table_status', 'shard_status', 'role_status', 'workflows',
        'migrations', 'xact_decisions', 'streams', 'sequences', 'restore_points',
        'serving', 'shard_map_generation'
    ] LOOP
        EXECUTE format('REVOKE ALL ON pgshard.%I FROM pgshard_admin, pgshard_reader', t);
        EXECUTE format('GRANT SELECT ON pgshard.%I TO pgshard_admin, pgshard_reader', t);
    END LOOP;
END
$$;

RESET ROLE;
