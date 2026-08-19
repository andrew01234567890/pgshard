-- Cluster-wide roles: attributes, memberships, normalized grants, per-role
-- settings and the per-group drift status the controller maintains.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.roles
    ADD COLUMN login            boolean     NOT NULL DEFAULT true,
    ADD COLUMN createdb         boolean     NOT NULL DEFAULT false,
    ADD COLUMN createrole       boolean     NOT NULL DEFAULT false,
    ADD COLUMN inherit          boolean     NOT NULL DEFAULT true,
    ADD COLUMN connection_limit integer     NOT NULL DEFAULT -1,
    ADD COLUMN valid_until      timestamptz;

CREATE TABLE pgshard.role_members (
    rolname            text        NOT NULL REFERENCES pgshard.roles (rolname) ON DELETE CASCADE,
    member             text        NOT NULL REFERENCES pgshard.roles (rolname) ON DELETE CASCADE,
    admin_option       boolean     NOT NULL DEFAULT false,
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rolname, member)
);

ALTER TABLE pgshard.grants
    ADD COLUMN object_schema text    NOT NULL DEFAULT '',
    ADD COLUMN column_name   text    NOT NULL DEFAULT '',
    ADD COLUMN grant_option  boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT grants_object_kind CHECK (object_kind IN ('table', 'sequence', 'schema', 'database', 'function', 'type', 'language', 'foreign server', 'foreign data wrapper', 'tablespace', 'large object', 'domain', 'parameter'));

CREATE UNIQUE INDEX grants_desired_key ON pgshard.grants (rolname, database, object_kind, object_schema, object_name, column_name);

CREATE TABLE pgshard.role_settings (
    rolname            text        NOT NULL REFERENCES pgshard.roles (rolname) ON DELETE CASCADE,
    database           text        NOT NULL DEFAULT '',
    name               text        NOT NULL,
    value              text        NOT NULL,
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rolname, database, name)
);

-- role_status becomes per (role, group); the unused per_shard form goes.
DROP TABLE pgshard.role_status;

CREATE TABLE pgshard.role_status (
    rolname          text        NOT NULL,
    group_name       text        NOT NULL,
    state            text        NOT NULL CHECK (state IN ('in_sync', 'drifted', 'missing', 'unmanaged')),
    details          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    roles_generation bigint      NOT NULL DEFAULT 0,
    checked_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rolname, group_name)
);

CREATE TABLE pgshard.role_group_status (
    group_name       text        PRIMARY KEY,
    roles_generation bigint      NOT NULL DEFAULT 0,
    materialized_at  timestamptz NOT NULL DEFAULT now()
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['role_members', 'role_settings'] LOOP
        EXECUTE format(
            'CREATE TRIGGER stamp_desired BEFORE INSERT OR UPDATE ON pgshard.%I
             FOR EACH ROW EXECUTE FUNCTION pgshard.stamp_desired_row()', t);
        EXECUTE format(
            'CREATE TRIGGER notify_desired_insert AFTER INSERT ON pgshard.%I
             REFERENCING NEW TABLE AS new_rows
             FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change()', t);
        EXECUTE format(
            'CREATE TRIGGER notify_desired_update AFTER UPDATE ON pgshard.%I
             REFERENCING NEW TABLE AS new_rows
             FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change()', t);
        EXECUTE format(
            'CREATE TRIGGER notify_desired_delete AFTER DELETE ON pgshard.%I
             FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change()', t);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.%I TO pgshard_admin', t);
        EXECUTE format('GRANT SELECT ON pgshard.%I TO pgshard_reader', t);
    END LOOP;
END
$$;

GRANT SELECT ON pgshard.role_status, pgshard.role_group_status TO pgshard_admin, pgshard_reader;

RESET ROLE;
