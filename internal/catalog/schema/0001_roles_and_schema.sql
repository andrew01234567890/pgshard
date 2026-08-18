-- Roles, schema ownership and shared plumbing for the pgshard control plane.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_system') THEN
        CREATE ROLE pgshard_system NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_admin') THEN
        CREATE ROLE pgshard_admin NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_reader') THEN
        CREATE ROLE pgshard_reader NOLOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    EXECUTE format('GRANT CREATE ON DATABASE %I TO pgshard_system', current_database());
END
$$;

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER SCHEMA pgshard OWNER TO pgshard_system;
ALTER TABLE pgshard.schema_migrations OWNER TO pgshard_system;

SET LOCAL ROLE pgshard_system;

GRANT USAGE ON SCHEMA pgshard TO pgshard_admin, pgshard_reader;
GRANT SELECT ON pgshard.schema_migrations TO pgshard_admin, pgshard_reader;

CREATE SEQUENCE pgshard.desired_generation_seq AS bigint;
GRANT USAGE ON SEQUENCE pgshard.desired_generation_seq TO pgshard_admin;

CREATE TABLE pgshard.hash_versions (
    version     integer PRIMARY KEY,
    description text    NOT NULL
);
INSERT INTO pgshard.hash_versions (version, description)
VALUES (1, 'postgresql-extended-hash-seed-8816678312871386365');
GRANT SELECT ON pgshard.hash_versions TO pgshard_admin, pgshard_reader;

CREATE FUNCTION pgshard.stamp_desired_row() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.desired_generation := nextval('pgshard.desired_generation_seq');
    NEW.updated_at := now();
    RETURN NEW;
END
$$;

CREATE FUNCTION pgshard.notify_desired_change() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    generation bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        generation := nextval('pgshard.desired_generation_seq');
    ELSE
        SELECT max(desired_generation) INTO generation FROM new_rows;
    END IF;
    IF generation IS NOT NULL THEN
        PERFORM pg_notify('pgshard_desired', TG_TABLE_NAME || ':' || generation);
    END IF;
    RETURN NULL;
END
$$;

RESET ROLE;
