BEGIN;

DO $pgshard_requirements$
BEGIN
    IF current_setting('server_version_num')::integer < 180000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '0A000',
            MESSAGE = 'pgshard requires PostgreSQL 18 or newer';
    END IF;

    IF current_database() <> 'shardschema' THEN
        RAISE EXCEPTION USING
            ERRCODE = '3D000',
            MESSAGE = 'the pgshard catalog must be installed in the dedicated shardschema database';
    END IF;

    IF getdatabaseencoding() <> 'UTF8' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'the shardschema database must use UTF8 encoding';
    END IF;
END
$pgshard_requirements$;

CREATE SCHEMA IF NOT EXISTS pgshard_catalog;
REVOKE ALL ON SCHEMA pgshard_catalog FROM PUBLIC;

DO $pgshard_roles$
DECLARE
    role_can_login boolean;
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_reader') THEN
        CREATE ROLE pgshard_catalog_reader NOLOGIN;
    ELSE
        SELECT rolcanlogin INTO role_can_login
          FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_reader';
        IF role_can_login THEN
            RAISE EXCEPTION USING ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard_catalog_reader role must be NOLOGIN';
        END IF;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_admin') THEN
        CREATE ROLE pgshard_catalog_admin NOLOGIN;
    ELSE
        SELECT rolcanlogin INTO role_can_login
          FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_admin';
        IF role_can_login THEN
            RAISE EXCEPTION USING ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard_catalog_admin role must be NOLOGIN';
        END IF;
    END IF;
END
$pgshard_roles$;

GRANT pgshard_catalog_reader TO pgshard_catalog_admin;
GRANT USAGE ON SCHEMA pgshard_catalog TO pgshard_catalog_reader;
GRANT USAGE ON SCHEMA pgshard_catalog TO pgshard_catalog_admin;

DO $pgshard_domains$
BEGIN
    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'sql_identifier'
    ) THEN
        CREATE DOMAIN pgshard_catalog.sql_identifier AS text
            CHECK (
                octet_length(VALUE) BETWEEN 1 AND 63
            );
    END IF;

    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'resource_name'
    ) THEN
        CREATE DOMAIN pgshard_catalog.resource_name AS text
            CHECK (
                VALUE ~ '^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$'
                AND octet_length(VALUE) BETWEEN 1 AND 63
            );
    END IF;

    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'uint64_boundary'
    ) THEN
        CREATE DOMAIN pgshard_catalog.uint64_boundary AS numeric(20, 0)
            CHECK (VALUE >= 0 AND VALUE <= 18446744073709551616);
    END IF;
END
$pgshard_domains$;

CREATE TABLE IF NOT EXISTS pgshard_catalog.cluster_configuration (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    cluster_id uuid NOT NULL DEFAULT gen_random_uuid(),
    home_shard_id pgshard_catalog.resource_name NOT NULL DEFAULT 'shard-0000',
    hash_algorithm text NOT NULL DEFAULT 'xxh3_64'
        CHECK (hash_algorithm = 'xxh3_64'),
    hash_version smallint NOT NULL DEFAULT 1 CHECK (hash_version = 1),
    hash_seed numeric(20, 0) NOT NULL DEFAULT 0
        CHECK (hash_seed >= 0 AND hash_seed <= 18446744073709551615),
    text_key_encoding text NOT NULL DEFAULT 'UTF8'
        CHECK (text_key_encoding = 'UTF8'),
    text_key_collation text NOT NULL DEFAULT 'C'
        CHECK (text_key_collation = 'C'),
    installed_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

COMMENT ON TABLE pgshard_catalog.cluster_configuration IS
    'Immutable routing hash contract; this catalog intentionally stores no credentials or password material.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.cluster_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    catalog_epoch bigint NOT NULL DEFAULT 0 CHECK (catalog_epoch >= 0),
    changed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (catalog_epoch < 9223372036854775807)
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_databases (
    logical_database_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    database_name pgshard_catalog.sql_identifier NOT NULL UNIQUE,
    schema_epoch bigint NOT NULL DEFAULT 1 CHECK (schema_epoch > 0),
    authorization_epoch bigint NOT NULL DEFAULT 1 CHECK (authorization_epoch > 0),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.shards (
    shard_id pgshard_catalog.resource_name PRIMARY KEY,
    shard_number bigint NOT NULL UNIQUE CHECK (shard_number BETWEEN 0 AND 4294967295),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('provisioning', 'active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.routing_epochs (
    routing_epoch bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    state text NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'active', 'superseded')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    superseded_at timestamptz,
    UNIQUE (logical_database_id, routing_epoch),
    CHECK ((state = 'staged') = (activated_at IS NULL)),
    CHECK ((state = 'superseded') = (superseded_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS routing_epochs_one_active_per_database
    ON pgshard_catalog.routing_epochs(logical_database_id)
    WHERE state = 'active';

CREATE TABLE IF NOT EXISTS pgshard_catalog.routing_ranges (
    routing_epoch bigint NOT NULL
        REFERENCES pgshard_catalog.routing_epochs(routing_epoch) ON DELETE RESTRICT,
    range_start pgshard_catalog.uint64_boundary NOT NULL,
    range_end pgshard_catalog.uint64_boundary NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    PRIMARY KEY (routing_epoch, range_start),
    CHECK (range_start < range_end),
    CHECK (range_start < 18446744073709551616)
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.active_routing_epochs (
    logical_database_id uuid PRIMARY KEY
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    routing_epoch bigint NOT NULL UNIQUE,
    activated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    FOREIGN KEY (logical_database_id, routing_epoch)
        REFERENCES pgshard_catalog.routing_epochs(logical_database_id, routing_epoch)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.registered_tables (
    registered_table_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    schema_name pgshard_catalog.sql_identifier NOT NULL,
    table_name pgshard_catalog.sql_identifier NOT NULL,
    shard_key_column pgshard_catalog.sql_identifier NOT NULL,
    shard_key_type text NOT NULL CHECK (shard_key_type IN ('bigint', 'uuid', 'text', 'bytea')),
    shard_key_encoding text,
    shard_key_collation text,
    hash_version smallint NOT NULL DEFAULT 1 CHECK (hash_version = 1),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (logical_database_id, schema_name, table_name),
    CHECK (
        (shard_key_type = 'text' AND shard_key_encoding = 'UTF8' AND shard_key_collation = 'C')
        OR
        (shard_key_type <> 'text' AND shard_key_encoding IS NULL AND shard_key_collation IS NULL)
    )
);

COMMENT ON COLUMN pgshard_catalog.registered_tables.shard_key_collation IS
    'Text shard keys use PostgreSQL C collation so byte-distinct UTF8 keys remain routing-distinct.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.operation_tombstones (
    operation_kind pgshard_catalog.sql_identifier NOT NULL,
    operation_id uuid NOT NULL,
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    outcome_code pgshard_catalog.sql_identifier NOT NULL,
    result_fingerprint bytea CHECK (result_fingerprint IS NULL OR octet_length(result_fingerprint) = 32),
    completed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (operation_kind, operation_id)
);

COMMENT ON TABLE pgshard_catalog.operation_tombstones IS
    'Permanent idempotency records. Only fixed-size fingerprints are retained; request/result bodies and secrets are forbidden.';

-- Disable existing statement triggers before idempotent seed statements. PostgreSQL
-- fires AFTER STATEMENT triggers even when ON CONFLICT inserts zero rows.
DROP TRIGGER IF EXISTS cluster_state_notify ON pgshard_catalog.cluster_state;
DROP TRIGGER IF EXISTS logical_databases_touch_catalog ON pgshard_catalog.logical_databases;
DROP TRIGGER IF EXISTS shards_touch_catalog ON pgshard_catalog.shards;
DROP TRIGGER IF EXISTS registered_tables_touch_catalog ON pgshard_catalog.registered_tables;

INSERT INTO pgshard_catalog.cluster_configuration(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO pgshard_catalog.cluster_state(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
VALUES ('shard-0000', 0, 'active')
ON CONFLICT (shard_id) DO NOTHING;

CREATE OR REPLACE FUNCTION pgshard_catalog.reject_all_changes()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = format('%I.%I is immutable', TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_routing_epoch_history()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'staged' THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing epochs are permanent';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.state = 'staged' THEN
        IF NEW.routing_epoch <> OLD.routing_epoch
           OR NEW.logical_database_id <> OLD.logical_database_id
           OR NEW.created_at <> OLD.created_at
           OR NEW.state NOT IN ('staged', 'active')
           OR (NEW.state = 'staged' AND (NEW.activated_at IS NOT NULL OR NEW.superseded_at IS NOT NULL))
           OR (NEW.state = 'active' AND (NEW.activated_at IS NULL OR NEW.superseded_at IS NOT NULL)) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid staged routing epoch transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'active'
       AND NEW.state = 'superseded'
       AND NEW.routing_epoch = OLD.routing_epoch
       AND NEW.logical_database_id = OLD.logical_database_id
       AND NEW.created_at = OLD.created_at
       AND NEW.activated_at = OLD.activated_at
       AND NEW.superseded_at IS NOT NULL THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing epoch history is immutable';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_routing_range_history()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    protected_epoch bigint;
    epoch_state text;
BEGIN
    protected_epoch := CASE WHEN TG_OP = 'DELETE' THEN OLD.routing_epoch ELSE NEW.routing_epoch END;

    SELECT state
      INTO epoch_state
      FROM pgshard_catalog.routing_epochs
     WHERE routing_epoch = protected_epoch
     FOR KEY SHARE;

    IF epoch_state IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'routing epoch does not exist';
    END IF;

    IF epoch_state <> 'staged' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing ranges are immutable';
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.routing_epoch <> NEW.routing_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a routing range cannot move between epochs';
    END IF;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.notify_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
BEGIN
    IF NEW.catalog_epoch IS DISTINCT FROM OLD.catalog_epoch THEN
        PERFORM pg_catalog.pg_notify(
            'pgshard_catalog_changed',
            NEW.catalog_epoch::text
        );
    END IF;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.touch_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
BEGIN
    UPDATE pgshard_catalog.cluster_state
       SET catalog_epoch = catalog_epoch + 1,
           changed_at = statement_timestamp()
     WHERE singleton;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
BEGIN
    PERFORM 1
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_shard_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    becoming_unavailable boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard identities are permanent';
    END IF;

    IF NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.shard_number IS DISTINCT FROM OLD.shard_number
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard identity is immutable';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'provisioning' AND NEW.state = 'active')
        OR (OLD.state = 'active' AND NEW.state = 'draining')
        OR (OLD.state = 'draining' AND NEW.state IN ('active', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid shard lifecycle transition';
    END IF;

    becoming_unavailable := NEW.state NOT IN ('active', 'draining');

    IF becoming_unavailable AND EXISTS (
        SELECT
          FROM pgshard_catalog.routing_ranges AS ranges
          JOIN pgshard_catalog.routing_epochs AS epochs
            ON epochs.routing_epoch = ranges.routing_epoch
         WHERE ranges.shard_id = OLD.shard_id
           AND epochs.state = 'active'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s is referenced by active routing', OLD.shard_id);
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_database_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    becoming_retired boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical database identities are permanent';
    END IF;

    IF NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.database_name IS DISTINCT FROM OLD.database_name
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.schema_epoch < OLD.schema_epoch
       OR NEW.authorization_epoch < OLD.authorization_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical database identity and epochs are monotonic';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'active' AND NEW.state = 'draining')
        OR (OLD.state = 'draining' AND NEW.state IN ('active', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid logical database lifecycle transition';
    END IF;

    becoming_retired := NEW.state = 'retired';

    IF becoming_retired AND EXISTS (
        SELECT FROM pgshard_catalog.active_routing_epochs
         WHERE logical_database_id = OLD.logical_database_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical database %s still has active routing',
                OLD.logical_database_id
            );
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.validate_routing_epoch(target_routing_epoch bigint)
RETURNS void
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    expected_start numeric(20, 0) := 0;
    range_count bigint := 0;
    current_range record;
BEGIN
    IF NOT EXISTS (
        SELECT FROM pgshard_catalog.routing_epochs
        WHERE routing_epoch = target_routing_epoch AND state = 'staged'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'routing epoch is not staged';
    END IF;

    FOR current_range IN
        SELECT range_start, range_end, shard_id
          FROM pgshard_catalog.routing_ranges
         WHERE routing_epoch = target_routing_epoch
         ORDER BY range_start, range_end
    LOOP
        IF current_range.range_start > expected_start THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch has a gap at %s', expected_start);
        ELSIF current_range.range_start < expected_start THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch overlaps at %s', current_range.range_start);
        END IF;

        IF NOT EXISTS (
            SELECT FROM pgshard_catalog.shards
            WHERE shard_id = current_range.shard_id AND state IN ('active', 'draining')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch references unavailable shard %s', current_range.shard_id);
        END IF;

        expected_start := current_range.range_end;
        range_count := range_count + 1;
    END LOOP;

    IF range_count = 0 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'routing epoch has no ranges';
    END IF;

    IF expected_start <> 18446744073709551616 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format('routing epoch ends at %s instead of 18446744073709551616', expected_start);
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.activate_routing_epoch(
    target_logical_database_id uuid,
    target_routing_epoch bigint,
    expected_active_routing_epoch bigint,
    expected_catalog_epoch bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    observed_catalog_epoch bigint;
    observed_active_routing_epoch bigint;
    target_database_id uuid;
    resulting_catalog_epoch bigint;
BEGIN
    SELECT catalog_epoch
      INTO STRICT observed_catalog_epoch
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;

    IF observed_catalog_epoch IS DISTINCT FROM expected_catalog_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'catalog epoch compare-and-swap failed: expected %s, observed %s',
                coalesce(expected_catalog_epoch::text, 'NULL'),
                observed_catalog_epoch
            );
    END IF;

    SELECT routing_epoch
      INTO observed_active_routing_epoch
      FROM pgshard_catalog.active_routing_epochs
     WHERE logical_database_id = target_logical_database_id
     FOR UPDATE;

    IF observed_active_routing_epoch IS DISTINCT FROM expected_active_routing_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'active routing epoch compare-and-swap failed: expected %s, observed %s',
                coalesce(expected_active_routing_epoch::text, 'NULL'),
                coalesce(observed_active_routing_epoch::text, 'NULL')
            );
    END IF;

    SELECT epochs.logical_database_id
      INTO target_database_id
      FROM pgshard_catalog.routing_epochs AS epochs
      JOIN pgshard_catalog.logical_databases AS databases
        ON databases.logical_database_id = epochs.logical_database_id
     WHERE epochs.routing_epoch = target_routing_epoch
       AND epochs.state = 'staged'
       AND databases.state <> 'retired'
     FOR UPDATE OF epochs;

    IF target_database_id IS NULL OR target_database_id <> target_logical_database_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'target routing epoch is not staged for the requested logical database';
    END IF;

    IF observed_active_routing_epoch IS NOT NULL
       AND target_routing_epoch <= observed_active_routing_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'routing epoch must advance: active %s, target %s',
                observed_active_routing_epoch,
                target_routing_epoch
            );
    END IF;

    PERFORM pgshard_catalog.validate_routing_epoch(target_routing_epoch);

    IF observed_active_routing_epoch IS NOT NULL THEN
        UPDATE pgshard_catalog.routing_epochs
           SET state = 'superseded',
               superseded_at = statement_timestamp()
         WHERE routing_epoch = observed_active_routing_epoch;
    END IF;

    UPDATE pgshard_catalog.routing_epochs
       SET state = 'active',
           activated_at = statement_timestamp()
     WHERE routing_epoch = target_routing_epoch;

    INSERT INTO pgshard_catalog.active_routing_epochs(
        logical_database_id,
        routing_epoch,
        activated_at
    )
    VALUES (target_logical_database_id, target_routing_epoch, statement_timestamp())
    ON CONFLICT (logical_database_id) DO UPDATE
        SET routing_epoch = EXCLUDED.routing_epoch,
            activated_at = EXCLUDED.activated_at;

    UPDATE pgshard_catalog.cluster_state
       SET catalog_epoch = greatest(catalog_epoch + 1, target_routing_epoch),
           changed_at = statement_timestamp()
     WHERE singleton
     RETURNING catalog_epoch INTO resulting_catalog_epoch;

    RETURN resulting_catalog_epoch;
END
$function$;

DROP TRIGGER IF EXISTS cluster_configuration_immutable ON pgshard_catalog.cluster_configuration;
CREATE TRIGGER cluster_configuration_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.cluster_configuration
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.reject_all_changes();

DROP TRIGGER IF EXISTS routing_epoch_history_immutable ON pgshard_catalog.routing_epochs;
CREATE TRIGGER routing_epoch_history_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.routing_epochs
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_routing_epoch_history();

DROP TRIGGER IF EXISTS routing_range_history_immutable ON pgshard_catalog.routing_ranges;
CREATE TRIGGER routing_range_history_immutable
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.routing_ranges
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_routing_range_history();

DROP TRIGGER IF EXISTS operation_tombstone_immutable ON pgshard_catalog.operation_tombstones;
CREATE TRIGGER operation_tombstone_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.operation_tombstones
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.reject_all_changes();

DROP TRIGGER IF EXISTS cluster_state_notify ON pgshard_catalog.cluster_state;
CREATE TRIGGER cluster_state_notify
AFTER UPDATE ON pgshard_catalog.cluster_state
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.notify_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_touch_catalog ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_lock_catalog ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_protect_active_routing ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_protect_active_routing
BEFORE UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_database_lifecycle();

DROP TRIGGER IF EXISTS shards_touch_catalog ON pgshard_catalog.shards;
CREATE TRIGGER shards_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS shards_lock_catalog ON pgshard_catalog.shards;
CREATE TRIGGER shards_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS shards_protect_active_routing ON pgshard_catalog.shards;
CREATE TRIGGER shards_protect_active_routing
BEFORE UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_shard_lifecycle();

DROP TRIGGER IF EXISTS registered_tables_touch_catalog ON pgshard_catalog.registered_tables;
CREATE TRIGGER registered_tables_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.registered_tables
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS registered_tables_lock_catalog ON pgshard_catalog.registered_tables;
CREATE TRIGGER registered_tables_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.registered_tables
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

REVOKE ALL ON ALL TABLES IN SCHEMA pgshard_catalog FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgshard_catalog FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgshard_catalog FROM PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;

GRANT INSERT (database_name) ON pgshard_catalog.logical_databases TO pgshard_catalog_admin;
GRANT INSERT (shard_id, shard_number, state), UPDATE (state)
    ON pgshard_catalog.shards TO pgshard_catalog_admin;
GRANT INSERT (logical_database_id) ON pgshard_catalog.routing_epochs TO pgshard_catalog_admin;
GRANT INSERT, UPDATE, DELETE ON pgshard_catalog.routing_ranges TO pgshard_catalog_admin;
GRANT INSERT (
    logical_database_id,
    schema_name,
    table_name,
    shard_key_column,
    shard_key_type,
    shard_key_encoding,
    shard_key_collation,
    state
) ON pgshard_catalog.registered_tables TO pgshard_catalog_admin;
GRANT INSERT ON pgshard_catalog.operation_tombstones TO pgshard_catalog_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pgshard_catalog TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.validate_routing_epoch(bigint)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.activate_routing_epoch(uuid, bigint, bigint, bigint)
    TO pgshard_catalog_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog GRANT SELECT ON TABLES TO pgshard_catalog_reader;

COMMIT;
