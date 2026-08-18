-- Desired-state tables: edited by operators through pgshard_admin.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.databases (
    name               text        PRIMARY KEY,
    default_placement  text        NOT NULL DEFAULT 'unsharded'
                                   CHECK (default_placement IN ('unsharded', 'sharded', 'reference')),
    home_shard         integer     NOT NULL DEFAULT 0,
    desired_generation bigint      NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.tables (
    database           text        NOT NULL REFERENCES pgshard.databases (name) ON DELETE CASCADE,
    schema_name        text        NOT NULL,
    table_name         text        NOT NULL,
    placement          text        NOT NULL CHECK (placement IN ('sharded', 'reference', 'unsharded')),
    shard_key          text,
    hash_version       integer     NOT NULL DEFAULT 1 REFERENCES pgshard.hash_versions (version),
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema_name, table_name),
    CONSTRAINT sharded_tables_need_shard_key CHECK (placement <> 'sharded' OR shard_key IS NOT NULL)
);

CREATE TABLE pgshard.shard_ranges (
    shard_set          text        NOT NULL DEFAULT 'default',
    shard_id           integer     NOT NULL CHECK (shard_id >= 0),
    range              int8range   NOT NULL CHECK (NOT isempty(range)),
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (shard_set, shard_id),
    CONSTRAINT shard_ranges_no_overlap
        EXCLUDE USING gist (shard_set WITH =, range WITH &&) DEFERRABLE INITIALLY DEFERRED
);

CREATE FUNCTION pgshard.check_shard_ranges_cover() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target   text;
    expected bigint;
    r        record;
    targets  text[];
BEGIN
    targets := ARRAY[]::text[];
    IF TG_OP IN ('DELETE', 'UPDATE') THEN targets := targets || OLD.shard_set; END IF;
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.shard_set IS DISTINCT FROM NEW.shard_set) THEN targets := targets || NEW.shard_set; END IF;
    FOREACH target IN ARRAY targets LOOP
    expected := -9223372036854775808;
    FOR r IN
        SELECT shard_id, range FROM pgshard.shard_ranges
        WHERE shard_set = target ORDER BY range
    LOOP
        IF expected IS NULL THEN
            RAISE EXCEPTION 'shard_set % has ranges after an unbounded range (shard %)', target, r.shard_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF lower_inf(r.range) THEN
            IF expected <> -9223372036854775808 THEN
                RAISE EXCEPTION 'shard_set % has an unbounded lower range that is not first (shard %)', target, r.shard_id
                    USING ERRCODE = 'check_violation';
            END IF;
        ELSIF lower(r.range) <> expected THEN
            RAISE EXCEPTION 'shard_set % has a gap or overlap at % (shard % starts at %)', target, expected, r.shard_id, lower(r.range)
                USING ERRCODE = 'check_violation';
        END IF;
        expected := CASE WHEN upper_inf(r.range) THEN NULL ELSE upper(r.range) END;
    END LOOP;
    IF expected IS NOT NULL AND EXISTS (SELECT 1 FROM pgshard.shard_ranges WHERE shard_set = target) THEN
        RAISE EXCEPTION 'shard_set % does not extend to the top of the key space (ends at %)', target, expected
            USING ERRCODE = 'check_violation';
    END IF;
    END LOOP;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER shard_ranges_cover_key_space
    AFTER INSERT OR UPDATE OR DELETE ON pgshard.shard_ranges
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_shard_ranges_cover();

CREATE TABLE pgshard.roles (
    rolname            text        PRIMARY KEY,
    verifier           text,
    attributes         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pgshard.grants (
    id                 bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rolname            text        NOT NULL REFERENCES pgshard.roles (rolname) ON DELETE CASCADE,
    database           text        NOT NULL REFERENCES pgshard.databases (name) ON DELETE CASCADE,
    object_kind        text        NOT NULL,
    object_name        text        NOT NULL,
    privileges         text[]      NOT NULL,
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['databases', 'tables', 'shard_ranges', 'roles', 'grants'] LOOP
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
        IF t = 'roles' THEN
            EXECUTE 'GRANT SELECT (rolname, attributes, desired_generation, updated_at) ON pgshard.roles TO pgshard_reader';
        ELSE
            EXECUTE format('GRANT SELECT ON pgshard.%I TO pgshard_reader', t);
        END IF;
    END LOOP;
END
$$;

GRANT USAGE ON SEQUENCE pgshard.grants_id_seq TO pgshard_admin;

RESET ROLE;
