-- Schemas of a multi-shard database whose objects all live on its home
-- shard.
--
-- A migration tool that keeps its state in the database it migrates creates
-- that state in a schema of its own: pgroll's init creates the pgroll schema
-- with its tables, functions and event triggers in one transaction. In a
-- database that fans DDL out, every one of those statements was refused, and
-- declaring the whole database local_only is not possible once it holds a
-- sharded table.
--
-- A statement whose object lives in a listed schema runs on the home shard,
-- on the client's own connection and in its own transaction, as every
-- statement of a local_only database does. The tables there are unsharded,
-- so a sharded or reference table cannot be declared in a listed schema, and
-- a schema holding one cannot be listed.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.databases
    ADD COLUMN local_schemas text[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN pgshard.databases.local_schemas IS
    'Schemas whose objects all live on home_shard: their DDL runs on the client''s connection instead of fanning out.';

CREATE FUNCTION pgshard.check_local_schemas_hold_no_distributed_tables() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    offending text;
BEGIN
    IF cardinality(NEW.local_schemas) = 0 THEN
        RETURN NEW;
    END IF;
    -- A reshard drops the listed schemas on every new shard but one, so a
    -- name PostgreSQL or pgshard owns cannot be listed.
    SELECT string_agg(s, ', ' ORDER BY s) INTO offending
      FROM unnest(NEW.local_schemas) s
     WHERE s IS NULL OR s = '' OR s LIKE 'pg\_%' OR s LIKE 'pgshard%' OR s = 'information_schema';
    IF offending IS NOT NULL OR array_position(NEW.local_schemas, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'database % cannot list % in local_schemas: an empty name, or a schema PostgreSQL or pgshard owns',
            NEW.name, coalesce(offending, 'NULL')
            USING ERRCODE = 'raise_exception';
    END IF;
    SELECT string_agg(format('%I.%I', schema_name, table_name), ', ' ORDER BY schema_name, table_name)
      INTO offending
      FROM pgshard.tables
     WHERE database = NEW.name AND placement <> 'unsharded' AND schema_name = ANY (NEW.local_schemas);
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'database % cannot list % in local_schemas: % are sharded or reference tables',
            NEW.name, NEW.local_schemas, offending
            USING ERRCODE = 'raise_exception';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER local_schemas_hold_no_distributed_tables
    BEFORE INSERT OR UPDATE ON pgshard.databases
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_local_schemas_hold_no_distributed_tables();

CREATE FUNCTION pgshard.check_table_placement_against_local_schemas() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    pinned text[];
BEGIN
    IF NEW.placement = 'unsharded' THEN
        RETURN NEW;
    END IF;
    -- FOR SHARE for the reason 0046 gives: listing the schema takes the
    -- database row FOR UPDATE, so whichever of the two runs second sees the
    -- other committed.
    SELECT local_schemas INTO pinned
      FROM pgshard.databases WHERE name = NEW.database FOR SHARE;
    IF NEW.schema_name = ANY (pinned) THEN
        RAISE EXCEPTION 'schema % of database % is listed in local_schemas: every object in it lives on the home shard, so %.% cannot be %',
            NEW.schema_name, NEW.database, NEW.schema_name, NEW.table_name, NEW.placement
            USING ERRCODE = 'raise_exception',
                  HINT = 'remove the schema from local_schemas first';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tables_of_a_local_schema_stay_local
    BEFORE INSERT OR UPDATE ON pgshard.tables
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_table_placement_against_local_schemas();

RESET ROLE;
