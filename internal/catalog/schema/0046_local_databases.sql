-- A database whose objects all live on its home shard.
--
-- pgshard routes DDL through a catalog migration that a controller applies
-- to every shard in that shard's own transaction. Because that fan-out is
-- not atomic it cannot be rolled back with the client's transaction, so DDL
-- inside BEGIN/COMMIT is refused -- and a whole family of statements that
-- cannot be fanned out sensibly (CREATE FUNCTION, CREATE TRIGGER, COMMENT,
-- CREATE EVENT TRIGGER) is refused outright.
--
-- None of those refusals are about the statement. They are about there
-- being more than one place to run it. A database declared local has ONE
-- place: every object in it lives on the home shard, so its DDL is ordinary
-- PostgreSQL, in the client's own transaction, with PostgreSQL's own
-- rollback -- which is what a migration tool that keeps its state in the
-- database it migrates (pgroll, sqitch, atlas, flyway) needs in order to
-- work at all.
--
-- The declaration is explicit rather than inferred from "has no sharded
-- tables yet": inferring it would silently change what DDL means the moment
-- somebody declared a sharded table, and leave the DDL that ran before that
-- point unrecorded.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.databases
    ADD COLUMN local_only boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN pgshard.databases.local_only IS
    'Every object lives on home_shard: DDL runs on the client''s connection instead of fanning out.';

-- A local database cannot hold a table that is anywhere but the home shard,
-- in either direction: the declaration and the tables have to agree at all
-- times, not only when the declaration is made.
CREATE FUNCTION pgshard.check_local_database_has_no_distributed_tables() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    offending text;
BEGIN
    IF NOT NEW.local_only THEN
        RETURN NEW;
    END IF;
    IF NEW.default_placement <> 'unsharded' THEN
        RAISE EXCEPTION 'database % is declared local_only, so its default placement must be unsharded, not %',
            NEW.name, NEW.default_placement
            USING ERRCODE = 'raise_exception';
    END IF;
    SELECT string_agg(format('%I.%I', schema_name, table_name), ', ' ORDER BY schema_name, table_name)
      INTO offending
      FROM pgshard.tables
     WHERE database = NEW.name AND placement <> 'unsharded';
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'database % cannot be declared local_only: % are sharded or reference tables',
            NEW.name, offending
            USING ERRCODE = 'raise_exception';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER local_databases_hold_no_distributed_tables
    BEFORE INSERT OR UPDATE ON pgshard.databases
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_local_database_has_no_distributed_tables();

CREATE FUNCTION pgshard.check_table_placement_against_local_database() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.placement = 'unsharded' THEN
        RETURN NEW;
    END IF;
    IF EXISTS (SELECT 1 FROM pgshard.databases WHERE name = NEW.database AND local_only) THEN
        RAISE EXCEPTION 'database % is declared local_only: every object in it lives on its home shard, so %.% cannot be %',
            NEW.database, NEW.schema_name, NEW.table_name, NEW.placement
            USING ERRCODE = 'raise_exception',
                  HINT = 'clear local_only on the database first; the DDL that ran on it was never recorded as migrations';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tables_of_a_local_database_stay_local
    BEFORE INSERT OR UPDATE ON pgshard.tables
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_table_placement_against_local_database();
