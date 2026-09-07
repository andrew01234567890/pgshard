-- Which non-built-in functions a scatter may project.
--
-- A scatter that concatenates its shards is only correct for a SCALAR
-- function: an aggregate returns one partial answer per shard, and reporting
-- those as the answer is a silent wrong result. Nothing in a parse tree
-- separates the two -- a user-defined aggregate is a plain FuncCall with no
-- aggregate flags -- so the router refuses every function it cannot name as
-- a PostgreSQL built-in, which takes ST_AsText, uuid_generate_v4, similarity
-- and every other extension scalar with it.
--
-- This table is how an operator says yes. It is desired state edited with
-- normal SQL, like the rest of the control plane, because the router cannot
-- learn it any other way today: CREATE FUNCTION, CREATE AGGREGATE and
-- CREATE EXTENSION are all refused through the router, so there is no DDL
-- path for it to record.
--
-- A name is safe when the database has a scalar row for it and no aggregate
-- row. The schema is recorded for the operator, not matched by the router:
-- resolving an unqualified call against search_path is not something the
-- router does, so a name that is a scalar in one schema and an aggregate in
-- another stays refused. Listing an aggregate is therefore useful on its own
-- -- it is how an operator keeps a name refused that would otherwise be
-- allowed by a scalar row elsewhere in the same database.
SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.functions (
    database text NOT NULL REFERENCES pgshard.databases(name) ON DELETE CASCADE,
    schema   text NOT NULL,
    name     text NOT NULL,
    kind     text NOT NULL CHECK (kind IN ('scalar', 'aggregate')),
    -- Stamped and notified like every other desired-state table, so a
    -- router picks a declaration up on the same path as a placement change.
    desired_generation bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (database, schema, name)
);

CREATE TRIGGER stamp_desired BEFORE INSERT OR UPDATE ON pgshard.functions
    FOR EACH ROW EXECUTE FUNCTION pgshard.stamp_desired_row();
CREATE TRIGGER notify_desired_insert AFTER INSERT ON pgshard.functions
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();
CREATE TRIGGER notify_desired_update AFTER UPDATE ON pgshard.functions
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();
CREATE TRIGGER notify_desired_delete AFTER DELETE ON pgshard.functions
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();

GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.functions TO pgshard_admin;
GRANT SELECT ON pgshard.functions TO pgshard_reader;

RESET ROLE;
