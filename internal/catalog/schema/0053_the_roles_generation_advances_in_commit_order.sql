-- The roles generation has to advance in COMMIT order, and a sequence does
-- not.
--
-- desired_generation is stamped by nextval in a BEFORE ROW trigger (0001),
-- and a sequence is not transactional: a row takes its number when it is
-- written, not when it is committed. So two overlapping writers commit out
-- of stamp order, and a reader between the two commits sees a generation
-- that the lower-numbered, still-uncommitted row will never exceed:
--
--     W1: BEGIN; INSERT INTO pgshard.roles ...;   -- stamped 2, uncommitted
--     W2:        INSERT INTO pgshard.roles ...;   -- stamped 3, committed
--     reader:    roles_desired_generation() -> 3
--     W1: COMMIT;
--     reader:    roles_desired_generation() -> 3, and w1 is stamped 2
--
-- RoleVerifier.MaterializeStale skips a group recorded at 3, so W1's row is
-- never materialized on it -- not late, never, until an unrelated write
-- stamps 4. That is the symptom the generation exists to prevent, and it is
-- silent: recovery falls to the verifier's drift check (PGS-813).
--
-- The fix is the LOCK, not the counter. An AFTER STATEMENT trigger on each
-- of the four tables roles materialization reads updates one shared row, so
-- a writer that reaches it first blocks every other writer on those tables
-- until it commits. Overlapping writes to them can no longer become visible
-- out of the order they were serialised in, which is what the generation
-- needed all along -- and it is a property of the mechanism now, not of
-- timing. (The max over stamps would be safe too once that lock exists. The
-- singleton is what the lock is ON; reading it is then simply cheaper than
-- four aggregates, and it also cannot go backwards when the newest-stamped
-- row is deleted, which the max could.)
--
-- The cost is that writes to the four tables serialise on one row. They are
-- rare: role and grant edits from the applier, which already serialises
-- them, plus the operator's catalog probe and the router's bootstrap, which
-- write single autocommit statements.
--
-- The sharp edge is a human one. An operator who leaves `BEGIN; INSERT INTO
-- pgshard.roles ...` open now blocks every other writer of these four
-- tables for as long as the session sits there -- previously it blocked
-- nothing. On a catalog the operator tunes that is bounded by
-- idle_in_transaction_session_timeout (10min from pgtune); on an external
-- one it is not bounded at all. A lost materialization is still worth more
-- than that, but it is a real change in what an idle session costs.
--
-- desired_generation stays exactly as it is. It still stamps every row,
-- still drives NOTIFY payloads, and is still what catalog.Generations reads;
-- this changes only what the roles generation means.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.roles_generation (
    only_row boolean PRIMARY KEY DEFAULT true CHECK (only_row),
    generation bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Seeded above BOTH old scales. The new counter counts statements and the
-- old one counted sequence values, so they are unrelated numbers, and a
-- group already recorded in role_group_status at the old scale must not
-- read as ahead of the new one -- that would skip it forever, which is the
-- bug being fixed. Seeding above it costs one materialization pass on
-- upgrade instead, and materialization is idempotent.
INSERT INTO pgshard.roles_generation (generation)
SELECT 1 + greatest(
    (SELECT coalesce(max(g), 0) FROM (
        SELECT max(desired_generation) g FROM pgshard.roles
        UNION ALL SELECT max(desired_generation) FROM pgshard.role_members
        UNION ALL SELECT max(desired_generation) FROM pgshard.grants
        UNION ALL SELECT max(desired_generation) FROM pgshard.role_settings) m),
    (SELECT coalesce(max(roles_generation), 0) FROM pgshard.role_group_status));

-- SECURITY DEFINER because the writers of the four tables must not be able
-- to move the generation by hand: pgshard_admin has no UPDATE on the
-- singleton, and this is the only thing that writes it.
CREATE FUNCTION pgshard.bump_roles_generation() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    UPDATE pgshard.roles_generation
       SET generation = generation + 1, updated_at = now();
    RETURN NULL;
END
$$;

REVOKE ALL ON FUNCTION pgshard.bump_roles_generation() FROM PUBLIC;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['roles', 'role_members', 'grants', 'role_settings'] LOOP
        EXECUTE format(
            'CREATE TRIGGER bump_roles_generation AFTER INSERT OR UPDATE OR DELETE ON pgshard.%I
             FOR EACH STATEMENT EXECUTE FUNCTION pgshard.bump_roles_generation()', t);
    END LOOP;
END
$$;

-- One definition still (0052): it just reads a different thing.
CREATE OR REPLACE FUNCTION pgshard.roles_desired_generation() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT generation FROM pgshard.roles_generation);

GRANT SELECT ON pgshard.roles_generation TO pgshard_admin, pgshard_reader;

RESET ROLE;
