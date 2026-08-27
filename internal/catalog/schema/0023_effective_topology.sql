-- The shard map is both the desired state a user edits and the effective
-- state routers plan against: pgshard.shard_ranges is what Locate() hashes
-- against, and the shard_sets row that says 'serving' is what picks the set.
-- So an UPDATE of a serving set's ranges moves the boundaries under live
-- traffic with no data having moved, and a single UPDATE of state to
-- 'serving' publishes a set whose groups were never provisioned or copied.
-- Both bypass the whole reshard state machine.
--
-- Separating the two is what the tables mean, not another pair of tables: a
-- set in state 'desired' IS the proposal, and only the controller may move
-- one along the lifecycle or reshape one it has taken over. pgshard_admin
-- keeps everything it needs -- propose a set, shape its ranges, abandon it --
-- and loses only the writes that were never its to make.
--
-- Membership of pgshard_system is the test because that is what the control
-- plane connects with (the controller's --catalog-dsn is documented as such)
-- and what a client session reaching the catalog through the router never
-- has.
SET LOCAL ROLE pgshard_system;

CREATE FUNCTION pgshard.control_plane() RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT pg_catalog.pg_has_role(current_user, 'pgshard_system', 'USAGE')
$$;

CREATE FUNCTION pgshard.check_shard_set_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF pgshard.control_plane() THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'INSERT' AND NEW.state <> 'desired' THEN
        RAISE EXCEPTION 'a shard set may only be proposed in state desired, not %', NEW.state
            USING ERRCODE = 'insufficient_privilege',
                  HINT = 'the controller publishes a set once its groups are provisioned, copied and verified';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION 'shard set % cannot be moved from % to % directly', OLD.shard_set, OLD.state, NEW.state
            USING ERRCODE = 'insufficient_privilege',
                  HINT = 'the controller advances a shard set through its lifecycle; edit the desired shard ranges instead';
    END IF;
    -- A set that has left 'desired' is the controller's entirely, not just
    -- its state column: renaming one would leave its ranges behind under a
    -- name nothing is serving, which is the same publication bug by another
    -- route.
    IF TG_OP = 'UPDATE' AND OLD.state <> 'desired' THEN
        RAISE EXCEPTION 'shard set % is in state % and cannot be edited', OLD.shard_set, OLD.state
            USING ERRCODE = 'insufficient_privilege',
                  HINT = 'propose a new shard set; the controller copies the data and cuts over';
    END IF;
    -- A set that is no longer merely proposed has groups behind it, and
    -- dropping its row is how the ranges become editable again (the
    -- workflow-ownership trigger permits dropping a set whole).
    IF TG_OP = 'DELETE' AND OLD.state <> 'desired' THEN
        RAISE EXCEPTION 'shard set % is in state % and cannot be dropped', OLD.shard_set, OLD.state
            USING ERRCODE = 'insufficient_privilege',
                  HINT = 'cancel the reshard that published it; the controller retires a set it has taken over';
    END IF;
    RETURN NULL;
END
$$;

-- Ranges are the routing boundaries themselves. Once a set has left
-- 'desired' the controller owns its shape, whether or not a workflow is
-- running: the gap between two reshards is exactly when an edit to the
-- serving set would go unnoticed.
CREATE FUNCTION pgshard.check_shard_ranges_proposed() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target text;
    targets text[];
    st     text;
BEGIN
    IF pgshard.control_plane() THEN
        RETURN NULL;
    END IF;
    targets := ARRAY[]::text[];
    IF TG_OP IN ('DELETE', 'UPDATE') THEN targets := targets || OLD.shard_set; END IF;
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.shard_set IS DISTINCT FROM NEW.shard_set) THEN targets := targets || NEW.shard_set; END IF;
    FOREACH target IN ARRAY targets LOOP
        SELECT state INTO st FROM pgshard.shard_sets WHERE shard_set = target;
        -- A set dropped whole in this transaction leaves no row; that is how
        -- a proposal is abandoned, and the lifecycle trigger has already
        -- refused it for any set that had left 'desired'.
        CONTINUE WHEN st IS NULL;
        IF st <> 'desired' THEN
            RAISE EXCEPTION 'shard set % is in state % and its ranges are the effective shard map', target, st
                USING ERRCODE = 'insufficient_privilege',
                      HINT = 'propose a new shard set with the ranges you want; the controller copies the data and cuts over';
        END IF;
    END LOOP;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER shard_sets_lifecycle_is_the_controllers
    AFTER INSERT OR UPDATE OR DELETE ON pgshard.shard_sets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_shard_set_lifecycle();

-- Named to sort ahead of shard_ranges_cover_key_space: "these ranges are not
-- yours to edit" is the answer to give, not "your ranges leave a gap".
CREATE CONSTRAINT TRIGGER shard_ranges_are_the_effective_map
    AFTER INSERT OR UPDATE OR DELETE ON pgshard.shard_ranges
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_shard_ranges_proposed();

RESET ROLE;
