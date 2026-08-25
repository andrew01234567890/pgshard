-- Routing is positional: catalog rows become a placement.RangeSet indexed by
-- key order and the recorded shard_id is dropped, so a set numbered any other
-- way is silently renumbered rather than rejected. Two rules close that:
-- shard IDs are their range's position in key order, and a set a workflow has
-- already snapshotted cannot have its ranges rewritten underneath it.
SET LOCAL ROLE pgshard_system;

LOCK TABLE pgshard.shard_ranges IN SHARE MODE;

DO $$
DECLARE
    bad text;
BEGIN
    SELECT string_agg(format('%s (shard %s sits at position %s)', shard_set, shard_id, pos), ', ' ORDER BY shard_set, pos)
      INTO bad
      FROM (SELECT shard_set, shard_id,
                   row_number() OVER (PARTITION BY shard_set ORDER BY range) - 1 AS pos
              FROM pgshard.shard_ranges) t
     WHERE shard_id <> pos;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'shard sets are not numbered 0..N-1 in key order: %', bad
            USING ERRCODE = 'check_violation',
                  HINT = 'routing has been using each range''s position, not its recorded shard_id, so these sets are already mis-numbered; renumber them in one transaction before applying this migration';
    END IF;

    -- A workflow snapshots its target set's ranges into spec when it is
    -- created, and the copier dials the shards that snapshot names. Renumbering
    -- shard_ranges alone would leave an in-flight workflow addressing shards
    -- that no longer exist, so a mis-numbered snapshot has to be cancelled
    -- rather than repaired underneath the workflow.
    SELECT string_agg(format('%s (workflow %s names shard %s at position %s)', w.spec->>'shard_set', w.id, r.value->>'shard_id', r.ordinality - 1), ', ' ORDER BY w.id)
      INTO bad
      FROM pgshard.workflows w
     CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(w.spec->'ranges') = 'array' THEN w.spec->'ranges' ELSE '[]'::jsonb END) WITH ORDINALITY AS r(value, ordinality)
     WHERE w.state IN ('provisioning', 'running', 'paused')
       AND ((r.value->>'shard_id') IS NULL OR (r.value->>'shard_id')::int <> r.ordinality - 1);
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'workflows still in flight hold mis-numbered shard ranges: %', bad
            USING ERRCODE = 'check_violation',
                  HINT = 'cancel these workflows before applying this migration; renumbering pgshard.shard_ranges alone would leave them addressing shards that no longer exist';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION pgshard.check_shard_ranges_cover() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target   text;
    expected bigint;
    ordinal  integer;
    r        record;
    targets  text[];
BEGIN
    targets := ARRAY[]::text[];
    IF TG_OP IN ('DELETE', 'UPDATE') THEN targets := targets || OLD.shard_set; END IF;
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.shard_set IS DISTINCT FROM NEW.shard_set) THEN targets := targets || NEW.shard_set; END IF;
    FOREACH target IN ARRAY targets LOOP
    expected := -9223372036854775808;
    ordinal := 0;
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
        IF r.shard_id <> ordinal THEN
            RAISE EXCEPTION 'shard_set % must number shards 0..N-1 in key order (position % holds shard %)', target, ordinal, r.shard_id
                USING ERRCODE = 'check_violation';
        END IF;
        expected := CASE WHEN upper_inf(r.range) THEN NULL ELSE upper(r.range) END;
        ordinal := ordinal + 1;
    END LOOP;
    IF expected IS NOT NULL AND EXISTS (SELECT 1 FROM pgshard.shard_ranges WHERE shard_set = target) THEN
        RAISE EXCEPTION 'shard_set % does not extend to the top of the key space (ends at %)', target, expected
            USING ERRCODE = 'check_violation';
    END IF;
    END LOOP;
    RETURN NULL;
END
$$;

-- A reshard or upgrade workflow snapshots its target set's ranges when it is
-- created, and the operator sizes the target groups from the same rows. Those
-- rows stay the workflow's until it reaches a terminal state -- which is well
-- after the cutover flips the set to 'serving', because the workflow holds the
-- reverse subscription open for the whole rollback and retirement window. So
-- the freeze follows workflow ownership rather than the set's own state.
-- Inserts stay open because that is how a set is first materialized, and an
-- Dropping a whole set stays open, because a cancelled reshard clears the
-- shard_sets row and every one of its ranges in the same transaction; both
-- halves are required, so clearing only the row is not a way in. Inserts are
-- covered as well: a set can otherwise be dropped and re-created reshaped under
-- the same name in two transactions, which would put the workflow back exactly
-- where this trigger exists to stop it. First materialization is unaffected
-- because a set's ranges are always written before its workflow exists.
--
-- The source set is owned as well, because every cutover pass rebuilds the
-- source shard IDs and ranges from live catalog rows rather than from the
-- snapshot, so reshaping the source mid-run would fence, drain and verify
-- against shards the copy never established.
--
-- A pending workflow owns nothing: nothing reads its snapshot -- the copier
-- takes only running workflows -- and nothing drives it to a terminal state, so
-- treating it as an owner would freeze a set with no way to release it.
CREATE FUNCTION pgshard.check_shard_ranges_owned() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target  text;
    targets text[];
    owner   uuid;
BEGIN
    targets := ARRAY[]::text[];
    IF TG_OP IN ('DELETE', 'UPDATE') THEN targets := targets || OLD.shard_set; END IF;
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.shard_set IS DISTINCT FROM NEW.shard_set) THEN targets := targets || NEW.shard_set; END IF;
    FOREACH target IN ARRAY targets LOOP
        -- Dropping the whole set clears its shard_sets row and every one of its
        -- ranges in the same transaction; that is how a cancelled reshard and a
        -- retirement remove a set the workflow still owns, so let it through.
        -- Both halves are required: dropping only the shard_sets row would
        -- otherwise be a way to reshape the ranges and restore the row after.
        CONTINUE WHEN NOT EXISTS (SELECT 1 FROM pgshard.shard_sets WHERE shard_set = target)
                  AND NOT EXISTS (SELECT 1 FROM pgshard.shard_ranges WHERE shard_set = target);
        SELECT id INTO owner FROM pgshard.workflows
         WHERE kind IN ('reshard', 'upgrade')
           AND (spec->>'shard_set' = target
                OR spec->>'source_set' = target
                -- A workflow created before the source was recorded in the spec
                -- resolves it on its first cutover pass and keeps it here.
                OR status->'cutover'->>'source_set' = target)
           AND (state IN ('provisioning', 'running')
                -- A workflow paused out of 'pending' never started, so pausing
                -- one must not be a way to freeze a set that was editable.
                OR (state = 'paused' AND status->>'paused_from' IS DISTINCT FROM 'pending'))
         LIMIT 1;
        IF owner IS NOT NULL THEN
            RAISE EXCEPTION 'shard_set % has its ranges owned by workflow %', target, owner
                USING ERRCODE = 'check_violation',
                      HINT = 'cancel the reshard or upgrade workflow before changing this shard set';
        END IF;
    END LOOP;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER shard_ranges_owned_by_workflow
    AFTER INSERT OR UPDATE OR DELETE ON pgshard.shard_ranges
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_shard_ranges_owned();

RESET ROLE;
