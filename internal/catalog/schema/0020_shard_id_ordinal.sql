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
     CROSS JOIN LATERAL jsonb_array_elements(w.spec->'ranges') WITH ORDINALITY AS r(value, ordinality)
     WHERE w.state NOT IN ('completed', 'failed', 'cancelled')
       AND jsonb_typeof(w.spec->'ranges') = 'array'
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
-- insert alone cannot reshape a set that already covers the key space -- it can
-- only overlap, which the exclusion constraint refuses. Dropping the whole set
-- stays open too: a cancelled reshard and a retirement both clear the
-- shard_sets row in the same transaction.
CREATE FUNCTION pgshard.check_shard_ranges_owned() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target  text;
    targets text[];
    owner   uuid;
BEGIN
    targets := ARRAY[OLD.shard_set];
    IF TG_OP = 'UPDATE' AND NEW.shard_set IS DISTINCT FROM OLD.shard_set THEN
        targets := targets || NEW.shard_set;
    END IF;
    FOREACH target IN ARRAY targets LOOP
        -- Dropping the whole set clears its shard_sets row in the same
        -- transaction; that is how a cancelled reshard and a retirement remove
        -- a set the workflow still owns, so let it through.
        CONTINUE WHEN NOT EXISTS (SELECT 1 FROM pgshard.shard_sets WHERE shard_set = target);
        SELECT id INTO owner FROM pgshard.workflows
         WHERE spec->>'shard_set' = target
           AND state NOT IN ('completed', 'failed', 'cancelled')
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
    AFTER UPDATE OR DELETE ON pgshard.shard_ranges
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pgshard.check_shard_ranges_owned();

RESET ROLE;
