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
