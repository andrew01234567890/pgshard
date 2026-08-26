-- Row-level constraint triggers never fire on TRUNCATE, so truncating the
-- shard map would empty it past every check that guards it: key-space
-- coverage, the ordinal rule, and the ranges an in-flight workflow owns. No
-- legitimate path truncates these tables -- a shard set is removed row by row
-- with DropShardSet -- so refuse outright rather than try to validate it.
SET LOCAL ROLE pgshard_system;

CREATE FUNCTION pgshard.refuse_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'truncating %.% is not supported', TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = 'feature_not_supported',
              HINT = 'delete the rows of one shard set instead; truncation would bypass the coverage, numbering and workflow-ownership checks';
END
$$;

CREATE TRIGGER shard_ranges_no_truncate
    BEFORE TRUNCATE ON pgshard.shard_ranges
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.refuse_truncate();

CREATE TRIGGER shard_sets_no_truncate
    BEFORE TRUNCATE ON pgshard.shard_sets
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.refuse_truncate();

RESET ROLE;
