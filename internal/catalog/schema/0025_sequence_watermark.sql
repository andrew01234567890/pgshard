-- pgshard.sequences carries two different things in one row: block_size,
-- which an operator tunes, and next_value, which is the allocator's
-- watermark. Routers cache the blocks it has handed out, so a next_value
-- that moves backwards -- lowered, or deleted and re-inserted -- hands a
-- second router values another one is already using, and the global
-- uniqueness the allocator advertises is gone with no error anywhere.

SET LOCAL ROLE pgshard_system;

REVOKE UPDATE, DELETE ON pgshard.sequences FROM pgshard_admin;
GRANT UPDATE (block_size) ON pgshard.sequences TO pgshard_admin;

-- The watermark only ever moves forward, whoever moves it: the allocator
-- adds to it, a catalog flip carries the greater of the two, and an
-- operator raises it through advance_sequence. Nothing lowers it.
CREATE FUNCTION pgshard.sequence_watermark_is_monotonic() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF NEW.next_value < OLD.next_value THEN
        RAISE EXCEPTION 'sequence "%" is at % and cannot go back to %: routers hold the blocks below it',
            OLD.name, OLD.next_value, NEW.next_value
            USING ERRCODE = 'check_violation',
                  HINT = 'use pgshard.advance_sequence() to move a sequence forward';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER sequence_watermark_only_moves_forward
    BEFORE UPDATE ON pgshard.sequences
    FOR EACH ROW EXECUTE FUNCTION pgshard.sequence_watermark_is_monotonic();

-- The administrative setval: forward only, and it reports where the
-- sequence ended up so a caller that asked for less can see it was already
-- past that.
CREATE FUNCTION pgshard.advance_sequence(seq_name text, to_value bigint)
RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    now_at bigint;
BEGIN
    UPDATE pgshard.sequences AS s
        SET next_value = greatest(s.next_value, to_value)
        WHERE s.name = seq_name
        RETURNING s.next_value INTO now_at;
    IF now_at IS NULL THEN
        RAISE EXCEPTION 'sequence "%" does not exist', seq_name USING ERRCODE = 'no_data_found';
    END IF;
    RETURN now_at;
END
$$;

REVOKE ALL ON FUNCTION pgshard.advance_sequence(text, bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgshard.advance_sequence(text, bigint) TO pgshard_admin;

RESET ROLE;
