-- Global sequences for sharded tables: which columns of a table the router
-- fills from pgshard.sequences, and the atomic block allocator routers call.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.tables ADD COLUMN sequence_columns text[];

GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.sequences TO pgshard_admin;

-- Hands out [block_start, block_end] and moves next_value past it in one
-- row update, creating the sequence on first use. Blocks are never reused,
-- so an unused remainder is a gap, as with PostgreSQL sequences. A NULL n
-- takes the row's block_size.
CREATE FUNCTION pgshard.allocate_sequence_block(seq_name text, n integer DEFAULT NULL)
RETURNS TABLE (block_start bigint, block_end bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    size integer;
BEGIN
    IF n IS NOT NULL AND n <= 0 THEN
        RAISE EXCEPTION 'allocate_sequence_block: block size must be positive, not %', n
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO pgshard.sequences AS s (name)
        VALUES (seq_name)
        ON CONFLICT (name) DO NOTHING;
    UPDATE pgshard.sequences AS s
        SET next_value = s.next_value + coalesce(n, s.block_size)
        WHERE s.name = seq_name
        RETURNING s.next_value - coalesce(n, s.block_size), s.next_value - 1
        INTO block_start, block_end;
    RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION pgshard.allocate_sequence_block(text, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgshard.allocate_sequence_block(text, integer) TO pgshard_admin;

RESET ROLE;
