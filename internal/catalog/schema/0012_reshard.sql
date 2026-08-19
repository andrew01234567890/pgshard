-- Shard sets: every shard_ranges.shard_set is one generation of the shard
-- map. The serving set is what routers use; a desired set is a pending
-- reshard whose target groups the operator provisions.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.shard_sets (
    shard_set          text        PRIMARY KEY,
    generation         bigint      NOT NULL UNIQUE CHECK (generation >= 1),
    state              text        NOT NULL DEFAULT 'desired'
                                   CHECK (state IN ('desired', 'provisioning', 'serving', 'retired')),
    desired_generation bigint      NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

INSERT INTO pgshard.shard_sets (shard_set, generation, state) VALUES ('default', 1, 'serving');

INSERT INTO pgshard.shard_sets (shard_set, generation, state)
SELECT shard_set, 1 + row_number() OVER (ORDER BY min(updated_at), shard_set), 'desired'
FROM pgshard.shard_ranges WHERE shard_set <> 'default' GROUP BY shard_set;

CREATE FUNCTION pgshard.register_shard_set() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO pgshard.shard_sets (shard_set, generation, state)
    SELECT NEW.shard_set, coalesce(max(generation), 0) + 1, 'desired' FROM pgshard.shard_sets
    ON CONFLICT (shard_set) DO NOTHING;
    RETURN NEW;
END
$$;

CREATE TRIGGER register_shard_set BEFORE INSERT OR UPDATE OF shard_set ON pgshard.shard_ranges
    FOR EACH ROW EXECUTE FUNCTION pgshard.register_shard_set();

CREATE TRIGGER stamp_desired BEFORE INSERT OR UPDATE ON pgshard.shard_sets
    FOR EACH ROW EXECUTE FUNCTION pgshard.stamp_desired_row();
CREATE TRIGGER notify_desired_insert AFTER INSERT ON pgshard.shard_sets
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();
CREATE TRIGGER notify_desired_update AFTER UPDATE ON pgshard.shard_sets
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();
CREATE TRIGGER notify_desired_delete AFTER DELETE ON pgshard.shard_sets
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_desired_change();

GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.shard_sets TO pgshard_admin;
GRANT SELECT ON pgshard.shard_sets TO pgshard_reader;

RESET ROLE;
