-- Notify routers when the effective shard map or table placement changes.

SET LOCAL ROLE pgshard_system;

CREATE FUNCTION pgshard.notify_serving_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('pgshard_serving', TG_TABLE_NAME);
    RETURN NULL;
END
$$;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['shard_status', 'table_status', 'shard_map_generation'] LOOP
        EXECUTE format(
            'CREATE TRIGGER notify_serving AFTER INSERT OR UPDATE OR DELETE ON pgshard.%I
             FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_serving_change()', t);
    END LOOP;
END
$$;

RESET ROLE;
