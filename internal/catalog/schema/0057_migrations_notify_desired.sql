-- A migration's progress notifies on the desired channel, not the serving
-- one.
--
-- 0011 notified routers of every change to pgshard.migrations through
-- notify_serving_change, so every per-shard step of every migration drew on
-- the serving notification budget -- the budget watchers keep separate so a
-- cutover flip is seen at once (PGS-750). A burst of DDL could hold that
-- flip back by up to a refill period, the window in which a router still
-- sends writes to a retiring source.
--
-- Routers still reload when a migration changes: the desired channel
-- reloads the same snapshot, from a budget that ordinary administration
-- already shares. A rewrite migration's hidden column does not depend on
-- that timing (the applier settles for a snapshot's maximum age first), and
-- a router that ran the DDL refreshes its snapshot before answering.

SET LOCAL ROLE pgshard_system;

DROP TRIGGER notify_serving ON pgshard.migrations;

CREATE FUNCTION pgshard.notify_migration_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('pgshard_desired', TG_TABLE_NAME);
    RETURN NULL;
END
$$;

CREATE TRIGGER notify_desired AFTER INSERT OR UPDATE OR DELETE ON pgshard.migrations
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_migration_change();

RESET ROLE;
