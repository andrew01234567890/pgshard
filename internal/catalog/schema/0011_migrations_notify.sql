-- Notify routers when a migration changes: an in-flight rewrite migration
-- hides its working column, so routers must reload the snapshot when one
-- starts, progresses or finishes.

SET LOCAL ROLE pgshard_system;

CREATE TRIGGER notify_serving AFTER INSERT OR UPDATE OR DELETE ON pgshard.migrations
    FOR EACH STATEMENT EXECUTE FUNCTION pgshard.notify_serving_change();

RESET ROLE;
