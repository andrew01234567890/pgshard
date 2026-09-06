-- A login role for the controller, so the component that drives every
-- workflow does not hold the cluster superuser password.
--
-- The router already reaches the catalog this way (0027). The controller was
-- the last component still connecting as the superuser, which meant anything
-- that could read the controller's environment held direct write access to
-- every shard and the catalog, bypassing the router entirely.
--
-- pgshard_system membership gives it the schema it owns and nothing outside
-- it. Everything below that is a capability the barrier needs on the CATALOG
-- GROUP, which the barrier reaches through this same connection rather than
-- through a shard DSN:
--
--   pg_read_all_stats            pg_stat_activity.backend_xid and xact_start
--                                are NULL for other backends without it, and
--                                the barrier counts writers with them -- so
--                                a missing grant here does not fail, it
--                                certifies a restore point while writers are
--                                still running.
--   pg_create_restore_point      the restore point itself.
--   pg_switch_wal                the segment it lands in, archived so the
--                                restore can reach it. Taken in the same
--                                step, and superuser-only for the same
--                                reason pg_create_restore_point is.
--   pg_control_checkpoint        the timeline recorded with it.
--   pg_reload_conf               makes the write pause take effect.
--   ALTER SYSTEM ON PARAMETER    the write pause itself. PostgreSQL 15 added
--     default_transaction_read_only  parameter-level grants, which is what
--                                makes this role possible without superuser.
--
-- The password is not set here. A schema migration is public and the same
-- for every cluster; the operator gives the role the password it generated
-- for that cluster.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_controller') THEN
        CREATE ROLE pgshard_controller LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

GRANT pgshard_system TO pgshard_controller;
GRANT pg_read_all_stats TO pgshard_controller;
GRANT EXECUTE ON FUNCTION pg_create_restore_point(text) TO pgshard_controller;
GRANT EXECUTE ON FUNCTION pg_switch_wal() TO pgshard_controller;
GRANT EXECUTE ON FUNCTION pg_control_checkpoint() TO pgshard_controller;
GRANT EXECUTE ON FUNCTION pg_reload_conf() TO pgshard_controller;
GRANT ALTER SYSTEM ON PARAMETER default_transaction_read_only TO pgshard_controller;
