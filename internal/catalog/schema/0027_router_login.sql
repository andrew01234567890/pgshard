-- A login role for the router, so the component that terminates untrusted
-- client connections does not hold the cluster superuser password. It needs
-- what pgshard_admin already has -- the roles table including its
-- verifiers, the decision log, the migration queue, the sequence allocator
-- -- and nothing beyond it: no CREATEDB, no CREATEROLE, no superuser, and
-- no reach outside the pgshard schema.
--
-- The password is not set here. A schema migration is public and the same
-- for every cluster; the operator gives the role the password it generated
-- for that cluster.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_router') THEN
        CREATE ROLE pgshard_router LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

GRANT pgshard_admin TO pgshard_router;

-- pgshard_admin carries the desired-state tables and the sequence
-- allocator. The router also writes two status tables: the decision log it
-- is the coordinator of, and the migration queue it enqueues DDL into. It
-- reads both back, which pgshard_reader already covers.
GRANT pgshard_reader TO pgshard_router;

SET LOCAL ROLE pgshard_system;
GRANT INSERT, UPDATE, DELETE ON pgshard.xact_decisions TO pgshard_router;
GRANT INSERT, UPDATE ON pgshard.migrations TO pgshard_router;
RESET ROLE;
