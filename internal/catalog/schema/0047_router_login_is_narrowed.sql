-- The router's login stops being a member of pgshard_admin.
--
-- 0027 created this role so that the component terminating untrusted client
-- SQL would not hold the superuser password, and its own comment enumerates
-- what it needs: the roles table including its verifiers, the decision log,
-- the migration queue, the sequence allocator. Then it granted
-- pgshard_admin, which is all of that AND full DML on every desired-state
-- table -- roles, role_members, grants, databases, tables, shard_ranges.
--
-- The router is the most likely thing in the system to be compromised, and
-- pgshard_admin's write access to pgshard.roles is a path from there to a
-- role on every shard. The desired-state boundary added in 0044 refuses the
-- shortest version of that -- a superuser membership -- but a compromised
-- router could still write roles, grants and placements that the controller
-- would then apply everywhere.
--
-- What it holds instead: SELECT on everything, which is what a router does
-- -- it plans against the whole catalog and terminates SCRAM against the
-- verifiers pgshard_reader deliberately cannot see -- and writes on exactly
-- two tables, the decision log it is the coordinator of and the migration
-- queue it enqueues DDL into. Those two grants are already in 0027 and are
-- direct, so they survive losing the membership.

-- Outside pgshard_system, as 0027's matching GRANT was: revoking a role
-- membership needs ADMIN OPTION on it, which the migrating superuser has
-- and pgshard_system does not.
REVOKE pgshard_admin FROM pgshard_router;

-- All of this runs as the migrating superuser rather than as
-- pgshard_system: revoking a role membership needs ADMIN OPTION on it, and
-- granting on every table in the schema needs to cover the ones
-- pgshard_system does not own -- the migration queue's partitions among
-- them.

-- Everything, including pgshard.roles.verifier: pgshard_reader is granted
-- that table by column and the verifier is not among them, because a
-- reader has no business with password material. The router's whole job
-- with it is to terminate SCRAM.
GRANT SELECT ON ALL TABLES IN SCHEMA pgshard TO pgshard_router;

-- And on what later migrations add, so a table created after this one does
-- not silently leave the router unable to plan. The migrations create their
-- objects as pgshard_system, which is what this follows.
ALTER DEFAULT PRIVILEGES FOR ROLE pgshard_system IN SCHEMA pgshard
    GRANT SELECT ON TABLES TO pgshard_router;

-- The sequence allocator is SECURITY DEFINER, so EXECUTE is the whole of
-- it: the router never touches pgshard.sequences itself.
GRANT EXECUTE ON FUNCTION pgshard.allocate_sequence_block(text, integer) TO pgshard_router;
