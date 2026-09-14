-- One definition of the desired roles generation.
--
-- It is the max of desired_generation over the four desired-state tables
-- roles materialization reads -- roles, role_members, grants, role_settings
-- -- which one sequence stamps. The max over ONE of them is a generation
-- the controller passed some time ago, not the one it is working towards.
--
-- The expression was written out in four places and had silently diverged
-- in three: two e2e helpers and a controller test each waited on
-- pgshard.roles alone. In TestRouterRolesAndGrants that made
-- awaitRolesMaterialized return before the pass it waits for had started
-- (PGS-802). Each copy read correctly on its own; the divergence was only
-- visible from all four at once, and a fifth desired-state table would
-- reintroduce it in whichever copies were not updated.
--
-- No GRANT: it is SECURITY INVOKER, so the four tables' own privileges are
-- the boundary, exactly as they are for the query it replaces.
--
-- Written as a standard SQL body (RETURN, not a quoted string) so the four
-- tables are recorded in pg_depend: the function then cannot outlive a
-- table it reads, and a test can ask what it reads.

SET LOCAL ROLE pgshard_system;

CREATE FUNCTION pgshard.roles_desired_generation() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT coalesce(max(g), 0) FROM (
    SELECT max(desired_generation) g FROM pgshard.roles
    UNION ALL SELECT max(desired_generation) FROM pgshard.role_members
    UNION ALL SELECT max(desired_generation) FROM pgshard.grants
    UNION ALL SELECT max(desired_generation) FROM pgshard.role_settings) m);

RESET ROLE;
