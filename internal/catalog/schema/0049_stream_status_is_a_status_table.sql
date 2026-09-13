-- pgshard_admin stops holding DML on pgshard.stream_status.
--
-- 0003 settled the rule for status tables: the control plane writes them,
-- everyone else reads them. It applies that to twelve tables by name --
-- database_status, table_status, shard_status, role_status, workflows,
-- migrations, xact_decisions, streams, sequences, restore_points, serving,
-- shard_map_generation -- with REVOKE ALL then GRANT SELECT.
--
-- stream_status was added later, in 0009, in the same GRANT as the desired
-- state it sits beside:
--
--     GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.streams, pgshard.stream_status TO pgshard_admin;
--
-- pgshard.streams is a table an administrator declares streams in, so that
-- half is right. stream_status is not: it is slot state -- wal_status,
-- invalidation_reason, confirmed_flush_lsn, retained_bytes -- written only
-- by the controller (internal/controller/streams.go, over a DSN the flag
-- documents as needing pgshard_system) and read by the admin UI to report
-- whether a stream is healthy and how much WAL its slots are pinning.
--
-- So the writable half of that grant buys nothing and costs the one thing
-- a status table is for: an administrator could make the console report a
-- stream as active and caught up while its slot was invalidated, or hide
-- WAL retention that is filling a disk. Monitoring integrity rather than
-- control -- nothing routes or fences on these rows -- which is why this
-- is a narrowing and not a fix for an escalation.

SET LOCAL ROLE pgshard_system;

REVOKE INSERT, UPDATE, DELETE ON pgshard.stream_status FROM pgshard_admin;

RESET ROLE;
