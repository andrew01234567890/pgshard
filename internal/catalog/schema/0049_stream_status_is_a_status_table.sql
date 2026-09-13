-- pgshard_admin stops holding DML on the two stream tables.
--
-- 0003 created pgshard.streams and pgshard.stream_status and listed both
-- among the status tables it revoked admin DML on: the control plane writes
-- them, everyone else reads them. 0009 then re-granted both in one
-- statement while adding stream_status's columns:
--
--     GRANT SELECT, INSERT, UPDATE, DELETE ON pgshard.streams, pgshard.stream_status TO pgshard_admin;
--
-- Nothing needs it. Streams are declared through Controller.CreateStream
-- and DropStream (internal/controller/streamadmin.go), which write as
-- pgshard_system, and the rows are read by the admin console to report
-- whether a stream is healthy and how much WAL its slots pin.
--
-- What the grant costs is what a status table is for. streams.state alone
-- decides the console's "lost" verdict, so an administrator could mark a
-- healthy stream lost, or -- worse -- report a stream active while its
-- slots were invalidated and their WAL was filling a disk. Monitoring
-- integrity rather than control: nothing routes or fences on these rows.

SET LOCAL ROLE pgshard_system;

REVOKE INSERT, UPDATE, DELETE ON pgshard.streams, pgshard.stream_status FROM pgshard_admin;

RESET ROLE;
