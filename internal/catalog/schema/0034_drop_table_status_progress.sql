-- table_status.progress never had a writer. Every INSERT and UPDATE of the
-- row leaves it at '{}', and the progress an operator actually wants lives
-- in workflows.status, where the workflow that owns it keeps it.
--
-- It was not merely unused. The metrics poller read it as avg(ts.progress),
-- for which PostgreSQL has no aggregate over jsonb, so every refresh failed
-- before it could update the workflow-progress, in-doubt-transaction and
-- migration gauges: telemetry about safety went missing because of a column
-- nothing filled in. That query now derives its value from workflows.status;
-- this drops the column that invited it.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.table_status DROP COLUMN IF EXISTS progress;

RESET ROLE;
