-- A cutover pauses writes on its source set with ALTER SYSTEM SET
-- default_transaction_read_only = on, and every ordinary exit gives it back:
-- the swap lifts it on success and on every retry, the fatal and abort paths
-- lift it, an unwind lifts it, and a controller that crashes mid-swap resumes
-- the step and reaches one of those.
--
-- Deleting the workflow row does not. Nothing is left to run the release,
-- ALTER SYSTEM survives a restart, and the sources refuse every writing
-- transaction with 25006 for good. The range fence has the same shape, but a
-- stuck fence only makes routers buffer and then refuse with a retry hint,
-- while a stuck pause is PostgreSQL refusing writes with no hint at all.
--
-- The pause is now claimed here before it is raised, so a sweep can find one
-- whose workflow is gone or finished. It is a separate column from migrating_by because
-- the fence is raised by workflows that never pause -- a copy, an upgrade --
-- and resetting default_transaction_read_only on a shard pgshard did not
-- pause would undo an operator's own maintenance setting.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_status
    ADD COLUMN write_paused_by uuid;

RESET ROLE;
