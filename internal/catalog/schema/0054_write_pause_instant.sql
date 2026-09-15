-- A switch drains the writers its source pause cannot stop: every client
-- transaction that began before the pause was confirmed in force, since
-- default_transaction_read_only is read when a transaction starts and one
-- begun earlier may still write. That instant -- the shard's own clock at
-- confirmation -- was kept only in the controller's memory for the pass
-- that raised the pause.
--
-- A controller that died during that drain left the claimed pause standing,
-- and the next pass found it standing and drained without the instant: only
-- transactions that had already written were waited for, and one that began
-- before the pause and had only read so far could write on a source after
-- the positions the targets were checked against.
--
-- The instant is recorded beside the claim, set when the claimed pause is
-- confirmed and cleared whenever the claim is.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_status
    ADD COLUMN write_paused_at timestamptz;

RESET ROLE;
