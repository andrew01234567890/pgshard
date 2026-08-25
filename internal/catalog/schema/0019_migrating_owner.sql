-- The write fence a cutover raises on its source set is a shared boolean:
-- any workflow could clear it. Two cutovers of the same source therefore
-- interleave badly - one aborting lifts the fence the other is still
-- relying on, and a write can then commit after that workflow sampled its
-- final LSN but before it flips, so the write is neither copied nor
-- rerouted. The fence now records who raised it, and only they may lift it.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_status
    ADD COLUMN migrating_by uuid;

RESET ROLE;
