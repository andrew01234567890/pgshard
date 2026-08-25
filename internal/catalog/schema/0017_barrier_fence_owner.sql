-- An owner token on the write fence: a certified barrier stamps the fence
-- with its token when raising and clears it only when the token still
-- matches, so a barrier whose advisory-lock session died mid-run cannot
-- clear a fence a later barrier has since raised.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_map_generation
    ADD COLUMN write_fence_owner text NOT NULL DEFAULT '';

RESET ROLE;
