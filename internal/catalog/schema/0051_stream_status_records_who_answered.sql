-- pgshard.stream_status records whether the member that answered was the
-- primary.
--
-- A stream is condemned when a slot reads unresumable twice in a row, and
-- the stored row is the first of the two. Gating only the SECOND reading on
-- "not in recovery" leaves the first ungated: a standby reports missing, it
-- is stored, and the next reading from a primary condemns the stream on one
-- primary sighting. The debounce becomes "anyone's prior unresumable row,
-- plus one primary look".
--
-- A standby's answer is still worth recording -- the console shows what each
-- member reports -- but it cannot stand as evidence.
--
-- Existing rows default to false, so nothing written before this migration
-- can serve as the first sighting. The first authoritative reading after an
-- upgrade records itself and the second condemns, which is the same two
-- sightings a fresh cluster needs.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.stream_status ADD COLUMN authoritative boolean NOT NULL DEFAULT false;

RESET ROLE;
