-- Three catalog objects nothing owns.
--
-- database_status is read and written by nothing outside its own creation;
-- streams.spec and streams.position were replaced by the normalised columns
-- added in 0009 and have been default '{}' ever since. Every one of them
-- advertises state that is never authoritative: a reader has no way to tell
-- an empty jsonb that means "nothing yet" from one that means "nobody
-- writes this", and both are replicated with the catalog and carried
-- through every upgrade.
--
-- Dropped rather than documented as unused, because the next reader will
-- not read the documentation.
DROP TABLE IF EXISTS pgshard.database_status;
ALTER TABLE pgshard.streams DROP COLUMN IF EXISTS spec;
ALTER TABLE pgshard.streams DROP COLUMN IF EXISTS position;
