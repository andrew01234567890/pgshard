-- A workflow is driven by one replica at a time.
--
-- Leadership is checked between passes, so a replica that lost the advisory
-- lock during a pass ran that pass to its end -- creating subscriptions,
-- building shadow tables, renaming them -- against shards the new leader was
-- already driving. The claim recorded here is taken when a pass begins and
-- carried by every write it makes, so a workflow taken over stops the old
-- pass at its next step instead of at its next tick.
ALTER TABLE pgshard.workflows ADD COLUMN owner text;
ALTER TABLE pgshard.workflows ADD COLUMN owned_at timestamptz;
