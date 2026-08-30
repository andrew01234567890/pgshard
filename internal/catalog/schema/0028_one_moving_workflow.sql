-- Two workflows moving data out of the SAME serving set can retire each
-- other's source: the second flips onto a set the first has already
-- retired, or publishes a second serving set. The cutover refuses that when
-- it reaches the flip, which is late -- by then both have provisioned
-- groups, copied data and fenced writes.
--
-- The rule is per source, not per cluster, because that is what the hazard
-- is: workflows out of different sources cannot retire each other, and a
-- cluster-wide rule would queue every newly declared shard set behind
-- unrelated work. An in-place reshard has no source_set -- its source and
-- target are one set -- so it is outside the rule entirely, which matters
-- because the reconciler recreates in-place work on every pass and a
-- queue behind it would never drain.
CREATE UNIQUE INDEX workflows_one_moving_per_source ON pgshard.workflows ((spec->>'source_set'))
    WHERE kind IN ('reshard', 'upgrade')
      AND state IN ('pending', 'provisioning', 'running', 'paused')
      AND spec->>'source_set' IS NOT NULL
      AND spec->>'source_set' IS DISTINCT FROM spec->>'shard_set';
