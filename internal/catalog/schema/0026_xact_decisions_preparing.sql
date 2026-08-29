-- pgshard.xact_decisions is a queue: a row is written when a multi-shard
-- transaction starts preparing, updated when it is decided, and deleted
-- as soon as it completes. At rest almost nothing is in it -- but with no
-- index beyond the primary key, every reader that asks about state has to
-- scan the heap, and the heap tracks bloat rather than live rows.
--
-- That matters exactly when it hurts most. During a burst of in-doubt
-- transactions -- a catalog blip, a coordinator restart, a resolver that
-- was down -- the table grows and every reader degrades at once: the
-- metrics poller on its tick, the resolver on every pass, and the barrier
-- drain check polling count(*) in a loop while writes are fenced across
-- the whole cluster.
--
-- The index is partial on purpose. Only rows still preparing are in it,
-- so it stays the size of what is actually in doubt, and a decided row
-- leaves it on the same update that decides it. The write cost is one
-- index entry per multi-shard transaction, added when it prepares and
-- removed when it commits or aborts.

SET LOCAL ROLE pgshard_system;

CREATE INDEX xact_decisions_preparing_idx
    ON pgshard.xact_decisions (created_at)
    WHERE state = 'preparing';
