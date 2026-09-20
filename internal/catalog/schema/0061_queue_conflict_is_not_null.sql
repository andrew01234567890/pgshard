-- A placement whose status carries no stage read as NULL in the queue rule,
-- and a NULL conflict is no conflict.
--
-- pgshard.operations computed ddl_hold_ends as
--
--     w.status->>'stage' = 'retiring'
--
-- which is NULL, not false, for a workflow whose status has no stage yet --
-- exactly what a placement looks like the moment it is created. The rule
-- then evaluated "... AND NOT a_hold_ends" to NULL, and the blocker query
-- keeps only rows where the conflict is true, so a fresh placement saw no
-- DDL queued ahead of it and a queued DDL saw no placement.
--
-- The window is small but it is the window that matters: the placer's first
-- act is to describe the table, and a DDL that runs beside it changes the
-- shape it just read.
--
-- Fixed in both places, because either alone leaves the other able to
-- produce a NULL: the view yields a real boolean, and the rule treats an
-- unknown hold as "still holding", which is the safe direction.
CREATE OR REPLACE VIEW pgshard.operations AS
SELECT 'ddl'::text AS kind, m.id, m.arrival,
       array_remove(ARRAY[m.database, m.meta->>'database'], NULL) AS databases,
       pgshard.cluster_scoped_migration(m.kind) AS cluster_scoped,
       m.state = 'running' AS started,
       true AS holds_queue,
       false AS ddl_hold_ends
FROM pgshard.migrations m
WHERE m.state IN ('queued', 'running')
UNION ALL
SELECT w.kind, w.id, w.arrival, NULL::text[], false,
       coalesce(w.status->>'stage', '') NOT IN ('', 'provisioning', 'ready_for_copy'),
       coalesce(w.status->>'stage', '') NOT IN ('', 'provisioning', 'ready_for_copy')
           OR NOT (w.state = 'paused' OR (w.state = 'pending' AND NOT (w.spec ? 'source_set')) OR coalesce(w.status->>'start_error', '') <> ''),
       false
FROM pgshard.workflows w
WHERE w.kind IN ('reshard', 'upgrade')
  AND (w.state IN ('pending', 'provisioning', 'running', 'paused') OR w.status->>'stage' = 'cancelling')
UNION ALL
SELECT w.kind, w.id, w.arrival,
       array_remove(ARRAY[w.spec->>'database'], NULL), false,
       coalesce(w.status->>'stage', '') NOT IN ('', 'preparing')
           AND (w.state IN ('running', 'paused') OR w.status->>'stage' = 'cancelling'),
       (coalesce(w.status->>'stage', '') NOT IN ('', 'preparing')
           AND (w.state IN ('running', 'paused') OR w.status->>'stage' = 'cancelling'))
           OR NOT (w.state = 'paused' OR coalesce(w.status->>'start_error', '') <> ''),
       coalesce(w.status->>'stage', '') = 'retiring'
FROM pgshard.workflows w
WHERE w.kind = 'table_placement'
  AND (w.state IN ('pending', 'provisioning', 'running', 'paused') OR w.status->>'stage' = 'cancelling');

CREATE OR REPLACE FUNCTION pgshard.operations_conflict(
    a_kind text, a_databases text[], a_cluster boolean, a_hold_ends boolean,
    b_kind text, b_databases text[], b_cluster boolean, b_hold_ends boolean) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN a_kind = 'ddl' AND b_kind = 'ddl' THEN a_cluster OR b_cluster OR coalesce(a_databases && b_databases, false)
        WHEN a_kind = 'ddl' AND b_kind = 'table_placement' THEN (a_cluster OR coalesce(a_databases && b_databases, false)) AND NOT coalesce(b_hold_ends, false)
        WHEN a_kind = 'table_placement' AND b_kind = 'ddl' THEN (b_cluster OR coalesce(a_databases && b_databases, false)) AND NOT coalesce(a_hold_ends, false)
        WHEN a_kind = 'table_placement' AND b_kind = 'table_placement' THEN false
        ELSE true
    END
    $$;
