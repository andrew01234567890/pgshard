-- The queue names the object of every migration, not only of the few whose
-- object the applier records.
--
-- migrations.meta.object is written for the kinds the applier has to check
-- for when it resumes -- CREATE TABLE, CREATE INDEX, DROP TABLE, the role
-- and database forms -- and for nothing else. ALTER TABLE, COMMENT, ALTER
-- INDEX, ALTER SEQUENCE and the rename and owner forms carry none, so three
-- of them waiting in the queue read as "ALTER TABLE", "ALTER TABLE",
-- "COMMENT": indistinguishable, in the one view that deliberately withholds
-- the statement they would have been told apart by.
--
-- The router now records the name the statement writes as meta.target, for
-- reading only. Migrations queued before this upgrade have none and read as
-- they did.

CREATE OR REPLACE VIEW pgshard.operation_queue_detail WITH (security_barrier = true) AS
WITH entries AS (
    SELECT 'ddl'::text AS kind, m.id, m.arrival, m.database,
           CASE WHEN m.state = 'running' THEN 'running' ELSE 'queued' END AS base_state,
           NULL::text AS stage,
           m.kind || coalesce(' ' || nullif(concat_ws('.', m.meta->'object'->>'schema', m.meta->'object'->>'name'), ''),
                              ' ' || nullif(m.meta->>'target', ''),
                              ' ' || (m.meta->>'role'), ' ' || (m.meta->>'database'), '') AS command,
           CASE WHEN m.meta ? 'verifier' OR m.meta->>'clear_verifier' = 'true' OR m.statement ILIKE '%password%'
                THEN m.kind || coalesce(' ' || (m.meta->>'role'), '') || ' (statement not shown: it sets a password)'
                ELSE left(regexp_replace(btrim(m.statement), '\s+', ' ', 'g'), 500) END AS statement,
           CASE WHEN m.state = 'running' THEN
               least(0.99, pgshard.fraction(
                   (SELECT count(*) FROM jsonb_each(m.per_shard) s WHERE s.value->>'state' IN ('applied', 'skipped', 'failed')),
                   (SELECT count(*) FROM jsonb_each(m.per_shard))))
           ELSE 0 END AS progress,
           CASE WHEN m.state = 'running' THEN
               (SELECT count(*) FILTER (WHERE s.value->>'state' IN ('applied', 'skipped')) || '/' || count(*) || ' shards applied'
                   || coalesce(', ' || nullif(count(*) FILTER (WHERE s.value->>'state' = 'retrying'), 0) || ' retrying', '')
                   || coalesce(', ' || nullif(count(*) FILTER (WHERE s.value->>'state' = 'failed'), 0) || ' failed', '')
                FROM jsonb_each(m.per_shard) s)
           END AS detail,
           m.created_at, m.started_at, m.updated_at, NULL::timestamptz AS retire_at
    FROM pgshard.migrations m
    WHERE m.state IN ('queued', 'running')
    UNION ALL
    SELECT w.kind, w.id, w.arrival, NULL,
           w.state, w.status->>'stage',
           CASE w.kind
               WHEN 'reshard' THEN 'reshard ' || coalesce((w.spec->>'source_shards') || ' to ', 'to ')
                   || coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(w.spec->'ranges') = 'array' THEN w.spec->'ranges' END)::text, '?') || ' shards'
               ELSE 'upgrade PostgreSQL ' || coalesce((w.spec->>'source_pg_major') || ' to ', 'to ') || coalesce(w.spec->>'pg_major', '?')
           END,
           NULL,
           CASE w.status->>'stage'
               WHEN 'provisioning' THEN 0.02
               WHEN 'ready_for_copy' THEN 0.10
               WHEN 'copying' THEN 0.10 + 0.60 * coalesce(pgshard.fraction(pgshard.status_number(w.status->'progress'->'tables_ready'), pgshard.status_number(w.status->'progress'->'tables_total')), 0)
               WHEN 'catch_up_done' THEN 0.70
               WHEN 'awaiting_switch_writes' THEN 0.75
               WHEN 'switching' THEN 0.85
               WHEN 'switched' THEN 0.90 + 0.09 * coalesce(pgshard.fraction(
                   extract(epoch FROM now() - pgshard.status_time(w.status->'cutover'->>'switched_at'))::numeric,
                   extract(epoch FROM pgshard.retire_at(w.status, w.spec) - pgshard.status_time(w.status->'cutover'->>'switched_at'))::numeric), 1)
               WHEN 'completing' THEN 0.99
           END,
           concat_ws(' · ',
               CASE WHEN w.status->>'stage' = 'copying' AND w.status->'progress' ? 'tables_total'
                    THEN 'copying ' || coalesce(w.status->'progress'->>'tables_ready', '0') || '/' || (w.status->'progress'->>'tables_total') || ' tables' END,
               CASE WHEN w.status->>'stage' = 'switched' AND pgshard.retire_at(w.status, w.spec) > now()
                    THEN 'old groups retire in ' || pgshard.short_interval(pgshard.retire_at(w.status, w.spec) - now()) END,
               nullif(w.status->>'message', '')),
           w.created_at, pgshard.status_time(w.status->>'started_at'), w.updated_at, pgshard.retire_at(w.status, w.spec)
    FROM pgshard.workflows w
    WHERE w.kind IN ('reshard', 'upgrade')
      AND (w.state IN ('pending', 'provisioning', 'running', 'paused') OR w.status->>'stage' = 'cancelling')
    UNION ALL
    SELECT w.kind, w.id, w.arrival, w.spec->>'database',
           w.state, w.status->>'stage',
           'move ' || concat_ws('.', w.spec->>'database', w.spec->>'schema_name', w.spec->>'table_name') || ' to '
               || coalesce(w.spec->'to'->>'placement', '?') || coalesce('(' || (w.spec->'to'->>'shard_key') || ')', ''),
           NULL,
           CASE coalesce(w.status->>'stage', 'preparing')
               WHEN 'preparing' THEN 0
               WHEN 'shadow' THEN 0.05
               WHEN 'copying' THEN 0.10 + 0.50 * coalesce(pgshard.fraction(
                   (SELECT count(*) FROM jsonb_each(CASE WHEN jsonb_typeof(w.status->'placement'->'copied') = 'object' THEN w.status->'placement'->'copied' ELSE '{}' END) c WHERE c.value = 'true'),
                   jsonb_array_length(CASE WHEN jsonb_typeof(w.status->'placement'->'sources') = 'array' THEN w.status->'placement'->'sources' ELSE '[]' END)), 0)
               WHEN 'catch_up' THEN 0.65
               WHEN 'buffering' THEN 0.80
               WHEN 'swapping' THEN 0.85
               WHEN 'retiring' THEN 0.95
           END,
           nullif(w.status->>'message', ''),
           w.created_at, pgshard.status_time(w.status->>'started_at'), w.updated_at, NULL
    FROM pgshard.workflows w
    WHERE w.kind = 'table_placement'
      AND (w.state IN ('pending', 'provisioning', 'running', 'paused') OR w.status->>'stage' = 'cancelling')
), waits AS (
    SELECT e.kind, e.id,
           -- No ORDER BY: operation_blockers already returns them oldest
           -- arrival first, and that head is the operation actually holding
           -- the queue -- the one a reader is sent after. Re-sorting them by
           -- (kind, id) named whichever blocker sorted first instead.
           --
           -- WITH ORDINALITY, because a LATERAL set function's row order is
           -- not something an aggregate is entitled to assume.
           coalesce(jsonb_agg(jsonb_build_object('kind', b.blocker_kind, 'id', b.blocker_id, 'reason', b.reason) ORDER BY b.n)
                    FILTER (WHERE b.blocker_id IS NOT NULL AND b.n <= 20), '[]') AS blockers
    FROM entries e
    LEFT JOIN LATERAL pgshard.operation_blockers(e.kind, e.id) WITH ORDINALITY AS b(blocker_kind, blocker_id, reason, n) ON true
    GROUP BY e.kind, e.id
)
SELECT row_number() OVER (ORDER BY e.arrival, e.kind, e.id) AS position,
       e.kind, e.id, e.database, e.command, e.statement,
       CASE
           WHEN e.base_state = 'paused' THEN 'paused'
           WHEN e.stage = 'cancelling' THEN 'cancelling'
           WHEN jsonb_array_length(w.blockers) > 0 THEN 'waiting'
           WHEN e.stage IN ('switched', 'completing') THEN 'retiring'
           WHEN e.stage = 'rolling_back' THEN 'rolling back'
           WHEN e.base_state IN ('pending', 'provisioning', 'queued') AND e.kind <> 'ddl' AND e.stage IS DISTINCT FROM 'provisioning' THEN 'queued'
           ELSE e.base_state
       END AS state,
       e.stage,
       (SELECT string_agg(
                  CASE b->>'kind' WHEN 'ddl' THEN 'migration' WHEN 'table_placement' THEN 'table placement' WHEN 'upgrade' THEN 'major upgrade' ELSE b->>'kind' END
                  || ' ' || (b->>'id') || CASE b->>'reason' WHEN 'started' THEN ' (in progress)' ELSE ' (queued before it)' END, ', ')
        FROM jsonb_array_elements(w.blockers) b) AS waiting_for,
       w.blockers,
       round(e.progress, 4) AS progress,
       pgshard.progress_bar(e.progress) AS progress_bar,
       e.detail,
       e.created_at, e.started_at, e.updated_at, e.retire_at
FROM entries e JOIN waits w USING (kind, id);
