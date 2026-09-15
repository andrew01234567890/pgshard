-- The operation queue as people read it: every unfinished DDL migration,
-- reshard, major upgrade and table placement, in the order they arrived,
-- with a readable command, where it stands, what it waits for, and a
-- progress bar.
--
--   SELECT position, kind, command, state, waiting_for, progress_bar, detail
--   FROM pgshard.operation_queue;
--
-- Two views. operation_queue is for any reader and never carries a
-- statement's text: a migration's statement can hold a password verifier,
-- and another tenant's DDL is not a reader's business, so a DDL entry is
-- named by its kind and object. operation_queue_detail adds the statement,
-- with a role statement's password shown as redacted, for the admin UI and
-- pgshard_admin.

SET LOCAL ROLE pgshard_system;

-- A bar of twenty characters and a percentage, readable in psql.
CREATE FUNCTION pgshard.progress_bar(p numeric) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE WHEN p IS NULL THEN NULL ELSE
        '[' || repeat('#', floor(least(greatest(p, 0), 1) * 20)::int)
            || repeat('-', 20 - floor(least(greatest(p, 0), 1) * 20)::int) || '] '
            || lpad(floor(least(greatest(p, 0), 1) * 100)::int::text, 3) || '%'
    END
    $$;

-- A share of done over total, NULL when there is nothing to count.
CREATE FUNCTION pgshard.fraction(done numeric, total numeric) RETURNS numeric
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$ SELECT CASE WHEN total IS NULL OR total <= 0 THEN NULL ELSE least(greatest(done / total, 0), 1) END $$;

-- A duration as "23h04m", "4m10s" or "12s".
CREATE FUNCTION pgshard.short_interval(i interval) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN i IS NULL THEN NULL
        WHEN extract(epoch FROM i) >= 3600 THEN floor(extract(epoch FROM i) / 3600)::text || 'h' || lpad((floor(extract(epoch FROM i) / 60)::bigint % 60)::text, 2, '0') || 'm'
        WHEN extract(epoch FROM i) >= 60 THEN floor(extract(epoch FROM i) / 60)::text || 'm' || lpad((floor(extract(epoch FROM i))::bigint % 60)::text, 2, '0') || 's'
        ELSE greatest(floor(extract(epoch FROM i)), 0)::text || 's'
    END
    $$;

-- A number from a status key, NULL when the value is not one.
CREATE FUNCTION pgshard.status_number(v jsonb) RETURNS numeric
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$ SELECT CASE WHEN jsonb_typeof(v) = 'number' THEN v::text::numeric END $$;

-- A timestamp from a status key, NULL when the value is not one.
CREATE FUNCTION pgshard.status_time(v text) RETURNS timestamptz
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$ SELECT CASE WHEN v IS NOT NULL AND pg_input_is_valid(v, 'timestamptz') THEN v::timestamptz END $$;

CREATE VIEW pgshard.operation_queue_detail WITH (security_barrier = true) AS
WITH entries AS (
    SELECT 'ddl'::text AS kind, m.id, m.arrival, m.database,
           CASE WHEN m.state = 'running' THEN 'running' ELSE 'queued' END AS base_state,
           NULL::text AS stage,
           m.kind || coalesce(' ' || nullif(concat_ws('.', m.meta->'object'->>'schema', m.meta->'object'->>'name'), ''),
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
                   extract(epoch FROM pgshard.status_time(w.status->'cutover'->>'retire_at') - pgshard.status_time(w.status->'cutover'->>'switched_at'))::numeric), 1)
               WHEN 'completing' THEN 0.99
           END,
           concat_ws(' · ',
               CASE WHEN w.status->>'stage' = 'copying' AND w.status->'progress' ? 'tables_total'
                    THEN 'copying ' || coalesce(w.status->'progress'->>'tables_ready', '0') || '/' || (w.status->'progress'->>'tables_total') || ' tables' END,
               CASE WHEN w.status->>'stage' = 'switched' AND pgshard.status_time(w.status->'cutover'->>'retire_at') > now()
                    THEN 'old groups retire in ' || pgshard.short_interval(pgshard.status_time(w.status->'cutover'->>'retire_at') - now()) END,
               nullif(w.status->>'message', '')),
           w.created_at, pgshard.status_time(w.status->>'started_at'), w.updated_at, pgshard.status_time(w.status->'cutover'->>'retire_at')
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
           coalesce(jsonb_agg(jsonb_build_object('kind', b.blocker_kind, 'id', b.blocker_id, 'reason', b.reason) ORDER BY b.blocker_kind, b.blocker_id)
                    FILTER (WHERE b.blocker_id IS NOT NULL), '[]') AS blockers
    FROM entries e
    LEFT JOIN LATERAL pgshard.operation_blockers(e.kind, e.id) b ON true
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

COMMENT ON VIEW pgshard.operation_queue_detail IS
    'The operation queue with each migration''s statement; see pgshard.operation_queue for the view without it.';

CREATE VIEW pgshard.operation_queue WITH (security_barrier = true) AS
SELECT position, kind, id, database, command, state, stage, waiting_for, blockers, progress, progress_bar, detail,
       created_at, started_at, updated_at, retire_at
FROM pgshard.operation_queue_detail;

COMMENT ON VIEW pgshard.operation_queue IS
    'Every unfinished DDL migration, reshard, major upgrade and table placement, in arrival order: what it is, where it stands, what it waits for, and how far along it is.';

-- The admin UI reads migrations too, and pgshard_reader deliberately cannot
-- (0037): role DDL carries a SCRAM verifier in its statement and meta. This
-- view hands back everything the pages show, with a statement that sets a
-- password replaced by its shape.
CREATE VIEW pgshard.migrations_detail WITH (security_barrier = true) AS
SELECT m.id, m.database,
       CASE WHEN m.meta ? 'verifier' OR m.meta->>'clear_verifier' = 'true' OR m.statement ILIKE '%password%'
            THEN m.kind || coalesce(' ' || (m.meta->>'role'), '') || ' (statement not shown: it sets a password)'
            ELSE m.statement END AS statement,
       m.kind, m.strategy, m.scope, m.home_shard, m.state,
       m.meta - 'verifier' AS meta,
       m.per_shard, coalesce(m.error, '') AS error, m.created_at, m.finished_at, m.started_at, m.arrival
FROM pgshard.migrations m;

COMMENT ON VIEW pgshard.migrations_detail IS
    'pgshard.migrations for the admin UI: no verifier in meta, and a password-setting statement shown by its shape.';

GRANT SELECT ON pgshard.operation_queue TO pgshard_reader, pgshard_admin;
GRANT SELECT ON pgshard.operation_queue_detail, pgshard.migrations_detail TO pgshard_admin;

RESET ROLE;

-- A login for the admin UI: what a reader may see, plus the queue and the
-- migrations as the views above redact them, and nothing else. It is
-- read-only and its statements are bounded, because the UI's queries are
-- the ones a browser can set off. The password is not set here; the
-- operator gives the role the one it generated for that cluster.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgshard_admin_ui') THEN
        CREATE ROLE pgshard_admin_ui LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

GRANT pgshard_reader TO pgshard_admin_ui;
ALTER ROLE pgshard_admin_ui SET default_transaction_read_only = on;
ALTER ROLE pgshard_admin_ui SET statement_timeout = '15s';
ALTER ROLE pgshard_admin_ui CONNECTION LIMIT 8;

SET LOCAL ROLE pgshard_system;
GRANT SELECT ON pgshard.operation_queue_detail, pgshard.migrations_detail TO pgshard_admin_ui;
RESET ROLE;
