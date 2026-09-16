-- One queue for the operations that change the shape of the cluster: DDL
-- migrations, reshards and major upgrades, and table placement moves.
--
-- Each of them used to decide on its own whether the others were in its
-- way, by counting what was running: a reshard started its copy when no
-- migration was running, and so overtook one that was queued; the router
-- refused DDL outright while a reshard provisioned; a migration started
-- again as soon as a reshard switched, and broke its reverse replication;
-- nothing kept DDL off a table a placement was moving. The order they
-- arrived in was not recorded anywhere, so none of them could wait its
-- turn.
--
-- Now every operation has an arrival, and one rule decides whether it may
-- start: nothing that conflicts with it has started and not finished, and
-- nothing that conflicts with it arrived earlier, is unfinished, and holds
-- its place. A started operation never waits, and one that has not started
-- waits only on started ones or earlier ones, so the waits cannot form a
-- cycle. Every gate asks operation_blockers, under the move gate lock, in
-- the statement or transaction that starts the operation.

SET LOCAL ROLE pgshard_system;

-- A sequence, not created_at: the clock can step back, and a transaction's
-- now() is when it began, not when its row became visible. Its value is
-- carried across a catalog major upgrade with the schema's other sequences.
CREATE SEQUENCE pgshard.operation_arrival;

-- Every role that inserts a migration, a workflow or a shard set draws its
-- arrival: the router enqueueing DDL, and an administrator declaring a set
-- through shard_ranges.
GRANT USAGE ON SEQUENCE pgshard.operation_arrival TO pgshard_router, pgshard_admin;

-- Existing rows take their arrival from the default as the table is
-- rewritten, then are renumbered in the order they were created, last of
-- all so that no ALTER or CREATE INDEX follows an UPDATE with trigger
-- events pending. A shard set is not renumbered: an update of one stamps a
-- new desired generation. Its arrival records when it was declared; the
-- reshard workflow draws its own when the controller creates it.
ALTER TABLE pgshard.migrations
    ADD COLUMN arrival    bigint NOT NULL DEFAULT nextval('pgshard.operation_arrival'),
    ADD COLUMN dedup_key  text,
    ADD COLUMN started_at timestamptz;
ALTER TABLE pgshard.workflows
    ADD COLUMN arrival bigint NOT NULL DEFAULT nextval('pgshard.operation_arrival');
ALTER TABLE pgshard.shard_sets
    ADD COLUMN arrival bigint DEFAULT nextval('pgshard.operation_arrival');

CREATE INDEX migrations_unfinished ON pgshard.migrations (arrival) WHERE state IN ('queued', 'running');
CREATE INDEX migrations_dedup ON pgshard.migrations (dedup_key, arrival) WHERE dedup_key IS NOT NULL;

-- Role and database statements are about the whole cluster: a GRANT in any
-- database can name the role a CREATE ROLE makes, so they keep their order
-- against DDL in every database.
CREATE FUNCTION pgshard.cluster_scoped_migration(kind text) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$ SELECT kind IN ('CREATE ROLE', 'ALTER ROLE', 'DROP ROLE', 'GRANT ROLE', 'REVOKE ROLE', 'CREATE DATABASE', 'DROP DATABASE') $$;

-- One row per unfinished operation, with what the rule reads.
--
-- started: a migration once running; a reshard or upgrade once past
-- ready_for_copy, in any state, because a paused copy's subscriptions keep
-- applying; a placement once past preparing and still running, paused or
-- cleaning up after a cancel -- the same test the move gate used.
--
-- unfinished: a workflow cancelled while it still cleans up (stage
-- cancelling) is unfinished: its shadows and replication objects are still
-- there.
--
-- holds_queue: whether an operation that has not started keeps later ones
-- behind it. A paused one does not, nor a reshard row nothing drives (an
-- in-place edit of shard_ranges, which has no source set), nor one whose own
-- pre-start step keeps failing: each of those would hold every later
-- operation indefinitely. Once it goes on, it waits for whatever started
-- meanwhile, which is an order inversion and safe.
--
-- ddl_hold_ends: a placement that has swapped holds no more DDL.
CREATE VIEW pgshard.operations AS
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
       w.status->>'stage' = 'retiring'
FROM pgshard.workflows w
WHERE w.kind = 'table_placement'
  AND (w.state IN ('pending', 'provisioning', 'running', 'paused') OR w.status->>'stage' = 'cancelling');

COMMENT ON VIEW pgshard.operations IS
    'Unfinished DDL migrations, reshards, upgrades and table placements, with what the queue rule reads. See operation_blockers.';

-- Whether two operations may not overlap.
--
-- DDL against a reshard or upgrade: always, since the copy covers every
-- database and logical replication carries no DDL. DDL against a
-- placement: in the placement's database, until it has swapped. DDL
-- against DDL: in the same database, or when either is about roles or
-- databases. Reshards and upgrades against each other and against
-- placements: always. Placements against each other: never, because the
-- per-table workflow lock already keeps two off one table.
CREATE FUNCTION pgshard.operations_conflict(
    a_kind text, a_databases text[], a_cluster boolean, a_hold_ends boolean,
    b_kind text, b_databases text[], b_cluster boolean, b_hold_ends boolean) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN a_kind = 'ddl' AND b_kind = 'ddl' THEN a_cluster OR b_cluster OR coalesce(a_databases && b_databases, false)
        WHEN a_kind = 'ddl' AND b_kind = 'table_placement' THEN (a_cluster OR coalesce(a_databases && b_databases, false)) AND NOT b_hold_ends
        WHEN a_kind = 'table_placement' AND b_kind = 'ddl' THEN (b_cluster OR coalesce(a_databases && b_databases, false)) AND NOT a_hold_ends
        WHEN a_kind = 'table_placement' AND b_kind = 'table_placement' THEN false
        ELSE true
    END
    $$;

-- What operation (p_kind, p_id) waits for, oldest first: every other
-- unfinished operation it conflicts with that has started, or that arrived
-- before it and holds its place. Empty for an operation that has started or
-- finished. It returns ids and reasons, never statement text.
CREATE FUNCTION pgshard.operation_blockers(p_kind text, p_id uuid)
    RETURNS TABLE (blocker_kind text, blocker_id uuid, reason text)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
    AS $$
    SELECT b.kind, b.id, CASE WHEN b.started THEN 'started' ELSE 'earlier' END
    FROM pgshard.operations a
    JOIN pgshard.operations b ON NOT (b.kind = a.kind AND b.id = a.id)
    WHERE a.kind = p_kind AND a.id = p_id AND NOT a.started
      AND pgshard.operations_conflict(a.kind, a.databases, a.cluster_scoped, a.ddl_hold_ends,
                                      b.kind, b.databases, b.cluster_scoped, b.ddl_hold_ends)
      AND (b.started OR (b.arrival < a.arrival AND b.holds_queue))
    ORDER BY b.arrival, b.kind, b.id
    $$;

-- What a statement run directly on a database's home shard would overlap:
-- DDL on a local database or in a local schema never becomes a migration,
-- so it cannot wait its turn, and runs only when no reshard or upgrade has
-- work left and no placement is moving a table of that database.
CREATE FUNCTION pgshard.home_ddl_blockers(p_database text)
    RETURNS TABLE (blocker_kind text, blocker_id uuid, reason text)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
    AS $$
    SELECT b.kind, b.id, CASE WHEN b.started THEN 'started' ELSE 'earlier' END
    FROM pgshard.operations b
    WHERE (b.kind IN ('reshard', 'upgrade') AND (b.started OR b.holds_queue))
       OR (b.kind = 'table_placement' AND p_database = ANY (b.databases) AND NOT b.ddl_hold_ends)
    ORDER BY b.arrival, b.kind, b.id
    $$;

WITH ordered AS (
    SELECT tag, id, row_number() OVER (ORDER BY created_at, arrival, tag, id) AS n
    FROM (SELECT 1 AS tag, id, created_at, arrival FROM pgshard.migrations
          UNION ALL SELECT 2, id, created_at, arrival FROM pgshard.workflows) all_rows
), renumbered AS (
    UPDATE pgshard.migrations t SET arrival = o.n FROM ordered o WHERE o.tag = 1 AND o.id = t.id
)
UPDATE pgshard.workflows t SET arrival = o.n FROM ordered o WHERE o.tag = 2 AND o.id = t.id;

-- The rewrite drew at least as many values as there were rows, so the
-- sequence is already past every renumbered arrival and every shard set's;
-- it is left where it is. Setting it to the migrations' and workflows'
-- maximum would hand the next operations the arrivals the shard sets hold.

REVOKE ALL ON FUNCTION pgshard.operation_blockers(text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgshard.home_ddl_blockers(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgshard.operation_blockers(text, uuid) TO pgshard_reader, pgshard_admin, pgshard_router;
GRANT EXECUTE ON FUNCTION pgshard.home_ddl_blockers(text) TO pgshard_reader, pgshard_admin, pgshard_router;

-- When the controller last showed it was alive, so a router waiting on a
-- migration can tell a long queue from no controller at all.
CREATE TABLE pgshard.controller_heartbeat (
    component text        PRIMARY KEY,
    term      bigint      NOT NULL,
    beat_at   timestamptz NOT NULL
);

GRANT SELECT ON pgshard.controller_heartbeat TO pgshard_reader, pgshard_admin;

RESET ROLE;
