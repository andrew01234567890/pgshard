# DDL and DCL migrations

Every schema or access-control statement a client sends through the router
is a *migration*: a row in `pgshard.migrations` that the controller's
applier drives across the shards it targets. The router validates and
classifies the statement, the applier executes it, and the row is the
durable record of what happened on every shard.

## Model

```
client ──DDL──▶ router ──INSERT queued──▶ pgshard.migrations ◀──poll── router
                   │                              ▲
                   └───── waits for state ────────┘
                                                  │
                     controller leader ── applier ─┘── SET ROLE <client>; BEGIN; SET LOCAL lock_timeout; DDL; COMMIT ──▶ shard 0..n
```

1. **Classify.** The router parses the statement (`internal/router/plan/ddl.go`)
   and produces `kind` (also the command tag: `CREATE TABLE`, `GRANT`, …),
   `strategy` (`direct` inside a per-shard transaction, `concurrent` outside
   one for `CREATE/DROP INDEX CONCURRENTLY` and `REINDEX CONCURRENTLY`), and
   `scope`:

   | Statement | Scope |
   |---|---|
   | tables declared sharded or reference; schemas, sequences, types, views over sharded tables; databases; roles; `GRANT`/`REVOKE` not limited to unsharded tables | `all` — every shard of the set |
   | unsharded tables and views/grants over them only | `home` — the database's home shard |
   | `DROP`/`ALTER`/`REINDEX INDEX`, `DROP`/`ALTER VIEW` (owning table unknown to the router) | `existing` — every shard, shards without the object are skipped; failed if no shard had it |

   Sharded-table rules are enforced here: `CREATE TABLE` must define the
   shard key column and every PRIMARY KEY/UNIQUE (also `CREATE UNIQUE INDEX`
   and `ALTER TABLE ADD PRIMARY KEY/UNIQUE`) must include it; the shard key
   column cannot be dropped, renamed or retyped; a sharded or reference
   table cannot be renamed or moved to another schema (the catalog declares
   it by schema and name). One statement cannot mix sharded and unsharded
   tables (`DROP TABLE a, b`, `GRANT … ON a, b`).

2. **Refuse what cannot be applied online.** `0A000`:
   * DDL inside a transaction block — each shard commits its own
     transaction; the fan-out cannot be rolled back with the client's.
   * Rewrite class (`ALTER COLUMN … TYPE`, `SET LOGGED`/`UNLOGGED`,
     `SET TABLESPACE`, `ADD COLUMN … DEFAULT <volatile expression>`): these
     rewrite the table under an exclusive lock and wait for online schema
     change (M8). Metadata-only and weaker-lock forms (`ADD COLUMN` with a
     constant default, `DROP COLUMN`, `ADD CONSTRAINT`, `SET NOT NULL`, …)
     are applied.
   * `TRUNCATE`, `LOCK`, `VACUUM`, `COPY` on sharded/reference tables and
     `CREATE TABLE AS` over them (not migrations; still refused).

3. **Queue and wait.** The router inserts the row (`state = queued`) with
   the statement text, the client's role (`meta.run_as`) and the object it
   creates or drops (`meta.object`), then polls the row until it is
   `complete` or `failed` and answers the client with the command tag or the
   error. `SET pgshard.ddl_async = on` returns immediately after the insert
   with the tag and a NOTICE naming the migration id; `RESET` restores
   synchronous DDL. Cancelling a waiting statement leaves the migration
   running in the background (`57014` names the id).

4. **Apply.** The controller leader's applier (`internal/controller/applier.go`)
   takes queued and running migrations oldest first, one at a time, and
   runs each on its targets in shard order through an admin connection to
   the shard's primary (`--shard-dsn`/`--shard-dsn-template`) as
   `SET ROLE <client role>`, so ownership and privilege checks are the
   client's:
   * `direct`: `BEGIN; SET LOCAL lock_timeout = '2s'; <statement>; COMMIT`.
   * `concurrent`: `SET lock_timeout = '2s'; <statement>` outside a
     transaction; when `CREATE INDEX CONCURRENTLY` fails and leaves an
     invalid index (`pg_index.indisvalid = false`) the index is dropped
     concurrently and the statement run once more before the shard fails.
   * `CREATE`/`DROP DATABASE` and role statements run outside a transaction
     against the maintenance database; everything else against the
     migration's database.
   * `55P03` (lock timeout), deadlocks, connection failures and unreachable
     shards are retried with backoff 0.5 s → 30 s for up to 5 minutes; the
     shard is `retrying` meanwhile. Because the DDL waits at most 2 s for its
     lock, readers and writers arriving behind it wait at most that long.
   * Any other error is a hard failure of that shard.

5. **Record.** `per_shard` holds one entry per target,
   `{"<shard>": {"state": pending|running|retrying|applied|skipped|failed, "attempts": n, "error": "...", "sqlstate": "..."}}`.
   The migration is `complete` when every shard is `applied` (or `skipped`
   under scope `existing`, with at least one applied) and `failed` as soon as
   one shard fails hard or exhausts its retries; `error` names the first
   failed shard. On completion `CREATE/ALTER ROLE … PASSWORD` upserts
   `pgshard.roles` with the SCRAM verifier, `DROP ROLE` deletes the row,
   `CREATE`/`DROP DATABASE` inserts or deletes `pgshard.databases`.

## Guarantees

* **Idempotent per shard.** DDL is transactional on one shard: a shard is
  either fully applied or untouched. The applier saves progress before and
  after every shard step; a restart resumes `running` migrations and
  re-drives shards that are not `applied`. A shard that was `running` or
  `retrying` when the applier died is first checked against
  `meta.object` (table/index/view/sequence exists or is gone, schema, type,
  role, database) and marked applied without re-running the statement when
  the object already matches. Statements without such an object (`ALTER
  TABLE`, `GRANT`) are re-executed; a `pending` shard is never guarded, so an
  object created out of band is a hard failure, not a silent success.
* **Same verifier everywhere.** `CREATE ROLE … PASSWORD 'plain'` is hashed
  in the router; every shard and the catalog store the same SCRAM verifier.
* **Ordering.** Migrations are applied in submission order across the whole
  catalog, one at a time; a migration retrying against a long lock delays
  the ones behind it.
* **Singleton.** Only the controller leader applies; a second controller
  waits for the leader lock.

## DEGRADED

A failed migration leaves the shards it already applied applied. The
schema of the table (or the role/grant set) then differs across shards
until the statement is fixed and run again; the client's error carries
`DETAIL: migration <id> failed on shard N; applied on shard …` and the row
keeps the per-shard detail. Re-running an idempotent form (`IF NOT
EXISTS`, `IF EXISTS`, `CREATE OR REPLACE`) converges the remaining shards;
a non-idempotent one fails on the shards that already have the object with
`42P07`/`42710`, which names them. Reads and writes keep working on every
shard meanwhile; a query touching the new column on a shard that lacks it
fails on that shard only.

## Operations

```sql
SELECT id, kind, state, scope, per_shard, error, created_at, finished_at
FROM pgshard.migrations ORDER BY created_at DESC LIMIT 20;
```

The controller applies migrations only when started with `--shard-dsn` or
`--shard-dsn-template` (the same DSNs the transaction resolver uses);
`--apply-interval` (1s) bounds how quickly a queued migration is picked up.
