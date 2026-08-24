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
                     controller leader ── applier ─┘── login pgshard_ddl; SET lock_timeout; SET ROLE <client>; BEGIN; DDL; COMMIT ──▶ shard 0..n
```

1. **Classify.** The router parses the statement (`internal/router/plan/ddl.go`)
   and produces `kind` (also the command tag: `CREATE TABLE`, `GRANT`, …),
   `strategy` (`direct` inside a per-shard transaction, `concurrent` outside
   one, `multistep` for a plan of several statements — see *Strategies*
   below), and `scope`:

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

   **Strategies.** Forms that would hold a strong lock for the length of a
   table scan or an index build are rewritten so no step takes a long
   `ACCESS EXCLUSIVE`/`SHARE ROW EXCLUSIVE` lock; the client sends the plain
   statement and gets the plain tag back. Steps live in `meta.steps` and
   are run per shard in order, each under its own `lock_timeout` retry:

   | Statement | Strategy | What runs on each shard |
   |---|---|---|
   | `ADD CONSTRAINT … CHECK (…)` | multistep | `ADD CONSTRAINT <name> CHECK (…) NOT VALID` (brief `ACCESS EXCLUSIVE`), then `VALIDATE CONSTRAINT` (`SHARE UPDATE EXCLUSIVE`) |
   | `ADD CONSTRAINT … FOREIGN KEY … REFERENCES t` | multistep | same `NOT VALID` + `VALIDATE` pair; only when the referenced rows are co-located: a sharded table may reference a reference table or a sharded table by mapping its shard key onto the referenced shard key, an unsharded table may not reference a sharded one, a reference table only another reference table; otherwise `0A000 cross-shard foreign key` |
   | `ALTER COLUMN c SET NOT NULL` (PostgreSQL 18+) | multistep | `ADD CONSTRAINT <table>_<c>_not_null NOT NULL c NOT VALID`, then `VALIDATE CONSTRAINT`; the validated not-null constraint is the column's `NOT NULL` |
   | `ADD PRIMARY KEY (cols)` / `ADD [CONSTRAINT n] UNIQUE (cols)` | multistep | for a primary key, the `SET NOT NULL` pair for every column first; `CREATE UNIQUE INDEX CONCURRENTLY <name> ON t (cols)`; `ADD CONSTRAINT <name> PRIMARY KEY/UNIQUE USING INDEX <name>` (`DEFERRABLE`, `INCLUDE`, `NULLS NOT DISTINCT` carried; `WITH (…)`, `USING INDEX TABLESPACE` and `WITHOUT OVERLAPS` forms stay direct). The shard-key rule applies as before |
   | `REINDEX INDEX/TABLE/SCHEMA/DATABASE` | concurrent | `REINDEX (CONCURRENTLY) …`; `REINDEX SYSTEM` stays direct |
   | `DROP INDEX name` (one index, no `CASCADE`) | concurrent | `DROP INDEX CONCURRENTLY name` |
   | `DETACH PARTITION p` | multistep | `DETACH PARTITION p CONCURRENTLY`, then `DETACH PARTITION p FINALIZE` (a no-op unless a crash left the detach pending) |
   | `ADD COLUMN … DEFAULT <constant>`, `GENERATED ALWAYS AS (…) VIRTUAL`, `DROP COLUMN`, `… NOT VALID` forms, `CONCURRENTLY` forms | direct / concurrent | as written |

   Constraint and index names the client left out are chosen by the router
   in PostgreSQL's shape (`<table>_<cols>_check`, `_fkey`, `_not_null`,
   `_key`, `<table>_pkey`), deterministically and at most 63 bytes (an
   over-long name keeps a prefix plus an 8-hex hash), so every shard ends up
   with the same name. An `ALTER TABLE` with several actions of which one
   needs steps is refused (`0A000`): run that action as its own statement.

2. **Rewrite class runs online.** `ALTER COLUMN … TYPE` (any change, with
   or without `USING`) and `ADD COLUMN … DEFAULT <volatile expression>`
   would rewrite the table under an exclusive lock; the router instead
   classifies them as strategy `rewrite` — an OID-preserving in-place
   column duplication driven by the applier. See
   [online-ddl.md](online-ddl.md) for the mechanism, the router's column
   hiding and the limitations. `VACUUM (FULL) <sharded or reference
   table>` becomes strategy `repack`: `REPACK (CONCURRENTLY)` on
   PostgreSQL 19+, the plain (locking) `VACUUM FULL` on 18. Metadata-only
   forms (`ADD COLUMN` with a constant or stable default such as
   `now()`/`CURRENT_TIMESTAMP`, `DROP COLUMN`, …) are applied as written;
   constraint, index and partition work takes the weaker-lock strategies
   above.

3. **Refuse what cannot be applied online.** `0A000`:
   * DDL inside a transaction block — each shard commits its own
     transaction; the fan-out cannot be rolled back with the client's.
   * Remaining rewrite class (`SET LOGGED`/`UNLOGGED`, `SET TABLESPACE`,
     `ADD COLUMN … GENERATED AS IDENTITY`, `ADD COLUMN … serial`,
     `ADD COLUMN … GENERATED … STORED`, `ALTER COLUMN … TYPE` combined
     with other actions or `COLLATE`): create a new table and copy the
     rows.
   * `TRUNCATE`, `LOCK`, plain `VACUUM`/`ANALYZE`, `COPY` on
     sharded/reference tables and `CREATE TABLE AS` over them (not
     migrations; still refused).
   * Roles with `SUPERUSER`, `REPLICATION` or `BYPASSRLS` (they would apply
     on every shard's server), `ALTER ROLE … RENAME`, `ALTER ROLE … SET …
     FROM CURRENT`, `ALTER DEFAULT PRIVILEGES`, `REASSIGN OWNED` and `DROP
     OWNED` — see [roles.md](roles.md).

4. **Queue and wait.** The router inserts the row (`state = queued`) with
   the statement text, the client's role (`meta.run_as`) and the object it
   creates or drops (`meta.object`), then polls the row until it is
   `complete` or `failed` and answers the client with the command tag or the
   error. `SET pgshard.ddl_async = on` returns immediately after the insert
   with the tag and a NOTICE naming the migration id; `RESET` restores
   synchronous DDL. Cancelling a waiting statement leaves the migration
   running in the background (`57014` names the id).

5. **Apply.** The controller leader's applier (`internal/controller/applier.go`)
   takes queued and running migrations oldest first, one at a time, and
   runs each on its targets in shard order. Client statements never run on
   a superuser session: the applier logs into the shard's primary as
   `pgshard_ddl`, a `NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE
   NOBYPASSRLS` login with no privileges of its own that it provisions on
   every shard through the admin DSN (`--shard-dsn`/`--shard-dsn-template`)
   with a password generated per controller process, grants `<client role>
   TO pgshard_ddl WITH SET TRUE, INHERIT FALSE` and then `SET ROLE <client
   role>`, so ownership and privilege checks are the client's — `CREATE
   ROLE` / `CREATE DATABASE` through the router need `CREATEROLE` /
   `CREATEDB` on the client role, not on `pgshard_ddl`. The membership is
   revoked (`REVOKE <client role> FROM pgshard_ddl`) as soon as the shard's
   statement or step ends, whether it applied or failed, and when the
   controller first touches a shard it revokes every membership
   `pgshard_ddl` still holds from a process that died mid-step. A function
   the statement evaluates (a `CHECK` or foreign-key validation, a default)
   that does `RESET ROLE` therefore lands on a role that owns nothing, can
   neither `ALTER ROLE … SUPERUSER` nor `SET SESSION AUTHORIZATION`, and
   can `SET ROLE` only back into the client it is running for — never into
   another tenant's role (`42501`). A superuser client role is refused
   (`42501`): DDL through the router runs as plain roles only.
   * `direct`: `SET lock_timeout = '2s'; BEGIN; <statement>; COMMIT`.
   * `concurrent`: `SET lock_timeout = '2s'; <statement>` outside a
     transaction; when `CREATE INDEX CONCURRENTLY` fails and leaves an
     invalid index (`pg_index.indisvalid = false`) the index is dropped
     concurrently and the statement run once more before the shard fails.
   * `multistep`: `meta.steps` in order; each step runs in its own
     transaction (or outside one when it is a `CONCURRENTLY` form) under the
     same `lock_timeout` retry, and `per_shard.<shard>.step` records the
     step the shard is on. Every step is idempotent for crash-resume: it is
     skipped when its `skip` check already holds on the shard (`constraint`
     exists, `constraint_valid`, a `notnull` constraint on the column
     exists / is `notnull_valid`, `index_valid`, partition `detached` or
     its `detach_pending`). A `CREATE UNIQUE INDEX CONCURRENTLY` step drops
     an invalid leftover before it runs, rebuilds once when the build fails
     and drops the invalid index again before the shard fails, so no invalid
     index is left behind. A `VALIDATE CONSTRAINT` that fails (a row
     violates the constraint, `23502`/`23503`/`23514`) drops the `NOT VALID`
     constraint on that shard before the shard fails, so the statement can
     be re-run as is once the rows are fixed; shards already applied stay
     applied and are skipped by the checks on the re-run.
   * `CREATE`/`DROP DATABASE` and role statements run outside a transaction
     against the maintenance database; everything else against the
     migration's database.
   * Role statements (`CREATE`/`ALTER`/`DROP ROLE`, `GRANT`/`REVOKE` of a
     role, `ALTER ROLE … SET`) also run on the **catalog group** — last,
     only when every shard applied, as the controller's own catalog role
     (no `SET ROLE`) — because the router authenticates against catalog
     verifiers and the pooler dials shards as the real user. Object grants
     stay on the shards. See [roles.md](roles.md).
   * `55P03` (lock timeout), deadlocks, connection failures and unreachable
     shards are retried with backoff 0.5 s → 30 s for up to 5 minutes; the
     shard is `retrying` meanwhile. Because the DDL waits at most 2 s for its
     lock, readers and writers arriving behind it wait at most that long.
   * Any other error is a hard failure of that shard.

6. **Record.** `per_shard` holds one entry per target,
   `{"<shard>": {"state": pending|running|retrying|applied|skipped|failed, "attempts": n, "error": "...", "sqlstate": "..."}}`.
   The migration is `complete` when every shard is `applied` (or `skipped`
   under scope `existing`, with at least one applied) and `failed` as soon as
   one shard fails hard or exhausts its retries; `error` names the first
   failed shard. On completion the statement's desired-state delta is
   recorded: `CREATE/ALTER ROLE` upserts `pgshard.roles` (verifier and
   attributes), `DROP ROLE` deletes the row, `GRANT/REVOKE` of a role edits
   `pgshard.role_members`, object `GRANT/REVOKE` edits `pgshard.grants`,
   `ALTER ROLE … SET/RESET` edits `pgshard.role_settings` and
   `CREATE`/`DROP DATABASE` inserts or deletes `pgshard.databases`. The
   router's verifier cache therefore serves a new password only once every
   shard and the catalog accepted it.

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
-- the steps of a multistep migration and the step each shard is on
SELECT id, jsonb_path_query_array(meta, '$.steps[*].sql') AS steps, per_shard
FROM pgshard.migrations WHERE meta ? 'steps' ORDER BY created_at DESC LIMIT 5;
```

The `strategy` column of the row stays `direct` for a multistep migration;
`meta.steps` being present is what makes it one (the catalog readers
report `multistep`).

The controller applies migrations only when started with `--shard-dsn` or
`--shard-dsn-template` (the same DSNs the transaction resolver uses); those
admin DSNs only provision `pgshard_ddl` (`--ddl-role`) and its membership,
every client statement runs on a `pgshard_ddl` session.
`--apply-interval` (1s) bounds how quickly a queued migration is picked up.
