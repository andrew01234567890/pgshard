# Queries, DDL and DCL

The router plans every statement against the catalog snapshot and routes it
to the shards it touches. This page is the user-level summary; the precise
rules, SQLSTATEs and refusal messages are in [router.md](../router.md) and
[ddl.md](../ddl.md).

## DML

| Placement | Reads | Writes |
|---|---|---|
| unsharded | home shard | home shard |
| sharded | shard of the key; multi-shard `SELECT` via scatter | shard of the key |
| reference | any one shard | every shard, transactionally |

**Keyed statements.** A `WHERE customer_id = $1` (or `IN (...)`) pins the
statement to the key's shard. `INSERT` must supply the shard key as a
constant or parameter. The shard key is immutable (`UPDATE ... SET key` is
refused). Untyped key literals that look numeric must be cast
(`'1'::int8`); drivers that prepare-and-describe (pgx, JDBC) carry the type
automatically.

**Scatter reads.** A read-only `SELECT` over one sharded table with no key
predicate fans out to every shard and merges the streams: plain scans,
`ORDER BY` (streaming merge; text keys need an explicit `COLLATE "C"`),
`LIMIT`/`OFFSET` (pushed down), `count`/`sum`/`min`/`max` without
`GROUP BY`, and `GROUP BY`/`DISTINCT` that include the shard key. Anything
else multi-shard — joins, subqueries, CTEs, window functions, `avg()`,
set operations, `FOR UPDATE` — is refused with `0A000` and a message naming
the rule. Scatter `UPDATE`/`DELETE` without a key predicate is refused.

**Reference tables.** `INSERT`/`UPDATE`/`DELETE` on a reference table run
the same statement on every shard inside one two-phase transaction, so
reference tables never diverge. Volatile functions (`now()`,
`gen_random_uuid()`, ...) and reads of sharded/unsharded tables inside such
a write are refused: they would produce different rows per shard. Compute
volatile values in the client and pass them as parameters, and keep
reference-table column defaults constant.

**Refused session features.** `LISTEN`/`NOTIFY`, `WITH HOLD` cursors and
temporary tables are refused (`0A000`). Session `SET` and named prepared
statements work and are replayed across backend changes; the usual
transaction-pooling caveats apply ([router.md](../router.md#session-model)).

## DDL

DDL never runs in your session. The router validates the statement, writes
it to `pgshard.migrations`, and the controller's applier drives it across
the target shards while your client waits for the tag (or the first shard's
error). `SET pgshard.ddl_async = on` returns at once with a NOTICE naming
the migration id.

- Statements on sharded or reference tables (and schemas, types, sequences,
  databases, roles, grants) target every shard; unsharded tables target the
  home shard.
- Forms that would hold a long strong lock are rewritten into weaker-lock
  steps automatically: `ADD CONSTRAINT ... CHECK/FOREIGN KEY` as
  `NOT VALID` + `VALIDATE`, `SET NOT NULL` likewise, `ADD PRIMARY
  KEY/UNIQUE` via `CREATE UNIQUE INDEX CONCURRENTLY`, `DROP INDEX` and
  `REINDEX` concurrently, `DETACH PARTITION CONCURRENTLY`.
- Refused (`0A000`): DDL inside a transaction block; table-rewrite forms
  (`ALTER COLUMN ... TYPE`, `ADD COLUMN` with a volatile default, identity
  or `serial`, `GENERATED ... STORED`, `SET LOGGED/UNLOGGED`,
  `SET TABLESPACE`) until online schema change lands; dropping, renaming or
  retyping the shard key; renaming or moving a sharded/reference table;
  mixing sharded and unsharded tables in one statement; `TRUNCATE`,
  `VACUUM`, `LOCK` and `COPY` on sharded/reference tables.
- A migration that fails on one shard leaves the applied shards applied;
  the error carries `migration <id> failed on shard N`. Fix the cause and
  re-run (idempotent forms such as `IF NOT EXISTS` converge the rest). See
  [ddl.md](../ddl.md#degraded).

Watch progress:

```sql
SELECT id, kind, state, per_shard, error
FROM pgshard.migrations ORDER BY created_at DESC LIMIT 20;
```

or on the admin UI's `/migrations` page.

## DCL (roles and grants)

`CREATE`/`ALTER`/`DROP ROLE`, `GRANT`/`REVOKE` and `ALTER ROLE ... SET` are
migrations too: they apply on every shard and then on the catalog, the
password is hashed once in the router so every server stores the same SCRAM
verifier, and the delta is recorded as desired state that the controller
verifies and repairs ([roles.md](../roles.md)). Superuser, replication and
BYPASSRLS roles, `ALTER ROLE ... RENAME`, `ALTER DEFAULT PRIVILEGES`,
`REASSIGN OWNED` and `DROP OWNED` are refused.

## COPY

`COPY ... FROM STDIN` and `COPY ... TO STDOUT` work on unsharded tables.
COPY on sharded and reference tables is not available yet.

## Errors worth knowing

| SQLSTATE | Meaning |
|---|---|
| `0A000` | refused shape; the message names the rule and usually a workaround |
| `40001` | shard failover inside your open transaction — retry the whole transaction |
| `57P03` | cluster write pause while a certified barrier is taken — retry |
| `08007` | two-phase commit outcome unknown; the resolver finishes it ([transactions.md](transactions.md)) |
| `53300` | the cluster is at a concurrency limit: no backend free, the failover buffer full, or the scatter budget exhausted. The first three clear on their own; a statement needing more shard streams than the whole budget will not succeed on retry |
| `55000` | wait and retry. Two conditions answer it: a stale routing generation during a topology change, and a table mid online-rewrite whose column list is not published yet. The error's `Reason` distinguishes them, because the SQLSTATE alone cannot |
