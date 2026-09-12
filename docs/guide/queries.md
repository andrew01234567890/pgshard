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

**Scatter reads.** A read-only `SELECT` with no key predicate fans out to
every shard and merges the streams: plain scans, `ORDER BY` (streaming
merge; text keys need an explicit `COLLATE "C"`), `LIMIT`/`OFFSET` (pushed
down), `count`/`sum`/`avg`/`min`/`max` without `GROUP BY`, and `GROUP BY`/
`DISTINCT` that include the shard key. Anything else multi-shard —
subqueries, CTEs, window functions, set operations, `FOR UPDATE` —
is refused with `0A000` and a message naming the rule. Scatter
`UPDATE`/`DELETE` without a key predicate is refused.

**`avg()` is computed from a sum and a count.** An average of averages is
not the average — a shard holding one row and a shard holding a thousand
would count equally — so each shard is asked for `sum(x)` and `count(x)`
and the division happens once, at the router, over the totals. The count is
a column you never see. The division follows PostgreSQL's own rule for the
scale of a numeric quotient, so `avg()` over integers and `numeric` gives
the same text a single node gives.

`avg()` over `real` is refused: PostgreSQL accumulates `avg(real)` in
double precision while its `sum(real)` accumulates in `real`, so a sum
taken per shard has already been rounded and dividing it would not be the
same number. Cast to double precision (`avg(x::float8)`), which is
order-dependent in the low bits for the reason below.

**`sum()` over `float8` is order-dependent.** A scatter sums each shard's
rows and then adds the per-shard totals, which is a different association
from one pass over all the rows. Floating-point addition is not
associative, so the two answers can differ in their low bits. The same is
already true on one PostgreSQL, where the answer depends on the order the
planner happens to read rows in and on whether it aggregates in parallel;
sharding makes a difference that was incidental into one that is
structural. Measured here on one large value and four hundred small ones
spread across shards:

```
sum(price) on one PostgreSQL   1e+16
through pgshard                1.00000000000004e+16
```

`count`, `min` and `max` are unaffected, and so is `sum()` over `numeric`
and the integer types, which is exact. Use `numeric` for anything being
reconciled against a total computed another way.

**Colocated joins.** More than one table may take part in a scatter,
provided every row a join could match is already on the shard that holds
it. That is the case when each sharded table is joined to the others **on
its shard key**, and when a reference table is joined to anything — every
shard has the whole copy. So this fans out and is answered correctly:

```sql
SELECT o.id, l.sku
FROM orders o JOIN order_lines l ON l.customer_id = o.customer_id
JOIN regions r ON r.id = o.region_id;
```

while joining two sharded tables on anything but their shard key is refused
(`cross-shard join is not available yet`) rather than answered from the
rows that happen to share a shard. Joining a sharded table to an unsharded
one is refused for the same reason: the unsharded table is on the home
shard alone.

A keyed statement is under the same rule, and it is worth being precise
about it: pinning *one* side is not enough. `WHERE o.customer_id = $1`
alone still leaves `order_lines` unpinned, so a join to it on anything but
the shard key is refused. Give every sharded table its own key —
`WHERE o.customer_id = $1 AND l.customer_id = $1` — and the whole statement
runs on that one shard, joined however you like.

The full rule, including how the key must be written, is in
[router.md](../router.md#routing).

**Reference tables.** `INSERT`/`UPDATE`/`DELETE` on a reference table run
the same statement on every shard inside one two-phase transaction, so
reference tables never diverge. Volatile functions (`now()`,
`gen_random_uuid()`, ...) and reads of sharded/unsharded tables inside such
a write are refused: they would produce different rows per shard. Compute
volatile values in the client and pass them as parameters, and keep
reference-table column defaults constant.

**Seeing the routing decision.** `EXPLAIN (PGSHARD) <statement>` returns the
router's own plan: which shards the statement reaches, its fan-out class, the
tables it routed on, and what the router does with the shards' rows. The
statement is not run, and it never leaves the router.

```
=> EXPLAIN (PGSHARD) SELECT * FROM orders ORDER BY id LIMIT 10;
                            PGSHARD PLAN
 ----------------------------------------------------------------
 Route [Scatter]
   Shards: 0, 1, 2, 3
   Fanout: scatter
   Table:  public.orders
   Merge:  the shards' rows are merged in ORDER BY order; LIMIT 10 applied at the router
```

A refusal is rendered as the plan rather than raised, so you can see why a
statement would be refused without the refusal ending your transaction --
including the statements `pgshard.fanout` below would reject. A plain
`EXPLAIN` still means what PostgreSQL means by it: it is routed like any
other statement and comes back with one shard's plan. The option takes no
others (`ANALYZE` would promise a run that does not happen).

**Catching an accidental scatter.** `SET pgshard.fanout = 'single'` refuses
any statement that would route to more than one shard, and `'multi'` allows a
bounded set (an `IN` list) but still refuses a scatter. The default,
`'scatter'`, refuses nothing. The refusal names both shapes:

```
ERROR:  the statement's fan-out (scatter) exceeds pgshard.fanout (single)
HINT:   add a shard key predicate, or raise pgshard.fanout for this session
```

Set it in tests and CI to catch a query that lost its shard-key predicate
before it reaches production, or on a request path that should always be
keyed. DDL is exempt, and so is the fan-out that maintains a reference
table -- a reference write reaches every shard by definition.

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
| `55000` | wait and retry — **unless it came from the shard**. Three things answer it: a stale routing generation during a topology change, a table mid online-rewrite whose column list is not published yet, and PostgreSQL itself (the placement-fence trigger, `REFRESH MATERIALIZED VIEW CONCURRENTLY` without a unique index, and more). The error's `Reason` distinguishes them, because the SQLSTATE alone cannot: `REASON_BACKEND_ERROR` is the shard's own answer and is not a topology change, so retrying it unchanged will get the same answer |
