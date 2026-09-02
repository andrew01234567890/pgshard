# Transactions and two-phase commit

`BEGIN` ... `COMMIT` works as in PostgreSQL. A transaction starts on the
shard of its first real statement; every further shard it touches gets its
own backend and is tracked as a participant. Full detail in
[router.md](../router.md#transactions).

## Single-shard (the common path)

A transaction with at most one writing participant commits that shard with
a plain `COMMIT`; read-only participants are rolled back. No coordination,
no decision log — the same durability as plain PostgreSQL with your
`synchronousCommit` setting.

## Multi-shard writes: two-phase commit

The first write to a second shard escalates the transaction to two-phase
commit (each shard must have `max_prepared_transactions > 0`; the operator
tuning sets it). On `COMMIT` the router:

1. durably records a `preparing` decision row in `pgshard.xact_decisions`
   on the catalog;
2. `PREPARE TRANSACTION` on every writer (each waits for its synchronous
   standbys); any failure rolls everything back and reports the error;
3. flips the row to `commit` — the point of no return;
4. `COMMIT PREPARED` everywhere, then deletes the row.

A successful `COMMIT` is therefore committed on every shard eventually,
even if the router dies immediately after. `08007`
(`transaction_resolution_unknown`) means the router could not learn whether
step 3 landed; the resolver finishes the transaction either way — treat it
as "in doubt", not as failure. Any other `COMMIT` error means rolled back
everywhere.

Readers are checked before commit: a `SELECT` that wrote through a function
is promoted to a writer and takes part in 2PC.

## The resolver

The controller resolves in-doubt transactions every few seconds: stale
`preparing` rows are aborted, `commit`/`abort` decisions are driven to
completion on every participant, and orphaned `pgshard-*` prepared
transactions with no decision row are rolled back. See the
[in-doubt 2PC runbook](../runbooks/in-doubt-transactions.md).

## Modes and limits

- `SET pgshard.transaction_mode = single` refuses the second writable shard
  (`0A000`) instead of escalating — useful to assert an application never
  pays for 2PC.
- Cross-shard reads run at READ COMMITTED on independent per-shard
  snapshots; there is no global snapshot. Scatter reads inside a
  transaction are allowed only before the transaction touches a shard, and
  refused under REPEATABLE READ / SERIALIZABLE.
- `SAVEPOINT` and `COMMIT/ROLLBACK AND CHAIN` are refused once a
  transaction spans several shards. Client-issued `PREPARE TRANSACTION`,
  `COMMIT PREPARED` and `ROLLBACK PREPARED` are always refused: they belong
  to the coordinator.

## Deadlocks that span shards

PostgreSQL detects deadlocks, but only the ones it can see. A multi-shard
transaction holds one backend, and its locks, on each shard it touched. If
`T1` holds a row on shard A and waits on B while `T2` holds a row on B and
waits on A, each server sees exactly one edge of that cycle and neither
finds it. `deadlock_timeout` does not help: it decides when a server *looks*
for a cycle, not how long a wait may last, so a server that looks and finds
nothing goes on waiting.

**pgshard does not detect deadlocks that span shards.** There is no global
wait graph. Such a cycle is broken by a timeout, not by detection, and both
transactions wait until it fires.

The timeout is `lock_timeout`, set on every shard of a transaction at the
moment it spans more than one — `--cross-shard-lock-timeout`, 30s by
default, negative to leave the wait unbounded. It is deliberately not
`deadlock_timeout`, which decides when a server *looks* for a cycle rather
than how long a wait may last, and so cannot end one it never finds.

What this means for an application:

- A cross-shard deadlock costs the timeout in latency before either side
  gives up, where a single-shard one is resolved by PostgreSQL almost at
  once.
- Single-shard transactions are unaffected — PostgreSQL's own detector sees
  the whole cycle and still resolves it as usual, with `40P01`.
- The usual defence is ordering: have transactions take their shards in a
  consistent order, so a cycle cannot form. That is the same advice as for
  single-node PostgreSQL, and here it matters more because the cost of
  getting it wrong is a timeout rather than an immediate abort.

Detecting these properly means collecting per-shard wait edges into one
graph and choosing a victim, which is what CockroachDB does. pgshard has not
built that, and this page will say so until it does.

## Retry guidance

Retry on the **exact** SQLSTATE. pgx, JDBC and psycopg retry loops test for
`40001` by value, so a safe retry reported under any other code is a retry
that will not happen.

| Error | Client action |
|---|---|
| `40001` shard failover | retry the whole transaction — nothing was written |
| `40001` resolver abort | retry the whole transaction — no participant committed. `DETAIL` distinguishes it from a serialization failure |
| `57P03` write pause (barrier) | retry after a moment |
| `08007` in doubt | **do not retry blindly** — the outcome is decided by the resolver; check your data or use an idempotency key |
| any other `COMMIT` error | not known to be safe. Do not retry automatically |

The last row used to say any other `COMMIT` error had rolled back everywhere
and was safe to retry. That is the more dangerous of the two mistakes a
client can make here: `08007` means the original transaction may still
commit, and a blanket retry policy duplicates it. Every outcome that *is*
safe to retry is named above.

Metrics: `pgshard_router_in_doubt_transactions_total` on the router's
`/metrics` endpoint.
