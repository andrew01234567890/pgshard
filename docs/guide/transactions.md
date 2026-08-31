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

## Retry guidance

Retry on the SQLSTATE, never on the message. These are the only codes that
mean a retry is safe, and pgshard uses no others for it:

| Error | Client action |
|---|---|
| `40001` serialization_failure | retry the whole transaction — nothing it wrote was committed |
| `40P01` deadlock_detected | retry the whole transaction |
| `57P03` cannot_connect_now (write pause, barrier) | retry after a moment |
| `08007` transaction_resolution_unknown | **do not retry** — the transaction may still commit; the resolver finishes it. Check your data or use an idempotency key |
| anything else | the transaction is over; treat it as a failure, not as something to run again |

`40001` is the single code for every outcome pgshard knows to be safely
retryable — a failover inside your transaction, and a transaction the
resolver rolled back because it presumed the coordinator dead. The `DETAIL`
says which; the code does not change, because retry loops match one value.

Do **not** retry on "any error other than `08007`". Most drivers surface
connection failures and timeouts as neither, and a `COMMIT` that ends in a
lost connection has exactly the unknown outcome `08007` describes.

Metrics: `pgshard_router_in_doubt_transactions_total` on the router's
`/metrics` endpoint.
