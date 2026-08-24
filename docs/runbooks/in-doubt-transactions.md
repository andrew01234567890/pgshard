# Runbook: in-doubt two-phase transactions

Mechanism: [router.md](../router.md#transactions),
[guide/transactions.md](../guide/transactions.md). Normally nothing to do:
the controller's resolver finishes every in-doubt transaction within
seconds. This runbook is for when it cannot.

## Symptoms

- Clients received `08007` (`transaction_resolution_unknown`) on `COMMIT`.
- `pgshard_router_in_doubt_transactions_total` climbing on `/metrics`.
- Rows stuck in the decision log, or prepared transactions lingering on a
  shard (they hold locks and pin the vacuum horizon):

```sql
-- catalog
SELECT gid, state, participants, created_at, decided_at
FROM pgshard.xact_decisions ORDER BY created_at;
-- on a shard primary
SELECT gid, prepared, database FROM pg_prepared_xacts WHERE gid LIKE 'pgshard-%';
```

## First checks

1. **Is the resolver running?** The controller leader resolves every
   `--resolve-interval` (5s). Check the controller log for resolution
   passes and errors; `ResolveTransactions` over gRPC triggers a pass on
   demand.
2. **Can the controller reach every shard?** The resolver needs admin DSNs
   (`--shard-dsn` / `--shard-dsn-template`). A shard it cannot reach keeps
   its prepared transactions; the pass logs which one.
3. **Is a shard's primary down?** `COMMIT PREPARED` waits for the shard to
   have a primary; resolution completes after failover.

## What the resolver guarantees

- A `preparing` row older than 10s (its router died before deciding) is
  aborted; a slow router that decides commit first wins the guarded update.
- `commit` rows are committed everywhere, `abort` rows rolled back, then
  deleted.
- A `pgshard-*` prepared transaction with **no** decision row is an orphan
  and is rolled back (the row is always written before any prepare).
- A commit-decided gid is never rolled back.

Prepared transactions with gids not starting `pgshard-` are never touched.

## Manual resolution — last resort only

Only when the catalog's decision log is available and the resolver is
permanently unable to act (e.g. the controller cannot be run). For each
gid, read its state from `pgshard.xact_decisions`, then on each
participant shard, **in the database it was prepared in**:

```sql
COMMIT PREPARED 'pgshard-...';    -- decision state = commit
ROLLBACK PREPARED 'pgshard-...';  -- decision state = abort, or no row
```

Never commit a gid whose decision row is absent or not `commit`, and never
roll back one whose row says `commit` — that de-synchronizes shards
permanently. Delete the decision row only after every participant is
finished.

After a **non-barrier restore**, leftover prepared transactions are
expected and reported by the operator (`PreparedTransactionsPending`);
see [restore-to-barrier](restore-to-barrier.md) before finishing them by
hand.
