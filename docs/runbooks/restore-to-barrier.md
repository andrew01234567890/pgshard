# Runbook: restoring to a certified barrier

Mechanism: [backup.md](../backup.md#certified-barriers),
[guide/backup-restore.md](../guide/backup-restore.md). A barrier target is
the only restore that is consistent across shards; use it for disaster
recovery unless you specifically need an arbitrary point in time.

## 1. Pick the barrier and backup

```sql
SELECT name, certified, created_at, per_group
FROM pgshard.restore_points WHERE certified ORDER BY created_at DESC;
```

(also `Controller.ListBarriers`, or the admin UI `/backups` page — if the
source catalog is gone, the list is in the restored catalog once recovery
reaches it). Choose a completed `PgShardBackup` taken **before** the
barrier; every group restores its own set from that run.

## 2. Create the restore

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardRestore
metadata: {name: dr-1, namespace: prod}
spec:
  clusterName: orders
  newClusterName: orders-dr
  backupId: nightly-orders-full-20260823-0200
  target:
    barrier: nightly-orders-20260823-0900
```

The source cluster is never touched. The new cluster keeps the source's
shard count and major; the superuser Secret is copied because the restored
catalog carries the source's roles.

## 3. Watch the phases

`Pending` → `Restoring` → `Recovered`/`Reconciling` → done, or `Failed`.

```sh
kubectl get pgshardrestore dr-1 -o jsonpath='{.status.phase} {.status.groups}'
kubectl get pgshardrestore dr-1 -o jsonpath='{.status.reconciliation}'
```

After every primary reaches the barrier's restore point, the restored
catalog still carries the write fence the barrier raised — routers of the
new cluster refuse writes (`57P03`) until reconciliation:

1. the decision log is read from the restored catalog;
2. every shard's prepared `pgshard-*` transactions are finished against it
   (commit when the log says commit, rollback otherwise; a commit-decided
   transaction the shard does not hold must read `committed` in
   `pg_xact_status`);
3. the fence is released — only when there is no contradiction.

A contradiction leaves the cluster **fenced** with `status.error` naming
the `group: gid` pairs. That means the restored state cannot be made
consistent automatically — escalate; do not release the fence by hand
without understanding which shard diverged.

## 4. Cut over

Point applications at `orders-dr-router`. The new cluster archives to its
own stanzas; confirm `BackupHealthy` and take a fresh full backup before
declaring recovery done. Retire the old cluster deliberately — never let
two clusters archive into one stanza
([backup-failures](backup-failures.md)).

## Non-barrier targets

`target.time`/`lsn`/`name`/`xid`/`immediate` restore each group to its own
point: per-group PITR, not a cluster snapshot. Afterwards the operator
*reports* leftover prepared transactions
(`PreparedTransactionsPending` condition, per-group lists) but never
finishes them without a decision log. Finish them by hand
(`COMMIT PREPARED`/`ROLLBACK PREPARED` in the database they were prepared
in), knowing the cross-shard outcome is your call; if the target fell
inside a barrier window, release the leftover fence via the agent's
`SetWriteFence`. If those words worry you — restore to a barrier instead.

## Failure modes

`Failed` names the cause: invalid spec, missing/incomplete backup, an
existing cluster of that name, a primary that crash-loops because the WAL
ends before the target (choose an earlier target or later backup), or the
four-hour timeout.
