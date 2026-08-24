# Runbook: failover and switchover

Mechanism: [ha.md](../ha.md). Failover is automatic; this runbook is for
watching it, forcing a planned switchover, and the cases where the operator
deliberately refuses to act.

## Planned switchover

```sh
kubectl annotate pgshardcluster demo pgshard.io/switchover=demo-shard-0-1
```

The target must have been a streaming standby at the last observation. The
operator fences the current primary, shuts it down (smart 5s, then fast)
and promotes; writes fail only between shutdown and promotion. The
annotation disappears when the switchover finished or was refused. One
switchover runs per cluster at a time.

## Watching a failover

Automatic failover starts after 10s of continuous primary unhealthiness.

```sh
kubectl get pgshardgroup -l pgshard.io/cluster=demo   # primary, epoch per group
kubectl get pods -L pgshard.io/role                   # primary | replica | unhealthy
kubectl get psc demo -o jsonpath='{.status.conditions[?(@.type=="PrimaryHealthy")]}'
```

On the catalog:

```sql
SELECT shard_set, shard_id, serving_state, primary_epoch, primary_endpoint, updated_at
FROM pgshard.shard_status;
```

Expected sequence: old primary labelled `unhealthy`, Lease fenced,
candidate promoted with a higher epoch, `shard_status` republished, old
primary pod recreated as a replica and rejoining via `pg_rewind` (falling
back to a re-clone from the backup repository, then `pg_basebackup`).
Routers buffer statements through the window (`--buffer-window`, 10s);
clients inside an open transaction get `40001` and must retry.

## When no failover happens — and why

- **Not enough survivors.** If a listed standby is unreachable and the
  reachable ones cannot be proven to hold every acknowledged commit, the
  operator recreates the old primary on its PVC instead of promoting:
  durability over availability. Fix the unreachable standby or the old
  primary's node.
- **Postgres crash inside a running pod** is a container restart, not a
  failover, until the pod stops being Ready for the failover delay.
- **`unhealthy` label stuck**: a fenced former primary that has not
  rejoined. Check its agent logs (`kubectl logs <pod> -c postgres`);
  rewind/re-clone failures name the cause.

## Manual intervention

There is no "force promote" switch: promotion outside the fencing protocol
risks split brain (the agent refuses non-fenced promotion by design). If a
group is stuck with no primary:

1. confirm the Lease (`kubectl get lease <cluster>-<group>-primary -o yaml`)
   and the group status agree on who is designated;
2. make the designated member's pod schedulable/healthy — the operator
   converges from there (a designated primary answering as a standby is
   re-promoted automatically);
3. as a last resort, restore the cluster from a backup
   ([restore-to-barrier](restore-to-barrier.md)).

Verify after: every member Ready, `pgshard.io/role` labels consistent,
`replay_lag_bytes` shrinking, and no `unhealthy` label older than a few
minutes.
