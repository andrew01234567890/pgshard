# Runbook: rolling restarts and configuration changes

Mechanism: [ha.md](../ha.md#rolling-operations),
[tuning.md](../tuning.md).

## What causes what

| Change | Effect |
|---|---|
| reloadable setting (`sighup`/`superuser`/`user`/`backend` context) | ConfigMap update + agent reload; no restart |
| `postmaster`-context setting (or unknown name) | rolling restart |
| image, resources, restart annotation | rolling restart |
| `storage.size` growth on an expandable class | in-place PVC patch; no restart |
| StorageClass change, or growth on a non-expandable class | member rebuild onto new PVCs ([storage-changes](storage-changes.md)) |

Settings go in `spec.postgresql.parameters`. The CRD rejects unsafe keys
(`fsync`, `wal_level`, `max_prepared_transactions`, ...); tuning re-derives
the rest and enforces the memory budget.

## Force a restart

```sh
kubectl annotate pgshardcluster demo pgshard.io/restart=r1 --overwrite
```

The token is recorded in `status.rollout.lastRestartToken` once every
member carries it.

## What a rolling restart does

Per group: each stale standby is deleted (PVC kept) and awaited back
streaming; then the primary is moved with an ordinary switchover and
recreated as a standby. One member per group at a time, groups in
parallel, one switchover per cluster at a time. Writes pause per group only
for the shutdown-to-promotion window. Nothing is taken down while a group
is unhealthy, a failover is in flight, or `streaming - 1 < minSyncStandbys`.

## Watching

```sh
kubectl get psc demo -o jsonpath='{.status.rollout}'
kubectl get psc demo -o jsonpath='{.status.conditions[?(@.type=="Degraded")]}'
```

## Stuck rollout (`Degraded=True/RolloutHeld`)

A step that has not completed within 15 minutes sets
`status.rollout.phase=Held`; nothing further is deleted, and the rollout
resumes on its own when the missing member returns.

1. `kubectl get pods` — find the member named in `status.rollout.member`.
2. Common causes: pod unschedulable (resources, node affinity), image pull
   failure, a standby that cannot catch up (check
   `status.members[].replayLagBytes` and the agent log), PVC binding.
3. Fix the cause; do not delete other members while held.
4. A reload-only change after a pending restart does not cancel it: the
   group remembers `settingsRestartPending` until every member restarted.

## Verifying a settings change

```sql
SHOW work_mem;                          -- through the router (runs on the session's shard)
```

or check `status.tuning.derived` for the derived value and its reason, and
the `TuningApplied` condition. The pod's settings hash is stamped only
after the agent reported the new settings, so a completed rollout means the
settings are live.
