# High availability: failover and switchover

Every replication group (the catalog and each shard) is a set of pods whose
PID 1 is `pgshard-agent`. The operator is the single decision maker: it
watches health, picks the candidate, publishes the fence and asks the agent to
promote. Agents refuse anything that is not properly fenced.

## Members

The PostgreSQL image ships the agent (`ENTRYPOINT ["pgshard-agent"]`,
`CMD ["run", "--config", "/etc/pgshard/agent.json"]`). The operator renders one
JSON config per member into the group ConfigMap (`<member>.json`) and runs
`pgshard-agent run --config /etc/pgshard/<member>.json`:

| Field | Value |
|---|---|
| `role` | `primary` for the designated primary, `standby` otherwise |
| `primaryConninfo` | `host=<cluster>-<group>-rw.<ns>.svc port=5432 user=postgres` (clone, rewind and streaming source) |
| `peerFailsafeURLs` | every other member's `http://<member>.<group>-peers.<ns>.svc:8080/failsafe` |
| `lease` | `{enabled: true, namespace: <ns>}`; the Lease is `<cluster>-<group>-primary` |
| `postgres.synchronousStandbyNames` | initial value; the operator maintains it afterwards |
| `postgres.parameters` | `spec.postgresql.parameters`; agent-owned settings win |
| `shutdownTimeout` | `5s` (smart shutdown bound before fast) |
| `passwordFile` | `/etc/pgshard-secret/password` from the superuser Secret |

Probes: startup `/startz`, readiness `/readyz`, liveness `/livez` on the agent
HTTP port 8080. A primary's `/livez` fails only when the kube API and every
peer `/failsafe` are unreachable; the agent self-fences (fast shutdown, exit)
once that isolation has lasted 30 s of consecutive probes, so one slow probe
under load never takes a primary down. The agent gRPC (`pgshard.v1.Agent`)
listens on 9090. Members run
under the `<cluster>-member` ServiceAccount whose Role allows Lease
get/create/update and Pod reads in the namespace.

Agent bootstrap: an empty data directory is initialised (`initdb`) or cloned
(`pg_basebackup` with a physical slot) according to `role`; an existing data
directory that belongs to a former primary while `role` is `standby` is
rewound against the `-rw` Service (falling back to a full reclone) and
restarted as a standby, so a failed primary rejoins on its own once its pod
comes back. A primary acquires its Lease before postgres starts and releases
it after a clean shutdown; a standby never touches it.

## State

Per group, `PgShardGroup.status.primary` and `.status.epoch` are the designated
primary and the fencing epoch (member 0 at epoch 0 for a new group). Pod labels
`pgshard.io/role` = `primary` | `replica` | `unhealthy` drive the `-rw` and
`-ro` Services; `unhealthy` is a fenced former primary that has not rejoined.
`status.members[].ready` for standbys records whether they were streaming at
the last healthy observation; it is preserved (not overwritten) while the primary
cannot be probed and gates whether a failover may start at all.

## Failover

The primary is unhealthy when its pod is missing, or the pod is not Ready and
`Agent.Status` fails or answers as a standby. After `DefaultFailoverDelay`
(10s) of continuous unhealthiness the operator, inside one reconcile:

1. relabels the old primary pod `role=unhealthy` (out of `-rw`) and **fences the
   Lease**: holder `pgshard-operator`, annotations
   `pgshard.io/primary-epoch` and `pgshard.io/primary` cleared. It refuses
   (`ErrLeaseHeldByOther`) when the Lease is renewed by any identity other than
   the old primary or the operator. A still-running old primary sees the
   foreign holder on its next renewal and self-fences (fast shutdown, exit).
2. waits (30s bound, 1s poll) until the old primary no longer answers
   `Status` as a running primary and every other reachable member reports no
   streaming WAL receiver; on timeout it proceeds only if the old primary is gone.
3. picks the candidate: highest `pg_last_wal_receive_lsn()` among **all**
   reachable in-recovery members — every non-primary member is in
   `synchronous_standby_names`, so even a lagging or not-yet-Ready standby may
   hold the only copy of an acknowledged commit; ties break by name. If a
   listed standby is unreachable and the reachable ones cannot be proven to
   hold every acknowledgement (`reachable + minSyncStandbys <= listed`), no
   candidate is admissible: the fence is released and the primary pod is
   recreated as primary (same PVC) — durability over availability.
4. computes `epoch = max(group epoch, candidate agent epoch) + 1` and **writes
   the fence first**: `pgshard.shard_status` (`primary_epoch`,
   `primary_endpoint`, never lowered) for shard groups, then
   `PgShardGroup.status`, then the Lease (holder handed to the candidate,
   annotations set). The catalog group is fenced by its status and Lease only.
5. `Agent.Promote{epoch, lease_holder}` on the candidate: the agent accepts only
   a strictly greater epoch, takes the Lease (already its own), disconnects
   its WAL receiver, runs `pg_ctl promote` and checkpoints.
6. relabels the candidate `role=primary`, re-renders the ConfigMap so the old
   primary's config says `standby`, and requeues.

The next passes recreate the old primary pod as a `replica` (it rejoins via
`pg_rewind`), create the other members' physical slots on the new primary
(slots do not replicate; the primary's own inherited slot is dropped so it
cannot pin WAL), recompute `synchronous_standby_names` around the new
primary, and publish `pgshard.shard_status` again. Convergence rules run every pass:
a designated primary answering as a standby is (re)promoted with an epoch
above whatever it accepted; a non-designated member answering as a primary is
labelled `unhealthy` and sent `Agent.Demote{group epoch}`.

A missing primary pod is never recreated while a candidate may exist: a fresh
pod would take the Lease back and no promotion would happen.

## Switchover

`kubectl annotate pgshardcluster <name> pgshard.io/switchover=<member>` moves
the member's group. The target must have been a streaming standby at the last
observation. The operator relabels the current primary `unhealthy`, deletes
its pod (the agent shuts postgres down: smart for 5s, then fast, then releases
the Lease) and runs the failover path with the target as the preferred
candidate; it wins when it holds the maximum flushed LSN, otherwise the
highest one does. The annotation is removed when the switchover finished (or
was refused). Writes fail only between the shutdown and the promotion.

## Rolling operations

Every change to a member's shape is applied one member per group at a time,
groups in parallel, and never while a group is unhealthy or a failover is in
flight. `status.rollout` (`phase`, `member`, `reason`, `lastRestartToken`) and
the `RolloutInProgress` condition report progress; each `PgShardGroup` carries
its own `status.rollout` step and the claim every member runs on
(`status.members[].pvc`).

### What a change is classified as

The operator renders a `MemberTemplate` per group: image, resources, restart
token, and the effective settings (`spec.postgresql.parameters` plus the
`pgshard.override.conf` derived by pgtune, see [tuning.md](tuning.md)). Two
hashes are stamped on every member pod: `pgshard.io/template-hash` (image,
resources, token) and `pgshard.io/settings-hash`.

| Change | Action |
| --- | --- |
| Settings whose `pg_settings.context` is `sighup`, `superuser`, `user` or `backend` | ConfigMap update + `Agent.Reload` on every member; no pod restart. The pod is stamped once the agent reports back the new settings hash (the ConfigMap volume can lag the API by a minute). |
| Any setting with context `postmaster` (or an unknown name) | rolling restart. The group remembers `settingsRestartPending` until every member restarted, so a later reload-only change cannot skip the pending restart. |
| Image, resources, `pgshard.io/restart=<token>` annotation | rolling restart; the token is recorded in `status.rollout.lastRestartToken` once every member carries it. |
| `storage.size` grows and the StorageClass has `allowVolumeExpansion` | the claim is patched in place; nothing restarts. |
| `storage.storageClassName` changes, or the size grows on a class that cannot expand | member rebuild onto a new claim. A shrink is ignored. |

The context is read from `pg_settings` on the primary before the ConfigMap is
rewritten; while the primary is unreachable the ConfigMap is left as it is.

### Rolling restart

For each group: every stale standby in name order — delete the pod (the claim
stays), wait until it is Running, Ready (the agent's readiness includes the
replay-lag check) and streaming again, and the sync set is re-applied — then
the primary: a switchover to the sync-set standby with the highest flushed
LSN is requested through the ordinary `pgshard.io/switchover` path, the old
primary pod is recreated as a standby with the new template by the same step,
and the group is settled again once it streams. One switchover per group,
writes pause only for its shutdown-to-promotion window. Only one switchover
runs per cluster at a time; groups otherwise proceed independently.

### Storage rebuild

For each stale member: create the successor claim `<member>-v<n>` on the new
class (labelled `pgshard.io/member=<member>`), record it in the group status,
delete the pod. The recreated pod mounts the new, empty claim and the agent
clones it with `pg_basebackup` from `-rw`. Once the member is Ready and
streaming the retired claim is deleted; it is never deleted while a pod still
mounts it and never before the successor streams. A primary is switched over
first, then rebuilt as a standby.

### Gates and holds

- Nothing is deleted while the group is not Ready, a switchover annotation is
  set or a failover timer is running.
- A standby is only taken down while `streaming - 1 >= minSyncStandbys`.
- The replica and primary PDBs describe the same budget for evictions; the
  operator's own deletions honour it through the one-at-a-time rule.
- A step that has not completed within `DefaultRolloutTimeout` (15 minutes)
  sets `Degraded=True/RolloutHeld` and `status.rollout.phase=Held`; nothing
  further is deleted. The missing member is still recreated and the rollout
  resumes by itself when it is back.

## Invariants

- The agent refuses `Promote`/`Demote` unless the epoch is strictly greater
  than the last accepted one, and `Promote` unless it holds the Lease.
- The operator never promotes while the Lease is renewed by another live
  holder; it fences the Lease before choosing a candidate and hands it to the
  candidate before `Promote`.
- Epochs only increase; the catalog row and Lease annotation are written
  before the promotion, so a router that reads `shard_status.primary_epoch`
  sees the new epoch no later than the new primary.
- Standbys' local epochs may lag the group epoch (only role changes bump them);
  same-term RPCs to them must use their own epoch.

## Not yet

- Operator-to-agent gRPC is plaintext inside the cluster; mTLS is a later layer.
- A postgres crash inside a running pod is a container restart, not a failover,
  until the pod stops being Ready and `Status` fails for the failover delay.
- `synchronous_standby_names` set via `ALTER SYSTEM` is dropped by an agent
  `Reload`/`Promote` and re-applied by the operator on its next probe.
