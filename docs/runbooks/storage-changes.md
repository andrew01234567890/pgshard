# Runbook: PVC growth and StorageClass changes

Mechanism: [ha.md](../ha.md#storage-rebuild).

## Growing volumes

If the StorageClass has `allowVolumeExpansion: true`, edit
`spec.storage.size` (or `spec.catalog.storage.size`) and the operator
patches every claim in place — no restarts, no data movement.

```sh
kubectl get storageclass <class> -o jsonpath='{.allowVolumeExpansion}'
kubectl get pvc -l pgshard.io/cluster=demo
```

A shrink is ignored by design.

## Changing the StorageClass (or growing a non-expandable class)

Set `spec.storage.storageClassName`. The operator rebuilds one member per
group at a time:

1. creates the successor claim `<member>-v<n>` on the new class;
2. deletes the pod; the recreated pod mounts the empty claim and the agent
   clones it (`pg_basebackup` from the `-rw` Service, or a repository
   restore when configured);
3. deletes the retired claim only after the member is Ready and streaming —
   never while a pod mounts it.

Primaries are switched over first and rebuilt as standbys, so each group
pays one shutdown-to-promotion window.

## Before you start

- Capacity: during the rebuild each member briefly needs both claims'
  worth of storage in the cluster.
- Clone source load: every rebuild streams a basebackup from the group
  primary; do it off-peak for large shards, or ensure a completed backup
  exists so members re-clone from the repository instead.
- Check `minSyncStandbys` headroom — a standby is only taken down while
  `streaming - 1 >= minSyncStandbys`.

## Watching and recovering

```sh
kubectl get pgshardgroup <cluster>-<group> -o jsonpath='{.status.rollout} {.status.members[*].pvc}'
kubectl get pvc -l pgshard.io/member=<member>
```

Stuck states follow the rolling-restart rules
([rolling-restarts](rolling-restarts.md)): a held rollout stops deleting
and resumes when the member returns. If a clone keeps failing, the agent
log names the phase (basebackup, rewind, repository restore); check
network policy to the `-rw` Service and repository reachability.

Never delete a retired claim by hand until its successor streams; the
operator does it as the last step.
