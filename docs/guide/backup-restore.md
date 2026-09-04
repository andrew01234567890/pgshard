# Backups and restore

pgshard backs up with pgBackRest: one stanza per replication group, WAL
archiving from every primary, full/differential/incremental sets, and
restores that build a *new* cluster. Reference: [backup.md](../backup.md).

## Configure a policy

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackupPolicy
metadata:
  name: nightly
spec:
  objectStore:
    type: s3                       # s3 | azure | gcs | posix | sftp
    bucket: pgshard-backups
    endpoint: s3.eu-west-1.amazonaws.com
    region: eu-west-1
    prefix: /demo
    credentials:
      secretRef: {name: backup-s3}          # keys: key, keySecret
    encryption:
      secretRef: {name: backup-cipher}      # key: passphrase (aes-256-cbc)
  schedules:
    full: "0 2 * * *"
    incremental: "*/30 * * * *"
  barrierSchedule: "0 * * * *"     # hourly certified restore points
  verifySchedule: "0 4 * * 0"      # weekly pgbackrest verify of every repo
  retention:
    full: 7
    differential: 4
```

Bind it with `spec.backup.policyRef: nightly` on the cluster. Attaching or
changing a policy triggers a rolling restart (archiving settings change).
Health shows up as the `BackupHealthy` condition on both the policy and the
cluster.

Take an ad-hoc backup:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackup
metadata:
  name: demo-before-upgrade
spec:
  clusterName: demo
  type: full
```

`status.groups[]` reports per-group label, LSNs, WAL range and sizes;
`status.backupId` is set once every group completed.

## Certified barriers

A barrier is a cluster-consistent restore point: the controller raises the
cluster write fence (routers hold new writes, in-flight transactions
finish), drains two-phase commits, creates `pg_create_restore_point`
(`pgshard-<name>`) on the catalog and every shard, waits until the WAL is
archived, and records the point in `pgshard.restore_points` with
`certified: true`. `barrierSchedule` automates this; writes pause for a few
seconds per barrier (`57P03` after the buffer window if it takes longer).

Only a barrier target gives you a restore that is consistent *across*
shards. Any other target (time, LSN, name, xid, immediate) is per-group
PITR: each group stops at its own point, and distributed transactions may
be half-applied — the operator reports leftover prepared transactions
rather than deciding them ([runbook](../runbooks/restore-to-barrier.md)).

## Restore

A `PgShardRestore` builds a new cluster from the repository; the source is
never touched:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardRestore
metadata:
  name: before-purge
spec:
  clusterName: orders            # source
  newClusterName: orders-pitr    # created by the operator
  backupId: nightly-orders-full-20260823-0200
  target:
    barrier: hourly-orders-20260823-0900   # or time / lsn / name / xid / immediate
```

- Same shard count and PostgreSQL major as the source; replicas and
  resources may change via `clusterSpec`.
- Each group's primary restores with `pgbackrest restore` plus archive
  recovery to the target, then the standbys clone from it. The new cluster
  archives to its own stanzas.
- A barrier restore ends with a reconciliation phase: prepared
  `pgshard-*` transactions are finished against the restored decision log,
  and the write fence the barrier raised is released only when there is no
  contradiction. Phases: `Pending` → `Restoring` → `Reconciling` →
  `Recovered` (or `Failed`, fenced, with the contradictions listed).

Track progress with `kubectl get pgshardrestore before-purge -o yaml` or on
the admin UI's `/backups` page.

## Verifying and operating

- `kubectl get psc demo -o jsonpath='{.status.conditions[?(@.type=="BackupHealthy")]}'`
- Certified points: `SELECT name, certified, created_at FROM pgshard.restore_points ORDER BY created_at DESC;`
- Repository state from a member: `pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=<stanza> info`
- Failure handling: [backup-failures runbook](../runbooks/backup-failures.md).

Not yet covered: backups from a standby, and a persistent spool volume for
asynchronous archiving (a pod restart re-pushes pending segments).
