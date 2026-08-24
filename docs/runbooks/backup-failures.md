# Runbook: backup failures and ArchiveDuplicateError

Mechanism: [backup.md](../backup.md). Symptoms: `BackupHealthy=False` on
the cluster or policy, a `PgShardBackup` in `Failed`, or growing WAL on a
primary because `archive_command` keeps failing.

## Triage

```sh
kubectl get psc demo -o jsonpath='{.status.conditions[?(@.type=="BackupHealthy")]}'
kubectl get pgshardbackup --sort-by=.metadata.creationTimestamp
kubectl get pgshardbackup <name> -o jsonpath='{.status.groups}'   # which group, which error
```

On the failing group's primary:

```sh
kubectl exec <primary-pod> -c postgres -- pgbackrest \
  --config=/etc/pgbackrest/pgbackrest.conf --stanza=<cluster>-<group>-pg<major> info
kubectl exec <primary-pod> -c postgres -- \
  psql -U postgres -c "select * from pg_stat_archiver"
```

`pg_stat_archiver.failed_count` climbing with a recent `last_failed_wal`
means archiving is broken; backups will fail and WAL will accumulate
([disk-pressure](disk-pressure.md)).

## Common causes

- **Store unreachable / credentials.** The pgBackRest console output is in
  the backup's `status.groups[].error` and the agent log. Check the
  credential Secret keys for the store type, endpoint DNS from the pod,
  and `verifyTLS` against the store's certificate.
- **Missing stanza.** The agent creates stanzas idempotently and retries
  every 30s until the store is reachable; a persistent
  `unable to find stanza` means the repository path or credentials changed
  under an existing cluster.
- **Backup lock held.** A `PgShardBackup` stays `Pending` while another
  backup of the same cluster runs — pgBackRest holds one backup lock per
  stanza. Wait, or delete an abandoned backup object.
- **Operator restart mid-backup** fails the object with an explicit
  message; create a new one.
- **Retention failure** is reported in the `RetentionApplied` condition
  without failing the backup; usually store permissions for delete.

## ArchiveDuplicateError

`archive-push` refuses a WAL segment that already exists in the repository
with *different* content (re-pushing an identical segment is fine and
happens routinely after a pod restart re-drains the async spool). Two
writers are, or were, archiving different histories into one stanza:

1. **Two primaries on one group** (split brain window, or an `unhealthy`
   former primary still running with archiving on). Check
   `kubectl get pods -L pgshard.io/role` and the Lease; the fencing path
   normally shuts the old primary down — make sure it is actually gone.
2. **A second cluster reusing the stanza.** A restored cluster archives to
   its own new stanza by design; a hand-built cluster pointed at an
   existing prefix does not. Never point two clusters at the same
   `objectStore.prefix`.

Recovery: identify the correct current primary; verify its timeline
(`pg_controldata`), then take a **fresh full backup** so recovery no longer
depends on the contested segments. If the repository history is truly
mixed, move the stanza aside (new `prefix`) and re-run stanza creation plus
a full backup — do not delete repository segments by hand while any
cluster references them.

## After fixing

A cluster is `BackupHealthy` again once every scheduled type has a success
no older than one period past its due time. Force one immediately with an
ad-hoc `PgShardBackup` and confirm the archived WAL range advances in
`pgbackrest info`.
