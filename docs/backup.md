# Backups: pgBackRest, WAL archiving and object stores

Every member image ships pgBackRest. When a cluster's `spec.backup.policyRef`
names a `PgShardBackupPolicy`, the operator renders a `backup` section into
each member's agent config, the agent writes `/etc/pgbackrest/pgbackrest.conf`
and turns on WAL archiving, and `PgShardBackup` objects run `pgbackrest backup`
on the primary of every group.

## Stanzas

One stanza per replication group and PostgreSQL major:
`<cluster>-<group>-pg<major>` (`demo-catalog-pg18`, `demo-shard-0-pg18`).
A major upgrade therefore starts a fresh stanza; the old one stays in the
repository until its retention expires it.

The agent creates the stanza (`stanza-create`, falling back to
`stanza-upgrade` when the repository already holds one for a different system
identifier) once the primary is up, and retries every 30s until the store is
reachable. Standbys never touch the stanza. A newly promoted primary runs the
same idempotent step.

## Repository configuration

`pgbackrest.conf` is rendered by the agent from the policy (see
`internal/agent/backup`):

| Setting | Value |
|---|---|
| `repo1-type` | `spec.objectStore.type`: `s3`, `azure`, `gcs`, `posix`, `sftp` |
| `repo1-path` | `spec.objectStore.prefix` (default `/pgshard`) |
| `repo1-bundle`, `repo1-block` | `y` (bundled small files, block-level incrementals) |
| `repo1-cipher-type` / `repo1-cipher-pass` | `aes-256-cbc` with the `passphrase` key of `spec.objectStore.encryption.secretRef` |
| `repo1-retention-full` / `repo1-retention-diff` | `spec.retention.full` (default 2) / `spec.retention.differential` |
| `archive-async=y`, `spool-path=/var/lib/pgbackrest/spool` | asynchronous archive-push/get; the spool lives on the container filesystem |
| `log-level-console`, `log-level-file` | `spec.logLevel` (default `info`) |
| `process-max` | `spec.processMax` (default 2) |
| `pg1-path`, `pg1-port`, `pg1-socket-path`, `pg1-user` | the member's PGDATA, port, `/tmp` socket and `postgres` |

Store-specific options and the credential Secret keys mounted at
`/etc/pgshard-backup/credentials`:

| Store | Options | Secret keys (`credentials.secretRef`) |
|---|---|---|
| `s3` | `bucket`, `endpoint` (URL or host[:port]), `region`, `uriStyle` (`host`/`path`), `verifyTLS`, `credentialType` `shared` (default), `web-id`, `auto` | `key`, `keySecret` for `shared`; `web-id` reads `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` from the pod environment |
| `azure` | `container`, `endpoint`, `uriStyle`, `verifyTLS`, `credentialType` `shared` (default) or `sas` | `account`, `key` (shared key or SAS token) |
| `gcs` | `bucket`, `endpoint`, `verifyTLS`, `credentialType` `service` (default), `token`, `auto` | `key.json` for `service`; `token` for `token` |
| `posix` | `prefix` is the mounted directory | none |
| `sftp` | `sftp.host`, `sftp.user`, `sftp.port`, `sftp.hostKeyCheck` | `privateKey` |

Endpoints beginning with `http://` use plain HTTP (the in-cluster MinIO,
Azurite and fake-gcs test stores); anything else is TLS, verified unless
`verifyTLS: false`.

Attaching, changing or removing a policy changes the member pod template
(mounted Secrets, `archive_mode`), so the operator performs its usual rolling
restart, standbys first and the primary last after a switchover.

## PostgreSQL settings

The agent owns these settings; values from `spec.postgresql.parameters` are
ignored:

| Setting | With a policy | Without |
|---|---|---|
| `archive_mode` | `on` | `off` |
| `archive_command` | `pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=<stanza> archive-push %p` | unset |
| `restore_command` | `pgbackrest --config=... --stanza=<stanza> archive-get %f "%p"` (standbys and `pg_rewind` fetch archived WAL) | unset |

`archive_timeout` stays at 5 minutes, so the archive is at most that far
behind the primary. Only the primary archives (`archive_mode=on`, not
`always`); backups run from the primary as well (`backup-standby` needs a
`pg2-*` host and is not configured yet).

## Agent RPCs

`pgshard.v1.Agent` gained the backup surface (`proto/pgshard/v1/agent.proto`):

* `Backup{epoch, type full|diff|incr}` runs `pgbackrest backup` on the primary
  (a standby refuses) at the caller's epoch and returns the resulting set:
  label, type, prior, start/stop LSN, first/last WAL segment, sizes and
  timestamps, plus the last 50 console lines.
* `RestoreInfo` parses `pgbackrest info --output=json` for the member's stanza:
  status, archived WAL range and every backup set.
* `Expire{epoch}` applies retention; `Verify` checks the repository.

## Objects

### PgShardBackupPolicy

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardCluster
metadata:
  name: demo
spec:
  backup:
    policyRef: nightly
  # ...
---
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackupPolicy
metadata:
  name: nightly
spec:
  objectStore:
    type: s3
    bucket: pgshard-backups
    endpoint: s3.eu-west-1.amazonaws.com
    region: eu-west-1
    prefix: /demo
    credentials:
      secretRef: {name: backup-s3}
    encryption:
      secretRef: {name: backup-cipher}
  schedules:
    full: "0 2 * * *"
    incremental: "*/30 * * * *"
  retention:
    full: 7
    differential: 4
```

A policy lives in the clusters' namespace and any number of clusters may
reference it. Schedules are standard five-field cron expressions (plus
`@daily` style descriptors) evaluated in UTC by the operator; every tick
creates one `PgShardBackup` per bound cluster, named
`<policy>-<cluster>-<full|diff|incr>-<yyyymmdd-hhmm>` and owned by the policy,
unless that cluster's previous scheduled backup is still pending or running.

Policy status: `Valid` (store settings and cron expressions parse),
`BackupHealthy` aggregated over the bound clusters, and `status.clusters[]`
with each cluster's `lastFullTime`/`lastDifferentialTime`/
`lastIncrementalTime`, `healthy` and message. The cluster carries its own
`BackupHealthy` condition (`NoPolicy` when `spec.backup.policyRef` is empty,
`PolicyMissing` when it names no policy). A cluster is healthy when every
scheduled type has a success no older than one full period past its due time
(a full satisfies a differential and an incremental, a differential satisfies
an incremental); without schedules any completed backup counts. Both are
refreshed at least every minute.

### PgShardBackup

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardBackup
metadata:
  name: demo-before-upgrade
spec:
  clusterName: demo
  type: full        # full | differential | incremental
```

Phases: `Pending` (waiting for every group to have a ready primary, or for
another backup of the same cluster to finish: pgBackRest holds one backup
lock per stanza),
`Running`, `Completed`, `Failed`. The operator calls `Agent.Backup` on the
catalog primary and then each shard primary in order; a failing group stops
the run. `status.groups[]` carries per group the stanza, backup label, start
and stop LSN, first and last WAL segment, database and repository sizes,
timings and error; `status.backupId` is the catalog label once every group
completed. `pgbackrest expire` runs on every group after its backup; a
retention failure is reported in the `RetentionApplied` condition without
failing the backup. An operator restart during a run fails the object with an
explicit message; create a new one.

## Local object stores

`hack/objectstores/k8s` deploys MinIO, Azurite and fake-gcs-server into the
`objectstores` namespace with bucket-creation Jobs; the `backup` e2e suite
(`go test -tags e2e ./test/e2e/backup/...`) runs one small cluster per store,
takes a full and an incremental backup, checks the archived WAL range through
`pgbackrest info` and waits for a scheduled incremental and `BackupHealthy`.
`E2E_BACKUP_STORES=s3` restricts the suite to one store.

## Not yet covered

* Restore (`PgShardRestore`) and point-in-time recovery.
* Backups from a standby (`backup-standby`).
* A persistent spool volume for `archive-async` (the spool is on the container
  filesystem, so a pod restart re-pushes the pending segments).
