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
* `ListTransactionDecisions` (catalog primary, read-only) returns
  `pgshard.xact_decisions`; `ReconcilePreparedTransactions{epoch, shard_id,
  decisions}` finishes the instance's `pgshard-*` prepared transactions
  against that log (each from the database it was prepared in) and reports
  committed, rolled back and contradictions; `SetWriteFence{epoch, active,
  reason}` raises or releases the catalog write fence;
  `ListPreparedTransactions` (read-only) lists the `pgshard-*` transactions
  the instance still holds prepared. See
  [Barrier restore](#barrier-restore).

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
  barrierSchedule: "0 * * * *"
  # controllerEndpoint: "{cluster}-controller.{namespace}.svc:15500"
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
`barrierSchedule` ticks instead ask each bound cluster's controller for a
[certified barrier](#certified-barriers) named
`<policy>-<cluster>-<yyyymmdd-hhmm>`. The operator dials that controller
with the client certificate given by its `--controller-tls-cert`,
`--controller-tls-key` and `--controller-tls-ca` flags; without them it
dials plaintext, which only a controller run with `--insecure-dev` accepts.

Policy status: `Valid` (store settings and cron expressions parse),
`BackupHealthy` aggregated over the bound clusters, `BarrierHealthy` (only
with a `barrierSchedule`: `True` once the last tick reached every bound
controller, `False` with the joined errors, `Unknown` before the first
tick), and `status.clusters[]`
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
`pgbackrest info`, waits for a scheduled incremental and `BackupHealthy`, then
restores the cluster twice: to a named restore point pinned to the incremental
backup and to a point in time, checking rows and timelines on the new clusters.
`E2E_BACKUP_STORES=s3` restricts the suite to one store.

## Restore

A `PgShardRestore` builds a **new** cluster from the repository; the source
cluster is never touched:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardRestore
metadata: {name: before-purge, namespace: prod}
spec:
  clusterName: orders           # source; its stanzas are read
  newClusterName: orders-pitr   # created by the operator
  backupId: orders-full-1       # a PgShardBackup name (per-group sets) or a raw label
  target: {name: before-purge}  # or time / lsn / xid / immediate; unset = end of WAL
  # clusterSpec: {...}          # optional; defaults to the source spec
```

The operator creates `PgShardCluster/orders-pitr` (spec copied from the
source unless `clusterSpec` is given, `spec.backup.policyRef` defaulting to
the source's so the repository is reachable) with the label
`pgshard.io/restored-from` and the annotation `pgshard.io/restore-source`
(source cluster, PostgreSQL major, per-group backup labels and the target).
The new cluster must keep the source's shard count and major; replica counts
and resources may change. The source's superuser Secret is copied to
`<newCluster>-superuser`, because the restored catalog carries the source's
roles and passwords.

Each group's designated primary bootstraps from the repository instead of
`initdb`: `pgbackrest restore --stanza=<source stanza> [--set <label>]
--type=<default|time|lsn|name|xid|immediate> [--target ...] [--target-timeline]
[--target-exclusive]` into the empty PGDATA, then archive recovery with
`restore_command` fetching WAL from the source stanza and the
`recovery_target_*` settings rendered by the agent (`recovery_target_action =
promote`). Once `pg_is_in_recovery()` is false the instance is stopped and
started again as a normal primary; the standbys clone from it as usual. While
recovering `archive_mode` is `off`, and afterwards the group archives to its
**own new stanza** (`<newCluster>-<group>-pg<major>`); the source stanza is
only ever read. A marker beside PGDATA makes an interrupted restore start over
from an empty directory rather than run a half-restored instance.

The same target applies to every group; because a restore point or a
timestamp is only meaningful where it exists in the WAL, create restore
points on every group (`Agent.CreateRestorePoint`) before relying on a name
target. Time and LSN targets let pgBackRest select the backup set; name, xid
and immediate targets need `backupId`. When `backupId` names a completed
`PgShardBackup` each group restores its own set from that run; any other
value is used as the pgBackRest label for every group.

Status: `Pending` → `Restoring` (cluster created) → `Recovered` when every
primary left recovery and the cluster is `Ready` (the operator then removes
the `pgshard.io/restore-source` annotation, so a member that later starts
with an empty PGDATA does not restore the source's old data again), with per-group
`sourceStanza`, `backupId`, `timeline` and `reachedTarget`; `Failed` on
invalid specs, a missing source, an incomplete backup, an existing cluster of
that name, a primary that crash-loops (PostgreSQL refuses to start when the WAL
ends before the target) or after four hours.

### Replica re-clone from the repository

`Agent.Reclone{source_kind: BACKUP}` rebuilds a standby with `pgbackrest
restore --delta --type=standby` from its own stanza (files that match the
backup are kept), drops stale slots, writes `standby.signal` and streams from
the primary. The operator sets `recloneFromRepo` in the agent config once a
completed backup exists for the cluster; a rejoining former primary whose
`pg_rewind` fails then restores from the repository and only falls back to
`pg_basebackup` when the repository cannot serve it.

## Certified barriers

A barrier is a cluster-consistent restore point. `Controller.CreateBarrier`
(and `PgShardBackupPolicy.spec.barrierSchedule`, a cron expression the
operator turns into `CreateBarrier` calls against
`spec.controllerEndpoint`, default `{cluster}-controller.{namespace}.svc:15500`,
one bound cluster after another) runs `controller.Barrier`:

1. raise `pgshard.shard_map_generation.write_fence` — routers hold new writes
   and two-phase commits (see [router.md](router.md#write-fence));
2. drain: run a resolver pass and wait until no `xact_decisions` row is
   `preparing` and no group holds a `pgshard-*` prepared transaction
   (`--barrier-drain-timeout`, 30s);
3. `pg_create_restore_point('pgshard-<name>')` on the catalog and every shard
   primary (through the resolver's shard DSNs), followed by `pg_switch_wal()`,
   recording LSN, timeline and WAL segment;
4. wait until `pg_stat_archiver.last_archived_wal` of every group covers its
   segment (`--barrier-archive-timeout`, 2m; a group with `archive_mode=off`
   fails the barrier);
5. verify no decision row was created since the fence was raised (a router
   that had not yet seen the fence), then insert `pgshard.restore_points`
   `{name, shard_map_generation, per_group, certified: true}`;
6. release the fence — on every failure path too, and nothing is recorded.

`Controller.ListBarriers` (`certified_only`) lists them newest first; the
restore point name every group shares is `pgshard-<name>`.

**What a certified barrier covers, and what it does not.** The pause is
`default_transaction_read_only` on every primary. The router refuses the
statements that would turn it off for a session -- `SET` of
`default_transaction_read_only` or `transaction_read_only`, and `set_config`
of either -- and neutralises the transaction-mode spellings of the same
thing, `BEGIN READ WRITE` and its relatives, by dropping the mode (see
[router.md](router.md#write-fence)). `READ ONLY` in any form still works, and
a plain `BEGIN` is writable whenever no pause is running.

That covers everything reaching a shard through pgshard. It does not cover a
connection made straight to a member's PostgreSQL: `pg_hba` admits only the
control plane's own roles there -- the superuser, and the router's catalog
role on the catalog group (see [operator.md](operator.md#member-pods)) -- and
a superuser can turn the pause off for itself. A superuser writing on a member during a
barrier is outside the certification contract -- step 5 above is what keeps
such a write from being certified rather than silently included: the barrier
fails instead.

A pause does not outlive the primary that holds it. It is an `ALTER SYSTEM`,
so it lives in `postgresql.auto.conf`, which the agent rewrites on bootstrap,
on promotion and after a restore: a primary that restarts, or a standby
promoted mid-barrier, comes back without it. Step 5 is what makes that safe
-- the barrier re-checks that every group still refuses writes and fails
rather than certifying a point that is not consistent -- so a barrier taken
across a restart or a failover fails and is retried; it does not certify a
bad point.

### Barrier restore

`PgShardRestore.spec.target.barrier: <name>` (with `backupId` naming a backup
taken before the barrier) restores every group to `recovery_target_name
pgshard-<name>` and then, once every primary promoted and the cluster is
Ready, enters phase `Reconciling`: the restored catalog still carries the
write fence the barrier raised, so routers of the new cluster refuse writes
until the operator has

1. read `pgshard.xact_decisions` through the catalog primary's agent
   (`Agent.ListTransactionDecisions`);
2. run `Agent.ReconcilePreparedTransactions` on every shard primary: a
   prepared `pgshard-*` transaction is committed when the log says commit
   and rolled back otherwise (abort, still preparing, or no row); a
   commit-decided transaction the shard does not hold prepared must read
   `committed` in `pg_xact_status(participant_xid)` — anything else is a
   contradiction;
3. released the fence (`Agent.SetWriteFence`) — only when there is no
   contradiction. Any contradiction sets phase `Failed` with the offending
   `group: gid` pairs in `status.error` and `status.reconciliation` and leaves
   the cluster fenced; nothing proceeds silently.

`status.reconciliation` reports decisions, committed, rolledBack,
contradictions and unfenced. The decision table lives in
`internal/twopc` (unit-tested against a fake `pg_prepared_xacts` view);
`TestRestoreReconciliationMatrix` (agent, `-tags integration`)
restores a shard to points before PREPARE, between PREPARE and the decision
and after it, and checks each outcome.

### Non-barrier restores are not cluster-consistent

A time, LSN, xid, name or immediate target is applied to every group
independently: each group stops at its own point, and nothing proves the
points agree on which two-phase transactions had committed. Such a restore
is per-group PITR, not a cluster snapshot. After it reaches `Recovered`
the operator asks every primary's agent for the `pgshard-*` transactions it
still holds prepared (`Agent.ListPreparedTransactions`) and reports them:
the condition `PreparedTransactionsPending` is `True` with the `group: gid`
pairs and each group lists them in `status.groups[].preparedTransactions`;
`Unknown` when a primary could not be asked; `False` when none are left.
Leftover prepared transactions hold their locks and pin the vacuum horizon
until finished by hand (`COMMIT PREPARED` / `ROLLBACK PREPARED` in the
database they were prepared in), and if the target lies inside a barrier
window the restored catalog may still carry that barrier's write fence
(`Agent.SetWriteFence` releases it). The operator never finishes them
itself without a decision log: use a barrier target for a consistent
restore.

## Not yet covered

* Backups from a standby (`backup-standby`).
* A persistent spool volume for `archive-async` (the spool is on the container
  filesystem, so a pod restart re-pushes the pending segments).
