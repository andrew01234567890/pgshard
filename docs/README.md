# pgshard documentation

## Start here

- **[Capability matrix](capability-matrix.md)** — what is implemented,
  partial and planned, with pointers to code and tests.

## User guide

| Page | Covers |
|---|---|
| [Getting started](guide/getting-started.md) | Install the operator, create a `PgShardCluster`, connect with psql |
| [Defining sharding](guide/sharding.md) | Databases, sharded/reference tables, shard ranges, global sequences — all as SQL on the catalog |
| [Queries, DDL and DCL](guide/queries.md) | What routes where, scatter reads, reference tables, online DDL, the refusal list |
| [Transactions](guide/transactions.md) | Single-shard commit, two-phase commit, in-doubt semantics, retry guidance |
| [Backups and restore](guide/backup-restore.md) | Object stores, schedules, PITR, certified barriers |
| [Resharding](guide/resharding.md) | Declaring range and key changes; current workflow status |
| [Major upgrades](guide/upgrades.md) | 18→19: what exists, what is planned |
| [Admin UI](guide/admin-ui.md) | Topology, backups, migrations, streams panels |
| [Router autoscaling](guide/router-autoscaling.md) | HPA, drain, why scale-down is safe |

## Runbooks

| Runbook | When |
|---|---|
| [Failover and switchover](runbooks/failover-and-switchover.md) | Primary loss, planned primary moves, refused failovers |
| [Rolling restarts and config changes](runbooks/rolling-restarts.md) | Settings, images, stuck rollouts |
| [PVC and StorageClass changes](runbooks/storage-changes.md) | Volume growth, class migration |
| [Backup failures](runbooks/backup-failures.md) | `BackupHealthy=False`, ArchiveDuplicateError |
| [In-doubt transactions](runbooks/in-doubt-transactions.md) | `08007`, lingering prepared transactions |
| [Stuck workflows](runbooks/stuck-workflows.md) | Reshard/re-key/upgrade workflows: pause, cancel, back out |
| [Slot invalidation](runbooks/slot-invalidation.md) | `POSITION_TOO_OLD`, lost slots, consumer re-baseline |
| [Disk pressure](runbooks/disk-pressure.md) | Full volumes, `max_slot_wal_keep_size`, WAL retention |
| [Restore to a certified barrier](runbooks/restore-to-barrier.md) | Disaster recovery, reconciliation, non-barrier caveats |
| [pgBackRest and PostgreSQL minor upgrades](runbooks/pgbackrest-and-minor-upgrades.md) | Image bumps and repository compatibility |

## Component references

| Page | Component |
|---|---|
| [catalog.md](catalog.md) | Catalog schema, desired/status tables, snapshots |
| [crd.md](crd.md) | `pgshard.io/v1alpha1` custom resources |
| [operator.md](operator.md) | Operator flags, member pods, router tier |
| [ha.md](ha.md) | Agents, failover, switchover, rolling operations |
| [tuning.md](tuning.md) | Automatic PostgreSQL tuning |
| [backup.md](backup.md) | pgBackRest, stanzas, restore, certified barriers |
| [router.md](router.md) | Wire protocol, planner, scatter, transactions, operations |
| [ddl.md](ddl.md) | DDL/DCL migration model and strategies |
| [roles.md](roles.md) | Cluster-wide roles, grants, drift repair |
| [pooler.md](pooler.md) | Per-shard pooler, SCRAM passthrough, fencing |
| [placement.md](placement.md) | Extended hash port, key ranges, controller reconcile |
| [resharding.md](resharding.md) | Reshard and placement workflows: stages, cutover steps, journal, retirement |
| [upgrade.md](upgrade.md) | Online major upgrade: group replacement, catalog upgrade, rollback window |
| [streams.md](streams.md) | Logical decoding, slots, VStream, initial copy |
| [admin.md](admin.md) | Admin UI pages, deployment, security |
| [pgparser.md](pgparser.md) | Bound PostgreSQL grammar |
| [ci.md](ci.md) | Workflows and image publishing |

## Decisions

Architecture decision records say why the system is shaped the way it is,
and what was rejected. They are historical: a superseded decision is
replaced by a later record rather than edited.

| ADR | Decision |
|---|---|
| [0001](adr/0001-durability-guc-enforcement.md) | Enforcing the durability floor against code the router cannot read |
| [0002](adr/0002-revocation-during-authentication.md) | What a client learns when a revocation lands mid-authentication |
| [0003](adr/0003-run-postgresql-ourselves.md) | Running PostgreSQL ourselves rather than delegating to another operator |
| [0004](adr/0004-route-as-the-real-user.md) | Connecting to shards as the client's own role |
| [0005](adr/0005-postgresql-own-hash-for-placement.md) | Placing rows with PostgreSQL's own extended hash |
| [0006](adr/0006-no-etcd.md) | Keeping coordination in Kubernetes and the catalog, not in etcd |
| [0007](adr/0007-libpg_query-per-major.md) | Parsing with libpg_query, pinned per PostgreSQL major |
| [0008](adr/0008-native-logical-replication-for-resharding.md) | Moving data with native logical replication |
| [0009](adr/0009-oid-preserving-online-rewrites.md) | Rewriting tables in place rather than swapping them |
| [0010](adr/0010-everything-automatic.md) | Making transitions automatic, with pauses as the opt-in |
| [0011](adr/0011-catalog-tables-are-the-control-plane.md) | Declaring the shard layout as SQL in the `pgshard` database |
| [0012](adr/0012-postgresql-images-from-source.md) | Building PostgreSQL images from source, per major, with a patch series |
| [0013](adr/0013-crd-api-group.md) | `pgshard.io` as the CRD API group |
| [0014](adr/0014-postgresql-18-and-19.md) | Targeting PostgreSQL 18 and 19, and nothing older |

Component references are design-plus-behaviour documents; where one
disagrees with the code, the code and its tests are authoritative, and the
[capability matrix](capability-matrix.md) is the summary of record.
