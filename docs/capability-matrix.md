# Capability matrix

Honest status of every capability against what is merged on `main`.
**Implemented** means code plus tests are merged; **Partial** means a
usable subset is merged and the gap is named; **Planned** means design or
scaffolding only. Where a document and the code disagree, the code and its
tests are authoritative.

## Cluster provisioning and HA

| Capability | Status | Where |
|---|---|---|
| PostgreSQL 18 and 19 images built from source (pgBackRest, agent as PID 1, pooler) | Implemented | `postgres/`, `docker-bake.hcl`; `19` is Beta 3 ([crd.md](crd.md)) |
| `PgShardCluster` CRD: catalog + shard groups, pods, PVCs, Services, PDBs | Implemented | `api/v1alpha1`, `internal/operator/reconciler.go`, [crd.md](crd.md) |
| Agent lifecycle: initdb/clone, rewind rejoin, lease fencing, self-fence on isolation | Implemented | `internal/agent`, [ha.md](ha.md) |
| Automatic failover with lease fencing, epoch publication, durability-over-availability candidate rule | Implemented | `internal/operator/failover.go`, [ha.md](ha.md#failover) |
| Planned switchover (`pgshard.io/switchover` annotation) | Implemented | `internal/operator/failover.go` |
| Rolling restarts, live reloads, restart-vs-reload classification | Implemented | `internal/operator/rollout.go`, [ha.md](ha.md#rolling-operations) |
| Online PVC growth and StorageClass change via member rebuild | Implemented | `internal/operator/rollout.go` |
| Automatic tuning from resources and profile, memory-budget invariant | Implemented | `internal/pgtune`, [tuning.md](tuning.md) |
| Pooler sidecar per member; router Deployment + HPA + PDB; admin and controller Deployments | Implemented | `internal/operator/router.go`, `admin.go`, `controller.go` |
| Controller deployed by the operator (Deployment/Service per cluster) | Implemented | one leader-elected replica plus the `{cluster}-controller` Service that barriers, workflow calls and backup policies already resolve; `internal/operator/controller.go`. Router `--vstream-listen`/`--controller` and pooler `--stream-dsn` are wired, and the Deployment carries the shard and subscription DSN templates without which the resolver does not start and a reshard has nothing to subscribe to. `ControllerReady` reports it, alongside `RouterReady`, `Fenced` and `ServingWrites`, all four of which were declared and unset before. Still open: a dedicated identity beyond the ServiceAccount (PGS-495, folded into PGS-591) |
| mTLS router↔pooler in operator-deployed clusters | Partial | wired when you supply the material: set `spec.internalTLS.secretRef` and the operator mounts it and passes `--pooler-tls-*` (router), `--tls-*` (pooler, controller). Without it the same wiring passes `--insecure-dev`, so the default is plaintext. What is missing is operator-**issued** certificates and rotation, not the plumbing ([operator.md](operator.md)) |
| mTLS operator↔agent | Planned | the agent's gRPC server registers auth interceptors but no transport credentials, and both callers (`operator/agentclient.go`, `controller/materialize.go`) dial with `insecure.NewCredentials()` unconditionally. The bearer token therefore travels in clear |

## Catalog and placement

| Capability | Status | Where |
|---|---|---|
| `pgshard` catalog schema, checksummed migrations, desired/status split, NOTIFY | Implemented | `internal/catalog`, [catalog.md](catalog.md) |
| PostgreSQL extended-hash port (bit-exact, differential-tested vs 18 and 19) | Implemented | `internal/placement`, [placement.md](placement.md) |
| Numeric / floating-point shard keys | Planned | refused by design until the numeric hash is ported |
| Router snapshots, watcher (LISTEN/NOTIFY + reload), generation fencing | Implemented | `internal/catalog/snapshot` |
| Controller leader election (catalog advisory lock) and desired-state reconcile | Implemented | `internal/controller/reconcile.go` |

## Router: query serving

| Capability | Status | Where |
|---|---|---|
| PostgreSQL wire protocol (simple + extended), SCRAM auth from catalog verifiers | Implemented | `internal/pgwire`, `internal/router`, [router.md](router.md) |
| Shard-aware planner: keyed routing, bind-time routing, search-path resolution, loud refusal list | Implemented | `internal/router/plan` (200+ golden plans) |
| Scatter reads: streaming merge, ORDER BY, LIMIT/OFFSET pushdown, count/sum/min/max, key-covering GROUP BY/DISTINCT | Implemented | `internal/router/scatter`, differential-tested vs an oracle PostgreSQL |
| Colocated joins: sharded tables joined on their shard key, and joins to a reference table | Implemented | `internal/router/plan`, differential-tested vs an oracle PostgreSQL |
| Cross-shard joins (not on the shard key, or an unsharded table), subqueries, CTEs, window functions, `avg()` in scatter | Planned | refused with `0A000` and a hint |
| Scatter UPDATE/DELETE, `INSERT ... SELECT`, multi-row INSERT spanning shards, COPY on sharded tables | Planned | refused with `0A000` |
| Reference tables: fan-out writes in one 2PC, volatile-function refusal | Implemented | `internal/router` ([router.md](router.md#reference-tables)) |
| Global sequences from the catalog (block allocation, INSERT rewrite, `nextval`) | Implemented | migration `0005`, [router.md](router.md#sequences) |
| Session state replay over transaction pooling; cancel; COPY (unsharded); drain on SIGTERM; peer cancel forwarding | Implemented | `internal/router`, `cancelpeer` |
| Failover buffering (`--buffer-window`), `40001` in-transaction, write fence (`57P03`) | Implemented | [router.md](router.md#operations) |
| Cross-shard consistent snapshots (scatter under REPEATABLE READ / SERIALIZABLE) | Planned | refused |
| PostgreSQL 19 grammar for the planner (mixed-major clusters mid-upgrade) | Planned | planner binds the PostgreSQL 18 grammar (`internal/pgparser`) |

## Transactions

| Capability | Status | Where |
|---|---|---|
| Single-shard transactions (plain COMMIT, readers rolled back, writer promotion check) | Implemented | `internal/router` |
| Two-phase commit with durable decision log; `08007` in-doubt semantics | Implemented | `internal/router`, `pgshard.xact_decisions`, crash matrix in `test/e2e/router` |
| In-doubt resolver (controller): stale-preparing abort, orphan sweep, decision completion | Implemented | `internal/controller/resolver.go` |
| `pgshard.transaction_mode = single` | Implemented | [router.md](router.md#modes) |
| Savepoints in multi-shard transactions | Planned | refused with `0A000` |

## DDL, DCL and roles

| Capability | Status | Where |
|---|---|---|
| Online DDL/DCL fan-out as migrations; idempotent per-shard resume; sync + async wait | Implemented | `internal/controller/applier.go`, [ddl.md](ddl.md) |
| Weaker-lock strategies (NOT VALID+VALIDATE, concurrent index PK/UNIQUE, DETACH CONCURRENTLY) | Implemented | [ddl.md](ddl.md) |
| Rewrite-class DDL (`ALTER COLUMN ... TYPE`, volatile-default ADD COLUMN, ...) — online schema change | Implemented | OID-preserving column duplication with trigger backfill; router hides the working column. **No rollback window once cutover starts**: cutting a shard over drops the old column, so a failure part-way leaves shards on both sides and the old values recoverable only from a backup ([online-ddl.md](online-ddl.md#failure-revert-and-gc)); `internal/controller/rewrite.go`, `internal/router/plan/hide.go` |
| Cluster-wide roles/grants with one SCRAM verifier, drift detection and repair | Implemented | `internal/controller/roles.go`, [roles.md](roles.md) |
| Superuser/replication/BYPASSRLS role management through the router | Planned | refused by design; manage on the shards directly |

## Backup, restore, barriers

| Capability | Status | Where |
|---|---|---|
| pgBackRest per-group stanzas, WAL archiving, s3/azure/gcs/posix/sftp, encryption, retention | Implemented | `internal/agent/backup`, [backup.md](backup.md) |
| Scheduled and ad-hoc backups (`PgShardBackupPolicy`, `PgShardBackup`), health conditions | Implemented | `internal/operator/backup*.go` |
| Restore to a new cluster: PITR targets, per-group sets, replica re-clone from repository | Implemented | `internal/operator/restore*.go` |
| Certified barriers (write fence, 2PC drain, archived restore points) and barrier restore with reconciliation | Implemented | `internal/controller/barrier.go`, `internal/twopc` |
| Backup from a standby; persistent archive spool volume | Planned | [backup.md](backup.md#not-yet-covered) |

## Change streams

| Capability | Status | Where |
|---|---|---|
| pgoutput v4 decoder (streamed + two-phase), golden captures from 18 and 19 | Implemented | `internal/pgoutput` |
| Failover slots synchronized to standbys; `synchronized_standby_slots`; slot monitor into the catalog | Implemented | `internal/agent`, `internal/controller/streams.go`, [streams.md](streams.md) |
| Pooler `Stream`/`Ack`/`CopyTables` | Implemented | `internal/pooler` |
| VStream: merged positioned stream over all shards, VGtid resume, two-phase events, failover continuity | Implemented | `internal/router/vstream` |
| Initial copy (exported snapshots, per-table checkpoints, kill-resume) | Implemented | `TestRouterVStreamInitialCopy` |
| Consumer contract: publicly importable stubs and a compatibility policy | Planned | the wire service `pgshard.v1.VStream` works and can be consumed by generating stubs from `proto/`, but pgshard's own generated Go stubs are under `internal/gen`, which another module cannot import, and the `v1` in the protobuf package is not a stability guarantee while the Kubernetes API is `v1alpha1` ([streams.md](streams.md), PGS-394) |
| Reshard journal events (`Error{RESHARDED}` / `Journal`) | Partial | the cutover writes journal rows (`pgshard.resharding_journal`, `cutoverpg.go`); the stream still synthesises its event from the shard-map generation change and its own participant list (`vstream/merge.go`) rather than reading those rows |

## Resharding and upgrades

| Capability | Status | Where |
|---|---|---|
| In-place range edit on the SERVING set drives a reshard | Planned | the edit commits and records a `pending` workflow, but nothing transitions it to `running`, so no data moves; `spec.shards` is the working path ([resharding.md](guide/resharding.md), PGS-508) |
| Shard-range and re-key edits detected; `reshard`/`table_rekey` workflows recorded; pause/resume RPCs | Implemented | `internal/controller/reconcile.go`, `internal/controller/placement.go` |
| Data movement, traffic switch, reverse replication, old-group retirement | Implemented | `internal/controller/copy.go` (`Copier.Pass`), `cutover.go`, `cutoverpg.go`; e2e `reshard`, `reshard-split`, `reshard-merge` under write load |
| `PgShardReshard` CRD and `spec.resharding.*` knobs | Implemented | `ClusterReconciler.reconcileReshard` provisions and retires target sets; `pauseBefore`/`proceed` gate the cutover ([resharding.md](resharding.md)) |
| Table placement changes (unsharded ↔ sharded, → reference, re-key) | Implemented | `internal/controller/placement.go`, `placementpg.go`; shadow build, catch-up, table-scoped swap |
| Major upgrade 18→19, online (blue/green via logical replication) | Implemented | triggered by a `spec.postgresql.major` change; `internal/controller/upgrade.go`, `internal/operator/catalogupgrade.go`; e2e `upgrade` on 18→19 ([upgrade.md](upgrade.md)) |
| Major upgrade, offline (`pg_upgrade --link`) | Planned | the online strategy is the only one implemented |
| Independent keyspaces within one cluster | Not planned | a shard set is a generation of one cluster-wide map, so every database shares the serving generation and a reshard's flip fences writes cluster-wide for its cutover window; isolation between unrelated workloads is a separate `PgShardCluster` ([resharding.md](resharding.md#one-cluster-is-one-keyspace), PGS-485) |

## Observability and operations

| Capability | Status | Where |
|---|---|---|
| Admin UI: topology, backups/restores/restore points, migrations, streams; JSON APIs; SSE live updates | Implemented | `internal/admin`, [admin.md](admin.md) |
| Admin UI authentication | Implemented | a credential on every route but `/healthz` ([admin.md](admin.md)) |
| Router metrics endpoint | Implemented | Prometheus metrics across router, pooler, controller and agent; [observability.md](observability.md), `internal/metrics` |

## Testing and CI

| Capability | Status | Where |
|---|---|---|
| `make verify` (fmt, vet, lint, race tests, build), buf lint + proto drift, govulncheck, gitleaks, action policy | Implemented | `.github/workflows/ci.yml`, [ci.md](ci.md) |
| e2e on kind: smoke, operator, backup (both majors); router integration suites in Docker | Implemented | `test/e2e` |
| Chaos suite | Partial | pipeline + noop experiment only; the catalogue in `test/chaos/README.md` is planned |
| Performance: benchstat gate on PRs; router `select 1` benchmark | Partial | `hack/perf`, `test/perf`; no published parity numbers vs single-node PostgreSQL yet |
