# Milestone 1: usable PostgreSQL sharding MVP

## Outcome

Deliver a Linux-container-only PostgreSQL 18 sharding product with a Rust SQL
pooler/router and a Go Kubernetes operator. A user can create independently
sharded logical databases, connect through read-write and read-only Services,
operate them through routine failures, back them up from standbys to S3, restore
an exact database backup under a configurable name, and move or reshard that
database online.

This is the implementation plan, not a statement that every item exists. The
current evidence is maintained in
[`website/docs/project/status.md`](../website/docs/project/status.md).

## Fixed decisions

- PostgreSQL 18 is the minimum and only Milestone 1 major. Only the published
  Linux container configuration is supported.
- The data plane is Rust. `pgshard-pooler` owns SQL pooling, routing, query
  buffering, catalog caching, distributed-transaction driving, and supervised
  `pgoutput` stream/materialization workers. There is no standalone
  `pgshard-stream` Deployment.
- The Kubernetes operator is Go and configures the complete supported cluster:
  cells, PostgreSQL, replication, poolers, orchestrators, Services, tuning,
  backup, observability, disruption policy, and autoscaling.
- `shardschema` is a dedicated PostgreSQL database on physical `cell-0000` for
  Milestone 1. Poolers LISTEN for commit hints, poll for loss recovery, validate
  complete snapshots, and cache immutable per-database routing epochs.
- A fleet contains physical PostgreSQL cells. Each logical database has its own
  database-scoped shard map and can use a different shard count. Databases can
  share cells, use separate cells on shared Nodes, or use dedicated cells.
- The lowest-ID participating database shard is the durable 2PC coordinator;
  its coordinator row resides in that shard's current physical cell. Cross-shard
  transactions are atomic and durable but support `READ COMMITTED` only. They do
  not claim a global snapshot, serializability, or simultaneous cross-shard
  visibility.
- Public change streams and reshard materializers prefer standby-local
  `pgoutput` decoding slots. Primary failover anchors are synchronized to
  standbys using PostgreSQL 18 slot synchronization. Eligible standbys require
  `hot_standby_feedback=on`; unsafe feedback, WAL retention, or slot-sync state
  fences the consumer rather than silently falling back.
- A restore always recreates the exact logical topology recorded by its backup.
  A topology override is forbidden. Any shard count, ordinal, range, hash
  version, or hash-seed mismatch returns permanent
  `RestoreTopologyMismatch` before any non-status mutation.
- A restored destination always gets a fresh logical-database UUID and fresh
  database-shard UUIDs and restore incarnations. Source identities remain
  immutable provenance. Source `A` may restore as `B`; it may restore as `A`
  only when that name is absent. Replacing an existing `A` is a later explicit
  online move.
- Milestone 1 restore materializes into new dedicated cells. Shared or explicit
  placement is reached with a subsequent online move. This avoids attaching
  quarantined physical restore bytes to a serving shared cell.
- DDL, role, grant, move, and reshard activation are catalog-versioned. Pooler
  acknowledgement is not sufficient: PostgreSQL-side generation fences reject
  stale writes and prepares. Existing sockets remain generation-pinned and are
  drained or failed for reconnect, never silently rebound.
- Pooler replica count may be fixed or controlled by an HPA. Availability
  controls and query buffering reduce restart and cutover interruption, but
  transaction/session semantics that cannot be replayed receive an explicit
  retry or disconnect outcome.
- Runtime images are test artifacts in Milestone 1. CI builds them but does not
  push images to a registry.

## Workstreams

### 1. Catalog and per-database topology

- Define physical cells, logical databases, database-scoped shards, placements,
  topology generations, routing generations, database generation fences,
  backup sets, retention pins, restore operations, move journals, DDL journals,
  role/grant epochs, and durable 2PC coordinator records in `shardschema`.
- Keep global and per-database identities distinct. Never reuse retired UUIDs,
  generations, slot names, GIDs, or operation IDs.
- Make every state transition idempotent with a canonical request envelope,
  monotonic generation, explicit terminal state, and replay/conflict behavior.
- Preserve the existing validated snapshot/cache boundary and add per-database
  limits, checksums, LISTEN hints, polling, stale-cache fences, and metrics.
- Add forward-only PostgreSQL 18 migrations with exclusive bootstrap access,
  hostile-state validation, live upgrade tests, and exact rollback behavior.

Acceptance:

- Two logical databases with different shard counts coexist in one fleet.
- `B` can occupy the first three compatible cells used by five-shard `A`, or
  move to completely separate dedicated cells without changing `A`'s epochs.
- A malformed, partial, or topology-mismatched restored catalog fails before
  migration or inventory mutation.

### 2. PostgreSQL cells and high availability

- Reconcile one primary and the configured standbys per cell, synchronous
  durability policy, physical slots, fencing, promotion, rewind/reseed, and
  role-aware recovery.
- Use immutable, content-addressed operator configuration and resource-derived
  PostgreSQL tuning. Pin durability, WAL, connection, memory, worker, slot,
  feedback, timeout, and checkpoint settings to safe supported values.
- Reject unsupported persisted server configuration and recovery state before
  a managed primary starts. Keep bootstrap credentials outside running
  containers and disable unsupported `ALTER SYSTEM` overrides.
- Implement agent/orchestrator leases, Node-incarnation fencing, promotion
  proofs, split-brain prevention, and bounded recovery.
  - Implemented foundation: each orchestrator owns a unique Pod-incarnation key
    under a persistent cluster-name/UID marker through bounded leader-required
    etcd gateway requests. Cluster identity and revision are pinned, and a
    monotonic lease deadline controls readiness across process pauses.
  - Remaining: authenticated etcd transport, durable operation and shard-term
    records, coordinator assignment, target-side epoch enforcement, agent
    self-fencing, promotion proofs, and automated recovery.
- Provide CNPG-style Services for writer/read-write, read-only, and any-instance
  access with selectors or operator-owned EndpointSlices that reflect proven
  roles.
- Implement staged rolling restart and configuration rollout. Drain poolers,
  fail over or replace one cell member at a time, preserve quorum, and prove
  application endpoints remain available where topology permits.

Acceptance:

- Loss and replacement of any one member does not create two writable primaries.
- Standby promotion preserves acknowledged writes under the configured policy.
- Rolling PostgreSQL, pooler, and operator restarts under continuous load have
  no unexplained failed request; documented non-replayable sessions receive the
  specified outcome.

### 3. Rust pooler and SQL routing

- Complete PostgreSQL startup/authentication, TLS, SCRAM, backend connection
  pools, cancellation routing, session state, simple and extended query cycles,
  prepared statement/portal virtualization, and transaction pinning.
- Route supported single-shard SQL from the immutable `shardschema` snapshot.
  Reject unknown schemas, ambiguous shard keys, stale epochs, unsupported SQL,
  and cross-database transactions before dispatch.
- Implement read-write, read-only, and any-instance policies with health,
  consistency, lag, transaction, and failover awareness.
- Buffer bounded eligible requests during pooler rollout, DDL activation,
  database move, and reshard cutover. Never replay a request after an unknown
  commit or across incompatible session/transaction state.
- Support fixed replicas and HPA CPU/memory/custom-metric scaling, topology
  spread, disruption budgets, readiness drain, and bounded pre-stop behavior.
- Export Prometheus metrics for connections, pools, routing, shards, catalog
  age, queueing, buffering, retries, errors, 2PC, DDL, reshard, streams, and
  latency. Emit OpenTelemetry traces with bounded cardinality and no SQL
  literals, secrets, or bind values.

Acceptance:

- Supported SQL can be executed through the public Services against multiple
  shards and independently sharded databases.
- A pooler can be killed or rolled under load without routing a request through
  two epochs or acknowledging an unknown commit as success.
- The benchmark gate compares throughput, p50/p95/p99 latency, connection
  churn, CPU, and memory with PgBouncer and detects agreed regressions.

### 4. Distributed transactions

- Allocate a globally unique GID and immutable participant set before prepare.
- Store the coordinator row on the lowest-ID participating database shard and
  replicate it with that cell.
- Add PostgreSQL prepare-guard and generation-fence hooks so delayed poolers
  cannot prepare after recovery fences an owner.
- Make `COMMIT` versus `ABORT` a single-winner CAS. Never infer a decision;
  prepared participants remain blocked if the coordinator is unavailable.
- Reconcile every durable decision to every participant and expose stuck
  prepared transactions and recovery backlog.
- Reject `REPEATABLE READ`, `SERIALIZABLE`, temporary/session-bound behavior,
  and other operations PostgreSQL cannot safely prepare before a transaction
  enlists a second shard.

Acceptance:

- Failure at every prepare/decision/apply boundary produces one final outcome.
- The documented `READ COMMITTED` visibility skew is distinguished from a
  partial final commit.

### 5. Online DDL and management propagation

- Parse a deliberately supported DDL subset and represent it as a durable,
  per-database migration plan.
- Apply nonblocking changes directly only when PostgreSQL proves they are safe.
  Rewrite blocking table changes into shadow-table/backfill/catch-up/swap work.
- Stage schema on all shards, validate identical object identity and schema
  epoch, then publish one catalog activation generation.
- Fence old schema generations in PostgreSQL, wait for destination and pooler
  acknowledgements, and release buffered requests only when all shards are on
  the same externally visible generation.
- Propagate users, roles, memberships, grants, revokes, ownership, default
  privileges, and password rotation through the same database-scoped journal.
  Use collision-free physical role names on shared cells and never store secret
  material in `shardschema`.

Acceptance:

- A blocking DDL is converted to an online operation or rejected before it can
  block production traffic.
- Failure before activation leaves the prior schema serving; no request can
  observe a mixed active schema generation after activation.

### 6. Change streams and standby offload

- Embed supervised stream workers in pooler Pods while keeping query and stream
  readiness, ports, budgets, and EndpointSlices distinct.
- Create synchronized primary failover anchors and independent standby-local
  decoding slots with permanent catalog generations and creation receipts.
- Validate receiver, sender, physical slot, replay LSN, timeline, WAL retention,
  continuous slot-sync worker, `hot_standby_feedback`, and exact source
  incarnation before decoding.
- Implement snapshots, ordered `pgoutput` transaction delivery, relation cache,
  durable checkpoint/feedback, two-phase records, resume tokens, resnapshot
  fences, failover, and bounded spool/backpressure.
- Allocate separate consumers and slots for public streams, restore/move, and
  reshard materialization.

Acceptance:

- Normal decoding and materialization load comes from an eligible standby.
- Promotion or slot loss resumes from a proven compatible checkpoint or forces
  a new snapshot; it never joins different histories by LSN alone.

### 7. Backup, restore, and database mobility

- Configure one pgBackRest stanza per physical cell with S3/MinIO, encryption,
  WAL archiving, standby-preferred backup, bounded primary fallback, checksums,
  and operator-owned expiry.
- Back up every physical cell containing a selected database shard. At a
  database mutation barrier, fully drain pre-barrier transactions and 2PC
  recovery, capture each shard's LSN/timeline, and capture a signed
  database-scoped catalog projection from `cell-0000` even when no selected data
  shard is there.
- Publish one immutable manifest only after every base backup, dependency
  closure, WAL interval, catalog projection, checksum, and cross-stanza
  retention pin is durable.
- Disable automatic pgBackRest expiry. Implement crash-safe two-phase logical
  backup deletion and reference-counted dependency/WAL pins; uncertainty leaks
  storage and alerts rather than deleting a possible restore point.
- Preflight restore before target mutation. Compare the complete logical
  topology fingerprint. `RestoreTopologyMismatch` is permanent and may update
  only restore status and Events.
  - Implemented foundation: `PgShardRestore` CRD validation rejects explicit
    PostgreSQL/hash/shard-count differences, its fail-closed validating webhook
    rejects ordered ordinal/range differences, and its status-only controller
    verifies an immutable Ed25519 key Secret plus the versioned signed manifest
    before repeating the comparison. It does not treat an omitted caller
    topology as absence; the request stays `Pending/Ready=False` until an
    authoritative destination catalog resolver is implemented. Repository
    ingestion and all materialization steps below remain pending.
- Restore physical cell backups into private quarantined staging, validate the
  signed catalog projection, and logically import only the selected database
  into fresh dedicated cells using PostgreSQL 18 `pg_dump`/`pg_restore` TOC
  validation plus bounded parallel copy and durable chunk receipts.
- Recreate database and role settings only from a versioned inert-GUC allowlist.
  Reject unknown, preload, callback, role, replication, path, access-method, and
  tablespace settings before destination mutation; never replay raw setting DDL.
- Allocate a configurable destination name plus fresh database, database-shard,
  and restore UUIDs.
  Keep colocated unselected databases unregistered and unqueryable, then destroy
  staging after a durable cleanup receipt.
- Move restored `B` back to `A` online with snapshot plus `pgoutput` catch-up,
  PostgreSQL generation fences, one catalog CAS, bounded query buffering, and
  explicit replacement semantics. Allow a separate online reshard during the
  move; never fold it into restore.

Acceptance:

- Standby backups and exact restores pass against MinIO, including interruption,
  corruption, missing WAL, retry, source failover, and shared-cell source bytes.
- A five-shard backup restores as five shards or returns
  `RestoreTopologyMismatch`; five-to-three is never accepted by restore.
- The no-mutation oracle compares Kubernetes objects, catalog epochs, PV/PVC
  data identity, pgBackRest metadata, and MinIO versions.
- Continuous load remains on one database generation while an exact restored
  copy is moved and optionally resharded online.

### 8. Operator automation and observability

- Default and validate every supported setting; reject unsafe partial
  configurations before durable resource creation.
- Autotune PostgreSQL, etcd, orchestrator, pooler pools, stream workers,
  buffering, HPA targets, PDBs, probes, and timeouts from resource requests,
  limits, topology, and workload policy. Expose overrides only for a documented
  safe allowlist.
- Add an admin web UI for cells, databases, shard/range maps, placements,
  routes, pooler/cache health, replication, backups/restores, DDL, moves,
  resharding, 2PC recovery, and alerts. It uses authenticated read APIs and
  redacts credentials, SQL literals, and customer data.
- Ship Prometheus scrape resources, dashboards, alerts, OpenTelemetry export,
  Grafana dashboards, and Tempo traces.

Acceptance:

- A minimal cluster needs no hand-written PostgreSQL or component config.
- Metrics and traces correlate one routed request and one control operation
  across pooler, operator, orchestrator, and PostgreSQL without leaking secrets.

### 9. Verification and delivery

- Unit/property tests cover parsers, routing, range coverage, state machines,
  tuning, receipts, fencing, retry classification, and canonical envelopes.
- Integration tests use real PostgreSQL 18 for replication, slot sync,
  `pgoutput`, DDL, 2PC, backup barriers, restore import, crash recovery, and
  role/grant propagation.
- One GitHub Actions workflow fans out independent Rust, Go, protobuf, docs,
  security, integration, container, KIND, performance, and aggregate jobs. It
  builds Linux containers only and never pushes them.
- KIND and local `docker-desktop` tests install MinIO, Prometheus,
  OpenTelemetry Collector, Grafana, and Tempo; exercise create/load, scale,
  rolling restart, failover, backup/restore, exact mismatch, online move,
  online reshard, DDL, HPA/fixed poolers, and cleanup.
- Load tests record a continuous operation history and explicit retry outcomes
  through shard-count increase and rolling restart. Tests must prove the actual
  generation/fence invariants rather than infer zero downtime from Pod
  readiness.
- Run security scanning and CodeQL for the languages present, including Go once
  operator code is in scope. Dependabot opens updates; only patch-level updates
  on the explicit safe allowlist may auto-merge after all checks succeed.
- Every change updates relevant docs and implementation status. GitHub Pages
  presents task-oriented documentation in the CockroachDB/YugabyteDB style.
- Work occurs on a branch, through a pull request, and is squash-merged only
  after required checks succeed. Commits use the configured GitHub noreply
  identity and contain no credentials, private paths, internal logs, or customer
  information.
- Successful `main` commits receive SemVer source-only releases. Do not publish
  container images in Milestone 1.

## End-to-end release gate

Milestone 1 is complete only when a clean cluster can be installed from the
documented manifests and the following sequence succeeds under recorded load:

1. Create a fleet with HA PostgreSQL cells and fixed or HPA poolers.
2. Create five-shard `A` and three-shard `B`; colocate `B` on `A`'s first three
   cells, then move `B` to dedicated cells.
3. Route supported SQL, online DDL, and role/grant changes independently for
   both databases.
4. Execute and recover cross-shard `READ COMMITTED` transactions through
   injected process, network, primary, and coordinator failures.
5. Stream `pgoutput` and run a five-to-eight-shard online reshard from eligible
   standbys while traffic continues.
6. Roll PostgreSQL, poolers, and the operator while traffic continues and verify
   request histories against the documented retry contract.
7. Back up `A` from standbys to MinIO, restore it exactly as dedicated
   five-shard `B-restore`, and prove five-to-three restore is rejected without
   non-status mutation.
8. Move `B-restore` online to `A-restored`, including a separate five-to-three
   reshard, while old and new generation fences are fault-injected.
9. Verify Prometheus metrics, Grafana dashboards, OpenTelemetry traces in
   Tempo, admin UI state, backup retention pins, and cleanup.
10. Pass the complete parallel GitHub Actions workflow and performance
    regression policy with no image publication.
