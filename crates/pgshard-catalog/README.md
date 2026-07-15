# pgshard catalog

This non-publishable crate contains the PostgreSQL 18 `shardschema` migration,
transactional snapshot loader, validated immutable Rust snapshot model, and
lock-free multi-epoch cache.
`shardschema` is authoritative and is hosted on stable shard 0000 in Milestone
1. Etcd is not a topology store.

The migration also stores permanent shard restore-incarnation history,
logical-consumer identities, per-shard ownership fences, never-reused
checkpoint and source-attachment generations, source-bound checkpoint
identities, and generation-encoded primary-anchor and standby-decoder slot
allocations. It also keeps one never-reused, generation-encoded slot-sync probe
allocation per live shard restore. A probe is a dedicated failover slot for
worker-health challenges; it is never a consumer resume point and its progress
cannot advance a consumer checkpoint. Probe and consumer-slot tombstones share
one never-reused generation namespace even though their lifecycle records have
different owners.
Primary anchors are cluster-scoped failover identities whose synchronized
copies follow PostgreSQL promotion; standby decoders are bound to one canonical
member ordinal.
Database triggers serialize mutations through the catalog epoch. A concurrent
`REPEATABLE READ` writer whose snapshot predates a committed mutation fails with
`40001` when it reaches the versioned epoch gate; a current writer then performs
the cross-table generation checks. The same triggers reject checkpoint seeding
and regression, require every progress change to advance its
ordinal, require every new generation to begin at a snapshot boundary, bind it
to one restore/system/database/timeline lineage, reject identity rebinding,
require active matching selected-source and primary-anchor slots for every
checkpoint advance, require both activation boundaries before snapshot
completion, and retain immutable retired names and generations as tombstones.
Probe allocation, activation, cleanup, and retirement are similarly ordered:
an active or possibly-created probe must enter `retiring` and be proven absent
before its permanent tombstone permits a replacement or restore retirement.
The orchestrator crate now exposes bounded `REPEATABLE READ` operations that
load and conditionally advance this exact lifecycle. A first allocation accepts
the catalog's genesis epoch, returns only after the insert advances it, and then
supplies a nonzero source identity to the PostgreSQL slot mutator. Activation
requires that mutator's unforgeable exact creation receipt; final retirement
requires its exact drop/absence receipt. Repeating an already-applied operation
is read-only, while an ambiguous commit is reconciled by reloading the permanent
generation before retry. This composes the clean same-process lifecycle but is
not yet a long-running controller or crash/external-mutation reconciler.
Creation-attempt rows, raw probe receipt columns, and the per-target fence
registry are hidden from catalog reader and administrator roles. Every fence
acquisition stores an opaque fence ID bound to the exact PostgreSQL backend PID,
backend start, and postmaster start. PID reuse, a public slot name, or a guessed
advisory-lock key therefore cannot manufacture lifecycle authority. PostgreSQL
advisory locks are not target-registry authority; mutation sessions preserve
bounded caller-held advisory locks. Registry writers first lock an existing
target row. Only first insertion of a target takes a fail-fast,
self-conflicting table lock, so unrelated established targets remain
independent while same-name first insertion cannot hide a unique-index wait.
Built-in advisory-lock ACLs remain at PostgreSQL defaults: defending a shared
postmaster against deliberately hostile or resource-exhausting SQL requires a
future all-database operator policy and is not claimed by this database-local
migration.
The restricted catalog role cannot update checkpoint progress directly. Its
checkpoint CAS requires the caller's expected ownership fence and checkpoint
ordinal, so a fence that wins the catalog lock makes an in-flight stale advance
fail before it can reinterpret durable WAL progress.
The Rust routing snapshot intentionally does not load this registry yet;
catalog records do not authorize a live replication session.

The migration expects a pre-created UTF8 database and a short-lived superuser
bootstrap principal. Superuser authority is required to create a dedicated
`pgshard_catalog_owner` NOLOGIN role plus the fixed reader and administrator
NOLOGIN group roles, reject unsafe pre-existing attributes or delegable role
memberships, and grant only the owner `pg_read_all_stats` so security-definer
fence functions can read exact backend generations. An upgrade from the
released shared-owner layout is accepted only when that legacy owner is already
a superuser, the schema and every relation, routine, type, and collation share
that owner, and its schema-local default privileges match the released
reader-only boundary. Rejecting a non-superuser legacy owner prevents arbitrary
triggers or security-definer routines from being promoted into the trusted
dedicated owner. For an eligible upgrade, the migration re-homes the legacy
owner's non-delegable reader and administrator grants under the bootstrap
principal, removes PostgreSQL 18's automatic creator memberships with
`CASCADE`, transfers standalone composite types with the rest of the catalog,
and clears all existing fixed-role ACLs and grant options before rebuilding the
exact runtime boundary. It then runs catalog DDL as the dedicated owner:

```sql
CREATE DATABASE shardschema TEMPLATE template0 ENCODING 'UTF8';
```

Apply `migrations/0001_shardschema.sql` while connected to that database. It is
transactional, idempotent, and rejects a non-superuser owner before creating
objects on a clean install. It also rejects a released catalog owned by a
non-superuser instead of elevating its executable objects. Runtime catalog and
mutation credentials are not superusers.
Application credentials, passwords, connection strings, and other secret
material do not belong in the catalog.

The checked-in live test requires a disposable PostgreSQL 18 database:

```console
PGSHARD_TEST_DATABASE_URL=postgresql://postgres:password@127.0.0.1:5432/shardschema make catalog-test
```

The cache retains the exact immutable snapshot used by an in-flight request
until an explicit monotonic fence retires that epoch. PostgreSQL notifications
contain only the committed decimal catalog epoch and are wake-up hints;
periodic polling remains required. `CatalogReader::subscribe` takes ownership
of a dedicated connection, rejects a manually opened transaction, clears
session-local state, and commits its subscription before the initial
transactionally consistent read. `run_catalog_refresh` drives that connection,
coalesces notification bursts through one latest-wins wakeup slot, ignores
invalid hints, and performs authoritative repeatable-read polling every 1 to
300 seconds. Each committed subscription/initial load and each later refresh
has a validated 100-millisecond-to-five-minute client deadline covering SQL,
validation, and cache publication. Deadline-first selection and a timed cache
write lock prevent a result from being published after expiry. A deadline
closes the dedicated session, leaves the last validated snapshot unchanged, and
is terminal to that driver instance. A slightly later PostgreSQL 18
`transaction_timeout` also interrupts server-side lock waits and rolls back if
the backend has not yet observed the dropped socket. Connection loss is likewise
terminal rather than a silent polling-only mode.
`CatalogSupervisor` creates a fresh session after failure with bounded,
jittered exponential backoff and bounds each connection attempt between 100
milliseconds and 30 seconds. Its cloneable status handle keeps a pooler
unready until the first validated load, permits an existing snapshot only
within a configured 2-to-900-second stale grace, fails readiness exactly at the
deadline or immediately after an epoch fence, and reports connection phase,
cache age, epoch, connection attempts, sessions completing their initial
authoritative load, and credential-safe connection, connection-timeout,
operation-timeout, load, and connection-pump failure classes. The default
policy uses a five-second connection deadline and 30-second operation deadline.
Its 90-second stale grace is strictly longer than the default 30-second poll.
The `pgshard-pooler` Linux control executable composes the supervisor with its
HTTP and Prometheus translation, bounded runtime configuration, a file-backed
DSN, bounded control-HTTP resources, and coordinated deadline-bounded shutdown.
Overall application readiness remains false because no SQL listener exists,
while status and metrics expose independent catalog usability. Its temporary `NoTls` connector
rejects every endpoint except loopback IP literals and Unix sockets and
requires the dedicated writer database explicitly. Authenticated TLS, remote
catalog transport, operator-provisioned credentials, and the SQL data plane
remain absent.

The empty installed catalog has genesis epoch zero. Loader queries fetch at
most one row beyond each published safety limit and reject rather than retain
oversized metadata: 1,024 logical databases, 4,096 ranges or 16,384 tables per
database, and 65,536 ranges or tables across one snapshot. Future streaming can
reduce the temporary cap-plus-one allocation without changing these bounds.

Each staged routing-range mutation versions its parent routing epoch. This
makes a concurrent activation using an older `REPEATABLE READ` snapshot fail
with a serialization error instead of validating stale child rows and
publishing incomplete routing coverage.
