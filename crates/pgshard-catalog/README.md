# pgshard catalog

This non-publishable crate contains the PostgreSQL 18 `shardschema` migration,
transactional snapshot loader, validated immutable Rust snapshot model, and
lock-free multi-epoch cache.
`shardschema` is authoritative and is hosted on stable shard 0000 in Milestone
1. Etcd is not a topology store.

The migration expects a pre-created UTF8 database and a trusted migration
principal able to create the two NOLOGIN group roles:

```sql
CREATE DATABASE shardschema TEMPLATE template0 ENCODING 'UTF8';
```

Apply `migrations/0001_shardschema.sql` while connected to that database. It is
transactional and idempotent. Application credentials, passwords, connection
strings, and other secret material do not belong in the catalog.

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
300 seconds. Connection loss is terminal rather than a silent polling-only mode.
`CatalogSupervisor` creates a fresh session after failure with bounded,
jittered exponential backoff. Its cloneable status handle keeps a pooler
unready until the first validated load, permits an existing snapshot only
within a configured 2-to-900-second stale grace, fails readiness exactly at the
deadline or immediately after an epoch fence, and reports connection phase,
cache age, epoch, connection attempts, sessions completing their initial
authoritative load, and credential-safe failure classes.
The default 90-second grace is strictly longer than the default 30-second poll.
The future pooler must still configure TLS and connection/query timeouts and
publish the status through its HTTP and Prometheus endpoints; that composition
is not yet implemented.

The empty installed catalog has genesis epoch zero. Loader queries fetch at
most one row beyond each published safety limit and reject rather than retain
oversized metadata: 1,024 logical databases, 4,096 ranges or 16,384 tables per
database, and 65,536 ranges or tables across one snapshot. Future streaming can
reduce the temporary cap-plus-one allocation without changing these bounds.

Each staged routing-range mutation versions its parent routing epoch. This
makes a concurrent activation using an older `REPEATABLE READ` snapshot fail
with a serialization error instead of validating stale child rows and
publishing incomplete routing coverage.
