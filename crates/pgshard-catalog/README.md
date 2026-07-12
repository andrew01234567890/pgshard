# pgshard catalog

This non-publishable crate contains the PostgreSQL 18 `shardschema` migration,
validated immutable Rust snapshot model, and lock-free multi-epoch cache.
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
periodic polling remains required. The actual pooler connection, snapshot SQL
loader, and LISTEN/reconnect loop are not implemented in this slice.
