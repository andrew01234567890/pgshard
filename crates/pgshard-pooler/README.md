# pgshard pooler

This crate contains the first fail-closed Linux control executable for the
future Rust pooler. It composes the live `shardschema` supervisor with four
low-frequency HTTP endpoints:

- `/healthz` reports process liveness independently of routing readiness;
- `/readyz` returns HTTP 503 until a validated catalog is usable and whenever
  its bounded stale-cache grace expires;
- `/status` publishes build and catalog state, with 64-bit counters and epochs
  encoded as decimal strings; and
- `/metrics` publishes Prometheus text exposition with only bounded labels.

The executable reads the catalog DSN from a bounded file, applies bounded
polling, staleness, reconnect, connection, and operation deadlines, and shuts
the supervisor and HTTP server down together on `SIGINT` or `SIGTERM`. Its
temporary `NoTls` connector accepts only loopback IP literals or Unix sockets,
requires the database name `shardschema` and
`target_session_attrs=read-write`, and rejects startup options. This prevents a
development-only connector from silently sending credentials to a remote
server without transport security.

For local development, place a single DSN in a file outside the repository:

```text
postgresql://pgshard_pooler@127.0.0.1:5432/shardschema?sslmode=disable&target_session_attrs=read-write
```

Then run:

```console
cargo run --locked -p pgshard-pooler -- \
  --shardschema-dsn-file /run/secrets/shardschema-dsn
```

This is not a deployable SQL pooler. A PostgreSQL listener, authentication,
authenticated TLS, backend connections and pooling, SQL execution,
OpenTelemetry export, and graceful data-plane drain remain unimplemented. The
operator does not yet create or mount the DSN file and cannot use the
local-only transport between Pods. Its application Services therefore remain
unusable.

Run its focused checks from the repository root:

```console
cargo test -p pgshard-pooler --all-targets --all-features
cargo clippy -p pgshard-pooler --all-targets --all-features -- -D warnings
```
