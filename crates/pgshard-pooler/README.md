# pgshard pooler

This crate contains the first fail-closed Linux control executable for the
future Rust pooler. It composes the live `shardschema` supervisor with four
low-frequency HTTP endpoints:

- `/healthz` reports process liveness independently of routing readiness;
- `/readyz` reports overall application readiness and therefore remains HTTP
  503 with reason `data_plane_unavailable` in this control-only executable,
  even after the catalog becomes usable;
- `/status` publishes build, overall readiness, and independent catalog state,
  with 64-bit counters and epochs encoded as decimal strings; and
- `/metrics` publishes Prometheus text exposition with only bounded labels.

The executable opens only a regular DSN file using nonblocking Linux flags,
reads at most 16 KiB plus one byte, applies bounded polling, staleness,
reconnect, connection, and operation deadlines, and shuts the supervisor and
HTTP server down together on `SIGINT` or `SIGTERM`. Its control HTTP server
limits accepted connections, header count and bytes, header time, total
connection lifetime, and shutdown drain time. Transient accept errors retry,
with capped exponential backoff for resource and system failures. A hard
runtime deadline aborts a child task that still does not stop.

The temporary `NoTls` connector accepts only loopback IP literals or Unix
sockets, requires the database name `shardschema` and
`target_session_attrs=read-write`, and rejects startup options. This prevents
the runtime configuration from directly selecting a remote server without
transport security.

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
