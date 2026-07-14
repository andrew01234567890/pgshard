# pgshard pooler

This crate contains the first fail-closed Linux runtime for the future Rust
pooler. It composes the live `shardschema` supervisor with four low-frequency
HTTP endpoints:

- `/healthz` reports process liveness independently of routing readiness;
- `/readyz` reports overall application readiness and therefore remains HTTP
  503 with reason `data_plane_unavailable` in this incomplete executable,
  even after the catalog becomes usable;
- `/status` publishes build, overall readiness, and independent catalog state,
  with 64-bit counters and epochs encoded as decimal strings; and
- `/metrics` publishes Prometheus text exposition with only bounded labels.

The executable opens only a regular DSN file using nonblocking Linux flags,
reads at most 16 KiB plus one byte, applies bounded polling, staleness,
reconnect, connection, and operation deadlines, and shuts the supervisor, HTTP
server, and PostgreSQL handshake listener down together on `SIGINT` or
`SIGTERM`. Its control HTTP server
limits accepted connections, header count and bytes, header time, total
connection lifetime, and shutdown drain time. Transient accept errors retry,
with capped exponential backoff for resource and system failures. A hard
runtime deadline aborts a child task that still does not stop.

`PGSHARD_RW_BIND` selects a bounded PostgreSQL read-write listener. It allows
at most 1,024 startup handshakes, caps each packet at PostgreSQL 18's 10,004-byte
limit, applies a five-second startup deadline, and drains for at most two
seconds. It refuses GSS and SSL negotiation with PostgreSQL's single-byte `N`,
closes malformed and cancellation requests without reflecting their contents,
and returns a minimal `FATAL`/`57P03` response to every regular startup. The
listener is only a tested transport boundary; it never authenticates or accepts
a session, and overall readiness remains false.

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
  --read-write-bind 127.0.0.1:6432 \
  --shardschema-dsn-file /run/secrets/shardschema-dsn
```

This is not a deployable SQL pooler. Authentication, authenticated TLS, backend
connections and pooling, SQL execution, OpenTelemetry export, accepted-session
drain, and the read-only listener roles remain unimplemented. The operator does
not yet create or mount the DSN file and cannot use the local-only catalog
transport between Pods. Its application Services therefore remain unusable.

Run its focused checks from the repository root:

```console
cargo test -p pgshard-pooler --all-targets --all-features
cargo clippy -p pgshard-pooler --all-targets --all-features -- -D warnings
```
