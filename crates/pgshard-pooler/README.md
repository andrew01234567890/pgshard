# pgshard pooler

This crate contains the first fail-closed Linux runtime for the future Rust
pooler. It composes catalog state with four low-frequency HTTP endpoints:

- `/healthz` reports process liveness independently of routing readiness;
- `/readyz` reports overall application readiness. It becomes HTTP 200 only
  when the catalog is usable and an explicit compatibility backend is
  configured; it does not probe a new backend socket. Catalog-only supervision reports `data_plane_unavailable`;
  explicit bootstrap mode reports `catalog_not_configured`;
- `/status` publishes build, overall readiness, and independent catalog state,
  with 64-bit counters and epochs encoded as decimal strings; and
- `/metrics` publishes Prometheus text exposition with only bounded labels.

Local catalog mode opens only a regular DSN file using nonblocking Linux flags,
reads at most 16 KiB plus one byte, and is restricted to loopback or Unix-socket
development endpoints. Operator mode constructs its PostgreSQL configuration
without a DSN: it fixes the database, login, port, application name,
`target_session_attrs=read-write`, and required SCRAM channel binding, then
verifies the exact Service DNS name over TLS 1.3 against one projected private
CA certificate. Its projected password must be exactly 64 lowercase hexadecimal
bytes. Both files are bounded, nonblocking regular-file reads; Kubernetes
Secret symlink projections are resolved and the opened target is checked again.

The explicit `bootstrap-unavailable` mode accepts no DSN, host, password, or CA
and performs no connection attempt. It exists for unsupported topologies that
cannot yet provision a catalog endpoint. Every mode applies bounded polling,
staleness, reconnect, connection, and operation deadlines. All modes shut the
catalog task, HTTP server, and PostgreSQL compatibility listener down together
on `SIGINT` or `SIGTERM`. The
control HTTP server
limits accepted connections, header count and bytes, header time, total
connection lifetime, and shutdown drain time. Transient accept errors retry,
with capped exponential backoff for resource and system failures, but Linux
pending-connection network errors do not consume the listener-outage budget and
a quiet pending accept resets its streak. An unusable listener fails
immediately and consecutive rapid failures have a 30-second ceiling. Because
the runtime supervises both listeners together, a
terminal PostgreSQL-listener failure also stops the HTTP task instead of
leaving a false-positive health endpoint. Simultaneous component failures are
retained in deterministic order. A hard runtime deadline aborts a child task
that still does not stop.

`PGSHARD_RW_BIND` selects a bounded PostgreSQL read-write listener. It allows
at most 1,024 startup handshakes, caps each packet at PostgreSQL 18's 10,004-byte
limit, applies a five-second startup deadline, and drains for at most two
seconds. It refuses GSS and SSL negotiation with PostgreSQL's single-byte `N`,
closes malformed requests without reflecting their contents, and returns a
minimal `FATAL`/`57P03` response when no ready compatibility target exists.
With `PGSHARD_RW_BACKEND_HOST` configured and the catalog ready, it relays one
raw client connection to that target, preserving PostgreSQL authentication and
subsequent session bytes end to end. Cancellation requests are forwarded to the
same singleton target. Direct `shardschema` and replication startups are
rejected before backend contact. The relay is not a connection pool or router;
it closes established sessions shortly after catalog readiness is observed
lost but does not epoch-fence individual queries.

The temporary local-mode `NoTls` connector accepts only loopback IP literals or
Unix sockets, requires the database name `shardschema` and
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
  --read-write-backend-host 127.0.0.1 \
  --read-write-backend-port 5432 \
  --shardschema-dsn-file /run/secrets/shardschema-dsn
```

This is not a deployable SQL pooler. Client-facing TLS, pooler-owned
authentication policy, backend pooling, SQL parsing and routing, OpenTelemetry
export, and the read-only listener roles remain unimplemented. For the
supported single-member development topology, the operator provisions an
immutable catalog-only credential and CA projection, selects `operator-tls`,
and points the compatibility relay at the ready-only shard-zero Service.
Catalog readiness can then make overall readiness true. The catalog login is
read-only and can connect only to `shardschema` over TLS; application
credentials pass unchanged to PostgreSQL for its native SCRAM exchange.
Application relay traffic is not made secure by this catalog-only path.
Kubernetes Lease coordination belongs to the orchestrator and is not exposed
through the pooler.

The operator does not yet rotate this static PostgreSQL serving certificate or
reload a replacement without interruption. It issues a five-year development
certificate and degrades the cluster before resource planning when less than
180 days remain; existing workloads continue, but the operator does not apply
any part of the desired resource plan. This is an explicit MVP limitation, not
a production certificate lifecycle.
Missing, replaced, malformed, or near-expiry catalog access material requires
an explicit recovery procedure and is never regenerated silently.

Run its focused checks from the repository root:

```console
cargo test -p pgshard-pooler --all-targets --all-features
cargo clippy -p pgshard-pooler --all-targets --all-features -- -D warnings
```
