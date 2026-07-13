# pgshard pooler

This crate contains the first fail-closed runtime surface for the future Rust
pooler. It translates the live `shardschema` supervisor into four
low-frequency Linux-container endpoints:

- `/healthz` reports process liveness independently of routing readiness;
- `/readyz` returns HTTP 503 until a validated catalog is usable and whenever
  its bounded stale-cache grace expires;
- `/status` publishes build and catalog state, with 64-bit counters and epochs
  encoded as decimal strings; and
- `/metrics` publishes Prometheus text exposition with only bounded labels.

The crate is a library, not a deployable pooler process. A PostgreSQL listener,
authentication, TLS, backend connections and pooling, SQL execution, catalog
connection configuration, OpenTelemetry export, and graceful data-plane drain
remain unimplemented. The operator's internal pooler Service and HTTP probes
define the intended container contract but do not make its application
Services usable.

Run its focused checks from the repository root:

```console
cargo test -p pgshard-pooler --all-targets --all-features
cargo clippy -p pgshard-pooler --all-targets --all-features -- -D warnings
```
