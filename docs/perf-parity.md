# Pooling parity: pgshard vs PgBouncer

The M9 goal is connection-pooling parity: on an **unsharded** database the
router+pooler path should cost about what PgBouncer costs. This page describes
the benchmark harness, the methodology, and the current baseline.

## Harness

`test/perf/parity/run.sh` stands up, on one docker network:

- **backend** — one `postgres:18` (`max_connections=1100`,
  `shared_buffers=256MB`), database `app`, role `app` (SCRAM).
- **direct** arm — pgbench straight at the backend (overhead reference).
- **pgbouncer-session** / **pgbouncer-txn** —
  `edoburu/pgbouncer:v1.24.1-p1` (digest-pinned in `run.sh`), tuned
  `default_pool_size=200`, `max_client_conn=1200`,
  `max_prepared_statements=256`, `auth_type=scram-sha-256`.
- **pgshard** — `pgshard-router serve` → `pgshard-pooler`
  (`--max-backends=200 --max-per-role=200`) → the same backend, with a
  catalog PostgreSQL and `dev-bootstrap` registering the unsharded `app`
  database on its home shard.

pgbench runs from a fifth container on the same network; `pgbench -i` loads
the schema once, directly, so every arm serves identical tables.

### Workloads

| workload | pgbench | notes |
|---|---|---|
| select | `-S` | select-only, read path |
| tpcb | `-N` | TPC-B-like without branch/teller contention |
| storm | `-S -C`, 10 clients | new connection per transaction (churn) |
| copy | `COPY pgbench_accounts TO STDOUT` | large result streaming |

select and tpcb run at 1/10/100/1000 clients in both `-M simple` and
`-M prepared`. Per run the harness records tps, latency avg and p50/p99/p999
(from `pgbench -l` per-transaction logs), front-end container CPU%
(docker stats sampling, converted to CPU-ms per transaction) and front-end
RSS. Rows land in `results.csv`; `go run ./test/perf/parity/analyze
results.csv` prints the comparison table with deltas vs direct.

### Running

```sh
docker build -f Dockerfile.router -t pgshard-router:dev .
test/perf/parity/run.sh matrix            # short (~10 min): 3 s per point
PARITY_DURATION=30 PARITY_SCALE=100 test/perf/parity/run.sh matrix   # long
test/perf/parity/run.sh smoke             # SELECT 1 through all four arms
```

`go test -tags perfparity ./test/perf/parity/` runs the smoke as a test.
CI: the `parity` job in `perf.yml` runs the short matrix on PRs labeled
`perf` and nightly, uploads `results.csv` + table as an artifact, and does
**not** gate merges (baseline collection only).

## Baseline (2026-08-24, short run)

A short reference run (3 s/point, scale 10, single WSL2 host — treat as
*relative*) is committed at `test/perf/parity/baseline/{results.csv,table.txt}`.
Honest summary of where pgshard's router+pooler stands today:

- **CPU per txn**: ~10x PgBouncer (select prepared c=1: 1.10 vs 0.10 CPU-ms).
- **Memory**: 90–107 MiB RSS vs PgBouncer's 10–16 MiB.
- **Latency/throughput at low concurrency**: behind both PgBouncer and direct
  (select prepared c=1: 1807 tps, -79% vs direct; PgBouncer -45..-49%).
- **High concurrency (c>=100)**: pgshard *beats* single-process PgBouncer on
  throughput (select prepared c=100: 34.8k vs ~20k tps) — the multi-goroutine
  pooler scales where PgBouncer's single thread saturates.
- **Connection storm**: pgshard within a few % of direct and ~2.7x PgBouncer.
- **Gaps / rough edges**: COPY ~2.6x slower; 1000-client prepared runs still
  drop connections (ERROR rows in the table) — backpressure at high fan-in.

Reproduce or run longer with:

```
PARITY_DURATION=30 PARITY_SCALE=100 test/perf/parity/run.sh matrix
```

The `parity` CI job (`.github/workflows/perf.yml`) runs a short matrix on the
`perf` label and nightly, uploads the artifact, and does **not** gate merges.
**M9.2** profiles the biggest gaps this surfaces: pgwire encode/decode
allocations, the router->pooler gRPC hop, per-query planning, and high-fan-in
backpressure.

## Where pgshard stands

Honest summary of the short baseline:

- **Single-connection latency is the biggest gap.** At c=1 pgshard adds
  ~0.5 ms per query (0.66 ms vs 0.23 ms through PgBouncer): every query
  crosses client→router (parse, plan, route) →gRPC→pooler→PostgreSQL and
  back. PgBouncer is a byte relay; pgshard is not, yet.
- **CPU per transaction is ~10x PgBouncer's** (0.5 vs 0.05 CPU-ms/txn on
  select c=10). Prime M9.2 profiling targets: per-message protocol
  encode/decode allocations, the gRPC hop, and per-query planning of
  statements that always route to the home shard.
- **1000 clients fail outright** on pgshard (`Run was aborted`, dropped
  client connections) where PgBouncer, given connection-ramp runway,
  serves them. Session/backpressure handling at high fan-in needs work.
  (PgBouncer's prepared+1000-client tpcb rows also error in the short run:
  its single-threaded SCRAM ramp needs more than the stretched duration.)
- **COPY throughput is ~2.6x slower** (0.44M vs 1.15M rows/s): large
  result streaming pays a relay penalty per chunk.
- **Bright spots.** Connection storm is *better* than both (+8% vs direct,
  ~2.7x PgBouncer here): the router accepts cheaply and reuses pooled
  backends, and PgBouncer's SCRAM handshake is single-threaded. At c>=100
  pgshard's multi-core front-end also overtakes single-process PgBouncer
  on throughput (33k vs 20k tps select c=100) - the parity problem is
  per-transaction cost, not scalability of the front-end process.
- **Memory:** ~50-100 MiB RSS vs PgBouncer's 2-16 MiB. Go runtime floor
  plus per-session buffers; fine for a shard-local sidecar, but worth a
  look once CPU is addressed.

These gaps are the M9.2 profiling targets; the harness above is the
measuring stick for that loop.
