# Hot-path profile: unsharded router+pooler vs PgBouncer

M9.2 profiles the fast path the parity baseline (docs/perf-parity.md) showed
~10x PgBouncer's CPU per transaction: an unsharded prepared SELECT through
`pgbench -> pgshard-router -> pgshard-pooler -> postgres`. This page records
the evidence and ranks the optimizations for M9.3. Nothing is optimized here.

## Capturing profiles

Both binaries grew a `--pprof-listen` flag (`internal/pprofserve`) that
serves `/debug/pprof/` and enables mutex/block sampling while up. The parity
harness drives it:

```sh
docker build -f Dockerfile.router -t pgshard-router:dev .
test/perf/parity/run.sh profile     # select/prepared c=10 for 30 s, captures
                                    # CPU, allocs and mutex from router+pooler
go test -tags perfprofile ./test/perf/parity/   # same, as a test
```

`PARITY_PPROF=1 run.sh up` exposes the endpoints on 127.0.0.1:16060 (router)
and :16061 (pooler) for ad-hoc capture. Component microbenchmarks:

```sh
go test -run xxx -bench . -benchtime 2s ./test/perf/
```

## Where the CPU goes (select, prepared, c=10, ~10.6k tps)

Front-end CPU during the profiled run: router ~2.84 cores, pooler ~2.57
cores at 10.6k tps — ~0.27 + 0.24 = **~0.51 CPU-ms/txn**, matching the
baseline's ~10x-PgBouncer figure. The flat CPU tops are dominated by the
kernel and the scheduler, not by protocol compute:

```
router:  36.6% syscall (net write 29.6% of cum)      pooler: 39.6% syscall (net write 31.3%)
         15.6% runtime.futex                                 18.3% runtime.futex
         17.5% cum runtime.findRunnable                      20.6% cum runtime.findRunnable
          6.7% cum grpc clientStream.SendMsg                  9.4% cum grpc.recv
         12.5% cum grpc loopyWriter.run                      14.4% cum loopyWriter.run
         19.2% cum pgproto3 Backend.Flush (to client)        20.3% cum pgproto3 Frontend.Flush (to PG)
```

Root cause: **message-per-hop fan-out**. One pgbench transaction
(Bind/Describe/Execute/Sync) becomes 4 gRPC `SendMsg` calls router->pooler
and 6+ responses back, each a separate HTTP/2 frame crossing a goroutine
boundary (executor -> loopyWriter, transport reader -> stream channel ->
executor pump). Every crossing is a futex wake; every flush is a write(2).
PgBouncer relays the same transaction with ~2 read/write pairs in one
thread. The syscall+futex+scheduler share is roughly two thirds of both
processes; protobuf marshal/unmarshal is only ~5-7% each.

The mutex profiles show **no application-level contention** (>98% runtime
locks, top user locks ~1%): the cost is wakeups, not blocking.

## Where the memory goes (allocs, 5 s window)

Router (240 MiB allocated in 5 s, ~4.5 KiB/txn):

```
 45.5MB 18.9%  router.(*Executor).migrationBatch      <- prepared struct copied
 17.5MB  7.3%  grpc mem.(*SimpleBufferPool).Get          from e.stmts escapes via
 16.5MB  6.9%  reflect.unsafe_New (proto decode)         `mig = &st` on EVERY batch
 14.0MB  5.8%  router.(*Executor).aimBatch            <- &target escapes per stmt
 11.0MB  4.6%  router.(*Poolers).Generation           <- new proto per request
  9.5MB  4.0%  router.bindReq                         <- Value wrapper per param
```

Pooler (83 MiB in 5 s, ~1.6 KiB/txn):

```
 15.0MB 18.1%  pooler.toResponse       <- new ExecuteResponse + Value per
 14.4MB 17.4%  grpc SimpleBufferPool      backend message, DataRow columns
 13.5MB 16.2%  reflect.unsafe_New         copied into fresh protos
  4.5MB  5.4%  pooler.toFrontend
```

So the per-transaction garbage is mostly **per-message proto envelopes**,
plus two avoidable escapes on the router (`migrationBatch`, `aimBatch`) and
a `Generation` proto stamped onto every request.

## Component microbenchmarks (test/perf, AMD 5950X)

```
BenchmarkPgwireDataRowEncode1Col          12.0 ns/op      0 B/op    0 allocs
BenchmarkPgwireDataRowEncode8Col          43.3 ns/op      0 B/op    0 allocs
BenchmarkPgwireBindExecuteSyncDecode     177   ns/op     40 B/op    3 allocs
BenchmarkPlannerPlanUnshardedSelect     11.2  us/op   2137 B/op   68 allocs
BenchmarkPlannerPlanShardedSelect        9.5  us/op   2354 B/op   67 allocs
BenchmarkParseCacheHit                    15.3 ns/op      0 B/op    0 allocs
BenchmarkParseUncached                  16.9  us/op   2420 B/op   49 allocs
BenchmarkSnapshotLocate                   25.4 ns/op      0 B/op    0 allocs
BenchmarkScramParseVerifier              822   ns/op    296 B/op    7 allocs
BenchmarkGRPCPoolerHop                  73    us/op   5911 B/op  167 allocs
```

Readings:

- **pgwire encode/decode is not the problem.** DataRow encode and
  Bind/Execute decode are tens/hundreds of ns with 0-3 allocs; the M9.1
  suspicion "pgwire encode/decode allocations" is refuted for the row path.
- **The gRPC hop is the problem.** One prepared transaction over the
  Execute stream (3 sends, 5 receives, in-memory transport, no PostgreSQL)
  costs ~73 us wall and ~170 allocs — the goroutine ping-pong alone
  explains most of the +0.5 ms/query the baseline saw at c=1.
- **Planning costs ~10 us + ~68 allocs per statement.** The live profile
  shows the planner is *off* the prepared-statement hot path (plans are
  cached on the prepared statement and replanned only on snapshot change),
  so this bites `-M simple` (per-query plan) and Parse-heavy clients, not
  `-M prepared`. Notably the *unsharded* plan is no cheaper than the
  sharded one: the full statement walk runs before falling back to home.
- Parse cache hit, snapshot Locate and SCRAM verifier parsing are noise.

## Ranked optimization targets for M9.3

1. **Batch the extended-protocol messages into one gRPC message per
   direction.** Carry `repeated ExecuteRequest` / `repeated ExecuteResponse`
   (or a batch envelope) so Bind..Sync is one SendMsg and
   BindComplete..ReadyForQuery is one Recv. Attacks the dominant
   syscall/futex/loopy cost on both processes at once; expected to be worth
   several x on CPU/txn and most of the c=1 latency gap.
2. **Cut per-message proto envelopes in the pooler relay.** For DataRow-
   dense traffic, forward the raw pgwire frame bytes (`bytes` field) instead
   of re-materializing `Value` protos in `toResponse`/`rowValues`; reuse
   response structs. Removes the top pooler alloc sites and the double
   decode/encode.
3. **Fast local path for the shard-local sidecar.** The router and pooler
   are deployed adjacent; offer unix-domain sockets for the gRPC hop (drop
   TCP+TLS overhead) and evaluate an in-process pooler client for the
   unsharded/single-shard case, which removes the hop entirely.
4. **Router per-request allocation fixes** (small, mechanical):
   `migrationBatch` copies every prepared statement out of `e.stmts` and
   escapes it (`mig = &st`) on every batch — check the plan kind before
   copying; `aimBatch` escapes `&target` per statement — store the value;
   `Poolers.Generation` allocates a proto per request — stamp it once per
   batch or only when the generation changed; pool the `bindReq` Value
   slices.
5. **Plan cache for the simple protocol**: an LRU from (database, sql,
   snapshot generation) to the planned result, so `-M simple` skips the
   ~10 us walk; add an early unsharded fast path when the database has no
   sharded tables (today the unsharded walk costs as much as the sharded
   one).
6. **Client-side flush discipline**: `readyForQuery` flushes are 19% cum of
   router CPU via many small writes; coalesce the whole batch response into
   one buffered write to the client socket.
7. **High fan-in backpressure (c=1000 drops)** stays open from M9.1: the
   mutex profile clears lock contention, so investigate accept/auth ramp
   and per-session goroutine counts separately.

Re-measure each with `run.sh matrix` + `run.sh profile` and the
microbenchmarks above; parity target remains PgBouncer's ~0.05 CPU-ms/txn
within small multiples.
