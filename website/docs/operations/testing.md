---
title: Testing strategy
description: Unit, integration, KIND, Jepsen/Elle, observability, backup, and performance tests.
---

# Testing strategy

:::info Current boundary
Foundation unit, contract, policy and documentation checks exist. A live
PostgreSQL 18 smoke corpus checks positive DML examples and records known syntax
the candidate parser accepts but PostgreSQL rejects. Parser regressions exercise
deep delimiter, data-type, binary-expression, and set-operation shapes on a
64 KiB thread stack, including destruction of both admitted and rejected trees.
Optimized CI repeats the small-stack and parser-log redaction regressions, while
trivia-padding tests and benchmarks verify that shallow queries do not acquire a
large AST stack reserve. The live `shardschema` fixture drives the production
LISTEN and periodic-refresh loop, observes a committed catalog change appear in
the lock-free cache before its deliberately long polling deadline, ignores a
malformed hint, and separately recovers a trigger-bypassed epoch through
polling. It also interrupts a lock-blocked initial load, shuts down cleanly, and
proves the unsupervised driver treats forced connection loss as terminal. A
second blocked initial load and a later refresh each reach their operation
deadline and prove exact phase classification, unchanged cache state, and
backend exit. Unit coverage separately holds the cache publication lock through
expiry and proves no late snapshot is installed. The supervisor scenario
then kills its live backend, deliberately blocks the next connection attempt,
observes repeated bounded connection timeouts, verifies readiness survives only
inside the configured stale-cache grace, observes readiness expire at the exact
age boundary, releases reconnection, proves a fresh authoritative load restores
readiness, interrupts another blocked reconnect during shutdown, and externally
aborts a connected supervisor to prove readiness fails immediately and its
backend exits. Pooler unit tests exercise the real HTTP router, keep health
independent from fail-closed catalog readiness, preserve maximum 64-bit status
values as decimal JSON strings, and validate bounded phase, readiness, and
failure labels in Prometheus exposition. Runtime tests compose the production
supervisor with a pre-bound HTTP listener, observe a failed connection enter
retry without becoming ready, and prove coordinated shutdown marks catalog
state stopped. They also prove a catalog-ready control process stays
application-unready. An HTTP regression holds a partial request under a
one-connection test policy and proves shutdown force-closes it after a bounded
drain; the production policy also bounds headers and connection lifetime.
Injected acceptor tests prove an accept error can recover and shutdown can
interrupt the capped retry backoff. A Linux subprocess test loads real
environment variables, a CLI override, and file configuration, serves health,
receives `SIGTERM`, and exits successfully.
Configuration tests open only regular DSN files nonblockingly, reject a FIFO
without waiting for a writer, bound the read and timing values, reject remote
plaintext or session-policy overrides, and prove invalid DSN contents do not
escape through errors. A separate raw-wire PostgreSQL 18
test validates four-byte protocol 3.0 and 32-byte protocol 3.2 server cancellation keys,
zero-copy `BackendKeyData` and `ParameterStatus`, typed `AuthenticationOk`, and
a real protocol 3.99 to 3.2 negotiation that returns the requested unsupported
`_pq_.` option. It also reconstructs each live startup-phase `AuthenticationOk`,
`ParameterStatus`, `BackendKeyData`, `NegotiateProtocolVersion`, and
`ReadyForQuery` frame through the production encoders and requires exact byte
equality. The live connections pass through the same linear startup proof
used to require the exact negotiated version, ordered option sequence, and
protocol-specific key length. Unit tests prove that missing, duplicate,
unexpected, version-mismatched, reordered, omitted, and added negotiation data
fails closed, and that a rejected response consumes its validator. The live
fixture also checks real `Describe`, `ParameterDescription`, empty completion,
`ReadyForQuery`, and `Close` messages through the production framing and body
decoders. Source-aligned `pgoutput` unit tests cover protocol v1-v4 option
combinations, XLogData and keepalive envelopes, buffered, streamed, and
two-phase transaction controls, every truncated prefix, feature mismatches,
reserved flags, strict booleans, authoritative client/server UTF-8, maximum
prepared-transaction GIDs, persistent slot two-phase state across a later false
request, custom logical Message flags, prefix encoding, binary lengths,
explicit `messages` option gating, streamed top-level-XID matching,
zero-copy borrowing, debug redaction, and exact fixed-size Standby Status Update
frames with ordered progress validation. All-or-nothing progress-state tests
reject write, flush, and apply regression without mutating the last accepted
sample.
The live PostgreSQL 18 fixture sends the production feedback frame through a
real COPY-BOTH connection, drains a catch-up keepalive, observes three distinct
positions in `pg_stat_replication`, receives the subsequent requested keepalive,
and completes COPY cleanly. It also creates a two-phase logical slot and proves
that protocol v1 with a
later `two_phase=false` request still emits and decodes Begin Prepare and
Prepare plus exact Relation, Insert, Update, Delete, and Truncate metadata from
the live `bRIUDIRTP` sequence. A second live slot decodes nontransactional and
streamed custom Message records with opaque binary contents. It proves that a
Message emitted inside a savepoint retains the top-level XID while the Relation
record carries the savepoint XID. Schema, row, and custom-message unit tests
cover buffered versus streamed layouts, distinct top-level and subtransaction
XIDs, nested/unmatched stream controls, every truncated prefix, tuple markers
and lengths, replica identity, reserved flags, UTF-8, zero-copy iteration, and
redaction. Complete transaction ordering, relation caching, feedback scheduling
and durable-checkpoint restart integration, replay, and cross-shard stream tests
are still absent. A targeted KIND test verifies operator PVC deletion and
same-name recreation against real Kubernetes 1.36 controllers. A unit
regression gives the informer cache a false absence while the authoritative API
reader still sees an owned PVC, and proves that finalization continues waiting.
The broader runtime, integration, KIND,
Jepsen/Elle and PgBouncer comparison suites below remain required Milestone 1
work; see
[implementation status](../project/status.md).
:::

One GitHub Actions workflow fans out into independent jobs and ends in one required aggregate result. Pull requests run focused checks; scheduled invocations expand Kubernetes versions, fault duration, fuzzing, and performance samples.

## Test layers

| Layer | Focus |
|---|---|
| Unit and property | Parser, hash/range coverage, routing, epochs, buffers, tuning, state machines |
| Fuzz | PostgreSQL frames, SQL, bind messages, `pgoutput`, tuples, catalog snapshots, resume tokens |
| Integration | PostgreSQL 18 replication, 2PC crash points, failover slots, pgBackRest, missed notifications |
| KIND end-to-end | Bootstrap, services, scaling, failover, backup/restore, DDL, resharding, observability |
| Jepsen and Elle | Histories under process failure, partitions, primary changes, and resharding |
| Performance | Pooler versus PgBouncer and change-stream overhead |

Jepsen/Elle check the guarantees actually offered: atomic final cross-shard outcomes and `READ COMMITTED`. A history is not evaluated as serializable when the product does not claim serializability. Expected phase-two read skew remains documented and tested separately from atomicity violations.

## Required end-to-end environments

- MinIO for pgBackRest S3 backup and restore, including corruption and interruption cases.
- Prometheus, OpenTelemetry Collector, Grafana, and Tempo for metric/trace assertions.
- HPA and fixed pooler deployments.
- PostgreSQL and orchestrator failures at every durable 2PC boundary.
- Delayed `PREPARE TRANSACTION` commands before, during and after each
  participant fence acknowledgement and the coordinator abort CAS.
- Operation replay across executor crashes for same ID/same canonical envelope,
  plus stable conflict for same ID with each individual field changed and a
  delayed replay after terminal-state retention boundaries.
- Active DDL, reshard, and CDC streams during failover.
- CDC reconnect/replay before every chunk and terminal event, including a
  transaction exactly at and one byte/event above configured flow limits, a
  checkpoint emitted while the data window is exactly full, and individual
  snapshot/relation/reshard events at and above their byte limit.
- CDC token tampering, future-token acknowledgement, cross-stream/configuration
  replay, duplicate and stale acknowledgements, and signing-key rotation.
- CDC snapshot initialization with failures and delayed DDL, 2PC recovery,
  reshard activation, topology publication, and semantic configuration mutation
  at every barrier boundary.
- CDC disconnect and gateway crash at every snapshot relation/chunk boundary,
  plus snapshot-holder, slot, and primary loss during row copy; resume must use
  the retained snapshot or require a clean resnapshot without a silent gap.
- CDC managed DDL activation, relation swap/drop, and holder failure during row
  copy; copy-lifetime relation locks must prevent mixed schemas or storage.
- CDC disconnect and gateway crash after `SnapshotComplete` but before its
  checkpoint acknowledgement, including durable-spool write failure and
  retention expiry; replay must come from the old spool or require resnapshot.

## Pooler performance

The pooler and PgBouncer run alternately on the same runner with identical PostgreSQL, CPU, TLS, pool size, and prepared-statement configuration. Seven or more warmed 60-second samples report throughput, p50/p95/p99, CPU, and RSS. The initial fast-path target is at least 85% of PgBouncer throughput with no more than 1.15× its p95 latency; base-branch regression gates account for confidence intervals.
