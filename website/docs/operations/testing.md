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
large AST stack reserve. A separate raw-wire PostgreSQL 18 test validates real
`Describe`, `ParameterDescription`, empty completion, `ReadyForQuery`, and
`Close` messages through the production framing and body decoders. A targeted
KIND test verifies operator PVC deletion and same-name recreation against real
Kubernetes 1.36 controllers. A unit regression gives the informer cache a
false absence while the authoritative API reader still sees an owned PVC, and
proves that
finalization continues waiting. The broader runtime, integration, KIND,
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
