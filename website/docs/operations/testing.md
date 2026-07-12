---
title: Testing strategy
description: Unit, integration, KIND, Jepsen/Elle, observability, backup, and performance tests.
---

# Testing strategy

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
- Active DDL, reshard, and CDC streams during failover.

## Pooler performance

The pooler and PgBouncer run alternately on the same runner with identical PostgreSQL, CPU, TLS, pool size, and prepared-statement configuration. Seven or more warmed 60-second samples report throughput, p50/p95/p99, CPU, and RSS. The initial fast-path target is at least 85% of PgBouncer throughput with no more than 1.15× its p95 latency; base-branch regression gates account for confidence intervals.
