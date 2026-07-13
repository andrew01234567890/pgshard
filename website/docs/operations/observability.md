---
title: Observability
description: Prometheus metrics, OpenTelemetry traces, Grafana dashboards, and cardinality rules.
---

# Observability

:::info Milestone 1 design contract
These signals and the Grafana/Tempo test stack are planned. No runtime telemetry
is shipped by the foundation release; see [implementation status](../project/status.md).
:::

Every Milestone 1 Rust service will expose health, readiness, Prometheus metrics,
and OTLP export. Internal gRPC will propagate W3C trace context. Query values
will be disabled in telemetry by default.

## Pooler signals

- Client and backend connections, pool utilization, queue wait, and saturation.
- Route type, scatter fanout, query latency, and errors.
- Routing/catalog epoch and stale-cache rejection.
- Buffer requests, bytes, age, resume, and rejection reason.
- Distributed transaction phase, prepared age, and recovery backlog.

## Cluster operation signals

- Primary term, replica replay lag, fencing, promotion, and restart state.
- Backup source role, per-shard completion, WAL readiness, and restore validation.
- DDL/reshard copy, catch-up, validation, barrier, and activation stages.
- CDC snapshot progress, event rate, acknowledgement age, LSN lag, retained WAL, and resnapshot requirements.

Metrics use a `pgshard_` prefix and bounded-cardinality labels. SQL text, bind values, row values, shard keys, usernames, and unbounded transaction or stream IDs are prohibited as labels.

Dependency `log` records are statically compiled out because candidate-parser
debug messages can contain full SQL and literal-bearing AST fragments. Rust
services use explicitly sanitized `tracing` fields for OpenTelemetry instead;
tests install a maximal logger and verify that parsing emits no record.

## Included stack

The Milestone 1 distribution will provide Prometheus recording rules and alerts,
Grafana dashboards, and OpenTelemetry Collector examples. The required KIND
tests will install Prometheus, Grafana, Tempo, and the Collector; execute a
traced sharded transaction; retrieve it from Tempo; query pooler metrics from
Prometheus; and verify dashboard and datasource health.
