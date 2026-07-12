---
title: Observability
description: Prometheus metrics, OpenTelemetry traces, Grafana dashboards, and cardinality rules.
---

# Observability

Every Rust service provides health, readiness, Prometheus metrics, and OTLP export. Internal gRPC propagates W3C trace context. Query values are disabled in telemetry by default.

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

## Included stack

The distribution provides Prometheus recording rules and alerts, Grafana dashboards, and OpenTelemetry Collector examples. KIND tests install Prometheus, Grafana, Tempo, and the Collector; they execute a traced sharded transaction, retrieve it from Tempo, query pooler metrics from Prometheus, and verify dashboard and datasource health.
