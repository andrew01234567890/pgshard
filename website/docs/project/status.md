---
title: Implementation status
description: Evidence-based status of the Milestone 1 design and runtime.
---

# Implementation status

Milestone 1 is under active development. Design contracts describe the intended
end state; they are not claims that a feature is available. This page is updated
in the same pull request whenever implementation status changes.

| Area | Current evidence | Status |
|---|---|---|
| Core key ranges and routing hash | Rust types, no-allocation hash, golden vectors, microbenchmark | Implemented in source |
| Control and change-stream contracts | Buf-linted alpha protobuf definitions | Implemented in source |
| Public-repository, CI and release policy | Parallel CI, privacy audit, source-only SemVer tooling | Implemented in source |
| Documentation site | Typed Docusaurus build and link validation | Implemented in source |
| Go operator API and supporting resources | Defaulting/validation, generated CRD/RBAC/webhook, deterministic ConfigMaps/Services/workloads/HPA/PDB/NetworkPolicy, semantic leader-election RBAC tests, uncached finalizer absence proofs, supervised PVC deletion, and targeted digest-pinned Kubernetes 1.36 KIND delete/recreate coverage | Implemented in source; deliberately not a database cluster |
| Rust agent and orchestrator foundations | Linux HTTP health/readiness/status/metrics, exact integer reporting, bounded lease TTLs, atomic catalog/fence/deadline precondition checks; orchestrator persistence remains disabled | Implemented in source; deliberately not ready for control traffic |
| PostgreSQL lifecycle and HA | No bootstrap, physical replication, durable lease integration, promotion or restart controller | Planned |
| Pooling and SQL routing | Fail-closed bound-parameter routing core with exact canonical hashing; no SQL parser, statement planner, connection pool, or PostgreSQL wire endpoint | Partial |
| `shardschema` catalog and cache | PostgreSQL 18 idempotent migration, dual-CAS route activation, commit-only notification, live PG18 tests, repeatable-read snapshot loader, LISTEN-before-load primitive, validated checksummed snapshots and lock-free retained-epoch cache; long-running pooler refresh driver is absent | Partial |
| Cross-shard 2PC and recovery | Design only; no executable coordinator | Planned |
| Online DDL and role propagation | Design only | Planned |
| `pgoutput` change stream | Contract only; no decoder or durable stream runtime | Planned |
| Backup/restore and MinIO verification | Design only | Planned |
| Online resharding | Design only | Planned |
| Admin UI, Prometheus and OpenTelemetry | Design only | Planned |
| KIND, Jepsen/Elle and PgBouncer comparison | Targeted operator PVC delete/recreate KIND test; full cluster, history and performance suites remain absent | Partial |

No development database cluster can be installed from the current source. The
operator does not create PostgreSQL Pods or PVCs, the pooler has no wire
endpoint or connection pool, and supporting Services are not usable application
endpoints. No runtime correctness, availability or performance guarantee is
claimed until its implementation and required tests are merged and listed here.
