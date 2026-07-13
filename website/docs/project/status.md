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
| Go operator and Kubernetes resources | No merged runtime or installable manifests | Planned |
| PostgreSQL lifecycle and HA | No merged agent, fencing, promotion or restart controller | Planned |
| Pooling and SQL routing | No PostgreSQL wire endpoint | Planned |
| `shardschema` catalog and cache | No catalog migrations or listener | Planned |
| Cross-shard 2PC and recovery | Design only; no executable coordinator | Planned |
| Online DDL and role propagation | Design only | Planned |
| `pgoutput` change stream | Contract only; no decoder or durable stream runtime | Planned |
| Backup/restore and MinIO verification | Design only | Planned |
| Online resharding | Design only | Planned |
| Admin UI, Prometheus and OpenTelemetry | Design only | Planned |
| KIND, Jepsen/Elle and PgBouncer comparison | Test plan only | Planned |

No development cluster can be installed from the foundation source alone. No
runtime correctness, availability or performance guarantee is claimed until its
implementation and required tests are merged and listed here.
