# ADR 0001: PostgreSQL 18 and a Go-owned runtime

- Status: Accepted design decision; implementation and validation pending.

## Context

The system needs PostgreSQL SQL, transaction, WAL, and storage semantics while
keeping routing, lifecycle, orchestration, and deployment logic in one runtime
language and ownership model.

## Decision

Use PostgreSQL 18 as the database engine. Make the surrounding runtime
Go-owned, including the gateway and pooler, PostgreSQL agent, orchestrator and
recovery services, administrative APIs, and deployment operator. PostgreSQL
remains the authority for SQL execution, transaction durability, WAL, and
storage; Go coordinates the distributed behavior around those boundaries.

## Consequences

The project inherits a well-defined PostgreSQL compatibility boundary and can
share Go control-plane types and tests. It must track PostgreSQL 18 behavior,
make cross-process contracts explicit, and prove that orchestration never
silently weakens PostgreSQL durability.

This decision does not claim a PostgreSQL fork, a completed Go service, or
compatibility with every PostgreSQL extension.
