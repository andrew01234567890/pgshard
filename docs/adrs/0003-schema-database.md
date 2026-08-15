# ADR 0003: Schema database and no custom SQL syntax

- Status: Accepted design decision; implementation and compatibility testing
  pending.

## Context

Sharding needs durable routing and placement metadata, but adding parser
syntax would create a second compatibility surface and make clients depend on
pgshard-specific SQL.

## Decision

Store routing and placement metadata in a schema database and expose it
through ordinary PostgreSQL tables and APIs. Do not add custom SQL commands or
a pgshard-specific query language. The PostgreSQL 18 parser and SQL surface
remain the compatibility boundary.

## Consequences

Existing PostgreSQL tooling can inspect and manage metadata. Metadata schema,
permissions, versioning, and planner behavior must be designed as normal
database contracts. Unsupported or ambiguous routing must produce an explicit
result rather than being hidden by new syntax.
