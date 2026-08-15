# Architecture overview

This document records the reviewed target architecture. It is a public design
boundary, not a claim that all components or guarantees are implemented.

## System boundary

pgshard is intended to provide a PostgreSQL-compatible sharded service. The
database engine is PostgreSQL 18. The runtime around PostgreSQL is Go-owned:
the gateway and pooler, PostgreSQL agent, orchestration and recovery services,
administrative APIs, and deployment operator belong to the Go control and
data-plane runtime. PostgreSQL remains the authority for SQL execution,
transactions, WAL, and storage durability.

The design separates:

- client-facing routing and pooling;
- protected PostgreSQL data and replication;
- control state, membership, fencing, and recovery decisions; and
- deployment desired state and reconciliation.

The separation is an ownership boundary. It does not by itself establish
availability, failover, or recovery behavior; those properties require code,
fault tests, and operational evidence.

## Groups and routing

The target topology is a four-member PostgreSQL data group for each shard and
a three-member control group for authoritative control state. A data group has
a serving primary and eligible replicas under an explicit health and fencing
protocol. A control-group decision is authoritative only when the control
protocol has the required quorum and current membership evidence. The member
counts are design targets; supported failure patterns must be documented and
validated before they are advertised.

Routing decisions must use live health and fencing evidence. A stored role label
or stale topology record is not sufficient to send writes. A stale or
ambiguous writer is fenced before another writer is published.

## Schema and SQL boundary

Routing and placement metadata is owned by a schema database and is expressed
through ordinary PostgreSQL tables and APIs. pgshard does not introduce custom
SQL syntax or require users to learn a second query language. PostgreSQL
parsing and SQL semantics remain the compatibility boundary; routing code
interprets the approved metadata and the query evidence available to it.

## Transaction boundary

A transaction confined to one data group uses PostgreSQL's native one-phase
commit. A transaction spanning groups uses a durable two-phase protocol: the
coordinator records prepare and commit decisions durably, participants use
PostgreSQL prepared transactions, and recovery resolves an uncertain outcome
from the durable decision rather than treating a timeout as permission to
rollback.

The protocol is a design requirement. It is not a claim that a coordinator,
participant recovery, or fault-injection harness already exists.

## Change streams and recovery

VStream-style change reads and bulk copy are replica-only operations. They must
use an eligible replica and must not silently fall back to a serving primary.
This keeps serving writes and latency separate from backfill and changefeed
work; if no suitable replica exists, the operation waits or fails explicitly.

Point-in-time recovery exposes only a certified point: a recovery point whose
required shard/group boundaries and durable records have been checked together.
A local backup or WAL position is not advertised as a cluster-consistent point
merely because it exists.

## Delivery boundary

Public changes are delivered as ordered, stacked pull requests. Each stack
slice has a narrow scope, explicit parent, reviewable tests, and documentation
that matches the code's evidence. See [ADR 0007](adrs/0007-stacked-pr-delivery.md)
for the delivery decision.

## Related decisions

- [PostgreSQL 18 and Go-owned runtime](adrs/0001-postgresql-18-go-runtime.md)
- [Group membership and topology](adrs/0002-group-topology.md)
- [Schema database and SQL boundary](adrs/0003-schema-database.md)
- [Native 1PC and durable 2PC](adrs/0004-native-commit-protocol.md)
- [Replica-only VStream and copy](adrs/0005-replica-only-streams.md)
- [Certified-point PITR](adrs/0006-certified-point-pitr.md)
- [Upstream research commit ledger](research/upstream-commit-ledger.md)
