# ADR 0004: Native one-phase commit and durable two-phase commit

- Status: Accepted design decision; implementation, crash recovery, and fault
  testing pending.

## Context

Most transactions touch one data group and should retain PostgreSQL's native
durability. Transactions spanning groups need a recoverable global decision;
best-effort fan-out cannot safely resolve a coordinator or network failure.

## Decision

Use PostgreSQL native one-phase commit for transactions confined to one data
group. Use durable two-phase commit for transactions spanning groups: the
coordinator persists prepare and commit decisions, participants use
PostgreSQL prepared transactions, and recovery follows the durable decision.
An uncertain result is not permission to roll back.

## Consequences

The common path stays native while distributed outcomes have an explicit
recovery record. The implementation must bound prepared-transaction buildup,
fence duplicate coordinators, test crash points, and document operational
repair. This ADR is a protocol requirement, not evidence that 2PC exists yet.
