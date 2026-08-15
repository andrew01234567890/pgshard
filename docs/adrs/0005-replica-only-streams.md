# ADR 0005: Replica-only VStream and copy

- Status: Accepted design decision; implementation and load validation
  pending.

## Context

Changefeed and bulk-copy work can consume I/O, CPU, and connection capacity.
Allowing it to fall back to a serving primary would turn maintenance pressure
into write-path risk.

## Decision

VStream-style change reads and bulk copy may use eligible replicas only. A
reader must not silently connect to or fall back to the serving primary. If no
replica satisfies health, freshness, and fencing requirements, the operation
waits or fails explicitly.

## Consequences

Serving traffic is protected by an enforceable placement rule. Operators must
provide enough replica capacity for maintenance and understand that streams
can be delayed or unavailable during replica recovery.
