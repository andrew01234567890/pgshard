# ADR 0002: Four-member data groups and a three-member control group

- Status: Accepted design decision; failure tolerance and implementation
  validation pending.

## Context

Data-serving state and authoritative control state have different load,
consistency, and failure domains. They need explicit membership rather than
implicit role labels.

## Decision

Use four PostgreSQL members per data group and a three-member control group for
authoritative topology, membership, fencing, and recovery state. Data-group
role changes require live health evidence and fencing. Control-group changes
require the control protocol's current quorum and membership evidence.

## Consequences

The topology has a fixed, reviewable starting shape and separates control
failures from data-serving failures. The project must specify quorum behavior,
partition handling, capacity limits, and supported failure patterns before
claiming availability. The member counts alone are not an availability SLA.
