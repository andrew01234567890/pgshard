# ADR 0006: Certified-point PITR

- Status: Accepted design decision; implementation and restore validation
  pending.

## Context

Independent shard backups can have different WAL positions and recovery
histories. The newest local position is not automatically a consistent cluster
state.

## Decision

Expose point-in-time recovery only at a certified point: a point whose required
shard/group boundaries, durable transaction decisions, and recovery metadata
have been checked together. Do not advertise an arbitrary per-shard backup or
WAL position as a cluster-consistent restore point.

## Consequences

Recovery may choose an older point or refuse a restore when certification is
missing. Backup tooling must preserve the metadata needed for certification,
and restore tests must prove both acceptance of valid points and rejection of
uncertified ones.
