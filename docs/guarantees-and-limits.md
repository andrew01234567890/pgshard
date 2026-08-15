# Guarantees and limits

These are intended properties of the reviewed design. They are not a claim of
current implementation, availability, or production readiness.

## Design guarantees

- PostgreSQL 18 is the SQL and transaction compatibility boundary.
- A single-group transaction relies on PostgreSQL native atomic commit.
- A cross-group transaction has a durable two-phase decision. Once the durable
  decision is commit, participant recovery must not reinterpret an uncertain
  result as rollback.
- Routing publishes writes only from current health and fencing evidence.
- VStream-style change reads and bulk copy run from eligible replicas only;
  there is no silent primary fallback.
- A PITR result is advertised only at a certified cluster point, not at an
  arbitrary per-shard backup position.
- The schema database uses ordinary PostgreSQL interfaces; pgshard adds no
  custom SQL syntax.

These properties become supportable guarantees only after implementation,
crash/restart testing, and operational evidence establish them.

## Deliberate limits

- Four-member data groups and three-member control groups describe the target
  topology, not a blanket promise to survive every failure pattern. Quorum,
  network-partition, and capacity limits must be stated for each release.
- Cross-group two-phase commit does not make arbitrary cross-shard DDL,
  resharding, or schema migration atomic. Those workflows need separate
  protocols and explicit evidence.
- A replica-only stream or copy operation can be delayed or unavailable when
  no eligible replica is healthy. It must not overload a primary to hide that
  limit.
- Certified-point PITR may provide a smaller recovery window than an individual
  shard's newest backup. Newer but uncertified positions are not presented as
  globally consistent.
- The design does not promise zero RPO, zero RTO, transparent recovery from
  arbitrary operator error, or automatic failover without live fencing and
  durable state.
- SQL compatibility follows the supported PostgreSQL 18 surface and the
  implemented routing rules. It does not imply that every query can be
  scattered safely or that every PostgreSQL extension is supported.

Open behavior must be documented as a limitation or an explicit error until
tests and an ADR establish a stronger contract.
