# 14. Targeting PostgreSQL 18 and 19, and nothing older

Status: accepted

## Context

Supporting more majors widens the audience. Supporting fewer makes several
guarantees simpler. pgshard had to pick a floor.

## Decision

PostgreSQL 18 and 19, both tested everywhere, with 19 tracked from beta
through RC to GA. No support for 17 or below.

The floor is set by failover slots. Resharding, VStream and the online
upgrade path all depend on logical replication slots surviving a promotion,
which means `failover = true` slots synchronised to standbys. On
PostgreSQL 17 that mechanism exists but is unsafe in combination with
two-phase decoding, which pgshard needs for streams that expose prepared
transactions. Supporting 17 would mean either giving up failover-safe
streams on it or maintaining a second set of guarantees for one major.

Testing both majors everywhere, rather than one plus a compatibility
promise, is what makes the 18-to-19 upgrade path testable: the e2e, chaos
and perf matrices run on both, and a dedicated job upgrades a live cluster
from 18 to 19 under load and asserts zero failed writes, data equality,
backup continuity, stream continuity and rollback.

## Consequences

- Every PostgreSQL-backed test runs twice, which is most of the CI budget
  and the reason for per-suite kind clusters and nightly full matrices.
- Features that exist only in 19, such as `REPACK (CONCURRENTLY)`, are used
  where available and have an 18 path, rather than being a reason to
  require 19.
- 19 being beta is a tracked risk with an explicit switch to RC and GA,
  including pgBackRest's own support for it.
