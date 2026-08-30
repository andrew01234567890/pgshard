# 3. Running PostgreSQL ourselves rather than delegating to another operator

Status: accepted

## Context

pgshard needs high availability, backups and point-in-time recovery for
every shard group. CloudNativePG already does all three well, and an
operator-of-operators design — pgshard managing CNPG `Cluster` objects —
would have started the project with a mature runtime.

The question is not whether CNPG is good at running one PostgreSQL cluster.
It is whether pgshard can express, through another operator's API, the
things a *sharded* cluster needs at the moments that matter.

## Decision

Build the runtime: `pgshard-agent` as PID 1 in each PostgreSQL pod, the
operator as the single decision maker for failover, pgBackRest for backups.

The mechanics are deliberately CNPG's, because they are the right ones: a
supervisor that reaps zombies and spawns `postgres`, role-aware probes that
tolerate crash recovery on startup and hold a replica unready when it lags,
an isolation self-fence on a primary that can reach neither the API server
nor its peers, two-phase failover that relabels the old primary out of the
read-write service before it promotes the new one, and `pg_rewind` from the
archive with an automatic re-clone when rewind fails.

What could not be expressed through another operator's API is the part that
makes those mechanics safe for a sharded cluster:

- **The epoch fence.** Every promotion bumps a token in the catalog, and
  poolers refuse writes stamped with an older one. A router that has not yet
  seen a failover cannot write to a demoted primary. The fence has to be
  raised inside the promotion decision, not observed after it.
- **Replication slots.** Resharding, VStream and the upgrade path all
  create logical slots with `failover = true`, and the operator maintains
  `synchronized_standby_slots` as the set of currently healthy sync members.
  A listed slot that is missing or inactive stalls every failover-slot
  walsender, so slot membership must move with the sync set, in the same
  decision.
- **Certified barriers.** A cluster-consistent restore point pauses writes
  on every group, drains two-phase transactions, and only then records a
  restore point on each. That is a cross-cluster transaction over the
  runtime; it cannot be assembled from per-cluster APIs that do not know
  about each other.

## Consequences

- We own failover correctness, including the crash matrices. The e2e and
  chaos suites are not optional extras; they are the evidence for a
  guarantee we now make ourselves.
- We inherit CNPG's hard-won details only by reading and reimplementing
  them. Where we deviate — automatic re-clone after a failed rewind, which
  CNPG leaves manual — the deviation is a decision, not an accident.
- A user who already runs CNPG runs a second PostgreSQL runtime to use
  pgshard. That cost is real and is the price of the three capabilities
  above.
