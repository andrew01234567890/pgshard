---
title: High availability
description: Replication, leases, fencing, promotion, restarts, and buffering.
---

# High availability

:::info Milestone 1 design contract
The Rust agent/orchestrator health surfaces and fail-closed in-memory fencing
models exist. They bound authenticated lease lifetimes and atomically reject an
operation unless its catalog epoch, fencing epoch and deadline match at
execution. An opt-in local lifecycle boundary can structurally preflight
required PostgreSQL 18 files, supervise one direct postmaster child with client
TCP and replication ingress disabled, propagate unexpected exit, and perform
bounded smart/fast/immediate signal escalation and HTTP drain. Final cleanup is
deliberately fail-closed rather than time-bounded: the PGDATA fence stays held
until the direct child is reaped and its process group has no live descendant,
including when supervision is cancelled. A filesystem-backed exclusive
supervisor lock prevents two pgshard agents that share PGDATA from starting
concurrently; it does not replace the future operator lease and storage fencing
needed before activation. The boundary rejects standby and archive-recovery
signal files and also runs the immutable PostgreSQL 18 `pg_controldata` sibling
to verify its CRC-backed report. Recovery control states are rejected even if a
signal file was lost, rather than silently turning former standby storage into
a writable primary. It does not bootstrap or activate a server.
The operator renders inactive common plus per-member primary and standby
PostgreSQL 18 configuration profiles, including promotion-safe slot capacity, `ANY 1`, and
mandatory standby feedback and slot synchronization. A bounded local observer
reads PostgreSQL 18 recovery, receiver, replay, slot-sync configuration, and
logical-slot state plus the local continuous worker's process generation and
activity before and after the slot query. The worker generation must stay
unchanged, and its post-slot sample must expose PostgreSQL's completed-cycle wait
state. A primary-side sample bounds the plain unique
synchronized-slot list and joins one managed physical slot's active PID to its
exact walsender while keeping peer-supplied reply time and `catalog_xmin`
non-authorizing. The same bounded primary statement also reads the exact
catalog-selected failover anchor. A pure bounded correlator now requires the
separately sampled standby and primary paths to match the catalog's observable
source components, database, roles, mandatory feedback and slot-sync configuration, matching
standby and primary checkpoint timelines plus live receiver and
writable-primary timelines, a standby control-file replay floor covering the
durable checkpoint, one stable local slot-sync worker generation around the slot
query, its post-query completed-cycle wait, receiver slot, gated
active physical slot, retained WAL, and exact streaming walsender identity. It
compares the primary anchor with the continuously synchronized standby copy,
requiring their name, database, plugin, failover-enabled primary and
synchronized-standby roles, non-temporary state,
two-phase boundary, invalidation, retained WAL, and bounded confirmed-flush
progress to be compatible. Synchronized progress cannot lead primary progress;
the primary anchor must be inactive. A promoted writable primary may retain
PostgreSQL 18's `synced = true` synchronized-origin marker, while its
hot-standby restrictions no longer apply, so either failover-enabled primary
shape is accepted. Transient slot-sync ownership of the standby copy is
accepted. It
remains a preflight endpoint-compatibility and
change token rather than proof of network adjacency or decoder authorization.
PostgreSQL's SQL API does not expose the live replay LSN with its atomically
sampled replay timeline, so this correlator neither compares nor carries the raw
value. Instead, the coherent control-file checkpoint pair provides a
source-bound replay floor and can lag live replay. A fresh standby may inherit
the pair from its base backup; later advances follow the restartpoint flush phase,
when PostgreSQL installs the safe checkpoint pair.
A bounded Rust mutator now creates and verifies persistent two-phase
`pgoutput` anchors on writable primaries and independent decoders on eligible
standbys. Standby creation fails before dispatch unless
the caller supplies a fresh correlated primary/standby path and the local
recheck still sees its exact receiver timeline and physical slot, a positive
bounded feedback interval, `hot_standby_feedback=on`,
`sync_replication_slots=on`, and the correlated slot-sync-worker generation.
The proof expiry is also the create-preflight deadline and is rechecked at the
dispatch boundary.
Create/drop errors after dispatch are always outcome-unknown and must be
observed rather than retried. A successful create returns only a process-local
cleanup receipt with an opaque identity for that exact create attempt; source,
role, a bounded session fence and required creation settings are rechecked after
dispatch. The slot-sync probe catalog persists that identity through activation
and cleanup so an absence receipt from an earlier same-name creation cannot
close the later lifecycle. Known pre-dispatch drop failures return the receipt,
and cleanup does not depend on a live receiver, its physical slot, healthy
feedback or slot synchronization. Restore incarnation outside probe allocation
and automatic unknown-outcome recovery still require a future durable
catalog-bound reconciler.

The observation and mutation paths run in a real primary/standby CI fixture.
Secure upstream connection
material, exact live-replay, upstream and network-adjacency proof,
restore-incarnation observation,
worker-connection and recent successful-cycle correlation, feedback freshness and
catalog-horizon proof, physical-slot lifecycle attestation, role activation,
durable logical-slot ownership and server-attested generation,
operator-managed replication, durable lease integration, promotion, automated
recovery, and rolling restarts are not implemented; see
[implementation status](../project/status.md).

`shardschema` now reserves one permanent generation/name history for a
dedicated slot-sync probe per live shard restore. The probe is explicitly
separate from consumer anchors, so a future freshness challenge cannot skip
unconsumed data by advancing a consumer resume slot. The bounded clean path now
allocates the catalog identity, creates the failover probe, observes its
continuous synchronized copy, persists the exact creation-attempt receipt ID,
and retires only after matching primary absence and synchronized-copy removal.
The live fixture rejects replay of an older absence receipt after same-name
recreation and reconciles one deliberately lost catalog activation COMMIT
response by exact reload and same-input retry. No controller yet runs that path
continuously or recovers it after process loss, and the source-bound progress
challenge is still absent.
:::

The target default is one primary and two physical streaming replicas per shard,
spread across failure domains. PostgreSQL will use `synchronous_commit=on` with
`ANY 1` synchronous standby acknowledgement. An explicit asynchronous policy is
a durability downgrade and must be surfaced as such.

## Primary fencing

The primary must hold a renewable shard/term lease in the three-member etcd cluster. The local agent self-fences PostgreSQL before it can outlive an unsafe lease. Both the orchestrator authority and receiving agent reject expired or overlong leases; configured TTL bounds are enforced by the state machines, not merely logged. Poolers route writes only to the primary identity and term currently authorized by the lease.

Promotion requires a candidate whose WAL and prepared-transaction state prove that all acknowledged commits are present. If no candidate satisfies that condition, pgshard stops writes instead of risking split brain or acknowledged-data loss.

Managed logical consumers, including public change streams and reshard
materializers, prefer a healthy physical standby as their decoding source. The
primary retains a failover anchor at the last durable consumer checkpoint, and
PostgreSQL 18 automatically synchronizes that anchor to managed promotion
candidates. Promotion can leave `synced = true` visible on the now-writable
primary as a record that the slot originated as a synchronized copy, while the
hot-standby restrictions no longer apply. Because
PostgreSQL does not allow a synchronized logical slot to be
decoded on a hot standby, normal standby decoding uses a distinct standby-local
slot. Promotion and source changes are catalog-fenced and may replay events,
but they must never skip an event. See
[change streams](../concepts/change-streams.md#standby-first-slot-topology) for
the slot roles, required settings, retention bounds, and failure tests.

Capacity includes the failover transition itself. A promoted decoder can still
hold synchronized anchor copies and standby-local decoder slots while it creates
physical slots for the remaining replicas. `max_replication_slots` is derived
from that combined footprint plus bounded repair headroom, so promotion does not
depend on a configuration restart. This is capacity only: the future
orchestrator must still prove candidate eligibility and remove an unavailable
physical slot from `synchronized_standby_slots` before a clean primary shutdown,
which PostgreSQL otherwise waits to complete.

Demotion has an additional slot-ownership gate. A former primary can retain
same-named failover anchors that PostgreSQL's slot-sync worker will not replace
with synchronized copies. Before activating its standby profile, the
orchestrator fences the member and slot users, verifies durable checkpoint
handoff, then removes only obsolete catalog-owned primary slots before an
orderly role change. After an unplanned failover, the old member is never
restarted writable for cleanup; it is reinitialized from the new primary and
its slot state is verified. The member cannot decode or become a promotion
candidate until synchronization from the new primary is observed healthy; an
unknown or user-owned collision requires operator intervention instead of
automatic deletion.

## Planned maintenance

For a PostgreSQL restart, the orchestrator catches up and promotes a replica before restarting the old primary. It performs one member operation at a time and respects disruption budgets.

Pooler Deployments use multiple replicas, topology spread, readiness draining, and a pre-stop period. Existing TCP sessions can still receive a disconnect when their pooler exits; endpoint availability does not imply transparent session migration.

## Query buffering

During a short, recognized primary outage, poolers can buffer eligible new autocommit requests in a bounded per-shard FIFO. They never blindly replay a write whose execution outcome is unknown. Buffer time, requests, bytes, and per-client contribution are capped; exceeding a limit produces a clear transient error.
