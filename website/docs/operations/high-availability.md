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
The agent control listener retries resource and system accept failures with
capped backoff for at most 30 seconds of consecutive rapid failures. A quiet
pending accept resets that streak, and Linux pending-connection network errors
retry without consuming or clearing it. Cancellation by another supervisor
branch does not discard time already spent in the pending accept. An unusable
listener descriptor fails immediately.
Either terminal path enters the existing process-wide supervisor,
which stops and reaps the quarantined postmaster because lease control and
health can no longer be served. The pooler applies the same bounded retry
contract; a terminal client-listener failure tears down its health listener so
Kubernetes cannot observe a permanently false-positive liveness endpoint.
Simultaneous component failures are retained in deterministic catalog, HTTP,
then client-listener order.
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
Create errors after dispatch are outcome-unknown and must be observed rather
than retried. Drop has one narrower effect-free exception: PostgreSQL 18's exact
`object_in_use` response from non-waiting replication-slot acquisition proves
that an active slot was rejected before drop mutation began, so the unchanged
receipt remains valid for a later bounded attempt. Every other received error,
timeout, connection loss, or postflight failure after drop dispatch remains
outcome-unknown. Before target preflight, create persists a permanent pending
attempt in `shardschema`, keyed by its opaque receipt and never-reused
generation. The transaction locks catalog state before the target name and
validates the exact source, role, restore, owner and lifecycle. A busy
database-enforced target fence fails fast so no writer retains global catalog
state while queued; the Rust create path retries acquisition within its bounded
preflight window. Each acquisition records a fresh opaque fence ID bound to the
exact backend start time, backend PID, and postmaster generation in a hidden
per-target registry in the authoritative writable `shardschema` database.
PostgreSQL advisory locks are not target-registry authority, and a mutation
session may retain its bounded caller-held advisory locks. A registry writer
first locks an established target row. Only first insertion of a target takes
the fail-fast, self-conflicting table lock, keeping established unrelated
targets independent while preventing a same-name unique-index wait. This
database-local migration deliberately does not alter built-in advisory-lock
ACLs because doing so in `shardschema` alone cannot protect the postmaster-wide
lock table. Hostile SQL resource isolation remains an operator/bootstrap
responsibility across every database. A stale row is
reclaimable only after that exact backend generation is no longer live, so PID
reuse after a backend or Pod restart is not fence authority. A successful create
returns the matching process-local cleanup receipt; source, role, the bounded
session state and required creation settings are rechecked after dispatch. After
any retry, the mutator reloads the
exact durable generation, lifecycle, restore/source identity, role, target
database, catalog epoch and pending receipt before it can dispatch on a separate
mutation connection. Known pre-dispatch failures durably abandon the attempt.
If the catalog-fence backend is lost before that cleanup commits, the pending
attempt remains visible and blocks slot, shard, restore, database, consumer,
ownership and attachment lifecycle changes until reconciliation. Activation
resolves the same attempt as activated through an exact-capability
security-definer function. Catalog roles cannot read the attempt ledger, raw
probe receipt columns, or target-fence registry.
Creating an attempt versions the shared catalog fence so an older
`REPEATABLE READ` lifecycle transaction cannot miss it. Cleanup resolves the
same attempt as retired. Final probe and consumer-slot retirement borrow the
drop path's live connection-bound catalog fence through COMMIT. Consumer
finalization also presents the exact opaque creation capability and atomically
retires the attempt and slot. Both paths present the exact opaque fence ID and
verify the same canonical backend on both sides of that COMMIT before returning
success under a fresh bounded post-COMMIT fence check. Known pre-dispatch drop
failures and the exact effect-free active-slot rejection return the receipt, and
cleanup does not depend on a live receiver, its physical slot, healthy feedback
or slot synchronization.
Catalog triggers serialize allocation, activation, cleanup-start, and related
parent lifecycle writes in the same lock namespace. Final retirement instead
requires the typed path's live connection-bound absence fence. Permanent
retired slot history is omitted from parent target-lock sets.
Privileged SQL that bypasses that typed finalization or mutates physical
replication slots directly remains outside the boundary. Automatic
observation and reconciliation of unknown post-dispatch outcomes still require
a long-running controller.

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
The live fixture starts a same-name managed recreation after primary absence,
observes it retrying the busy hidden target fence, commits permanent retirement
while it remains blocked, releases the fence, and requires the waiter to reject
the now retired durable generation even though its mutation connection targets
another database. A separate fault case buffers PostgreSQL's confirmed COMMIT
response beyond the original retirement deadline while keeping the canonical
fence backend alive. It releases that response inside the fresh post-COMMIT
verification window, requires the original deadline outcome-unknown result to
survive instead of becoming `TargetFenceLost`, and reloads the exact committed
retirement. Another fault case gates target-server preflight after a
pending create is durable, terminates the canonical catalog backend, proves
catalog retirement stays fenced, then completes and cleans up the physical slot
without producing a retired tombstone with a live name. The fixture also reconciles
one deliberately lost catalog activation COMMIT response by exact reload and
same-input retry. No controller yet runs that path continuously or recovers it
after process loss, and the source-bound progress challenge is still absent.
:::

The target default is one primary and two physical streaming replicas per shard,
spread across failure domains. PostgreSQL will use `synchronous_commit=on` with
`ANY 1` synchronous standby acknowledgement. An explicit asynchronous policy is
a durability downgrade and must be surfaced as such.

## Primary fencing

The primary must hold a renewable shard/term lease in the three-member etcd cluster. The local agent self-fences PostgreSQL before it can outlive an unsafe lease. Both the orchestrator authority and receiving agent reject expired or overlong leases; configured TTL bounds are enforced by the state machines, not merely logged. The in-process orchestrator retains Unix expiry only for request validation and reporting and uses a separate monotonic deadline for live ownership, so forward or backward wall-clock steps cannot shorten or extend a term. A renewal extends the installed monotonic deadline only by its requested Unix-expiry delta and cannot move that deadline beyond the current TTL policy. Poolers route writes only to the primary identity and term currently authorized by the lease.

Promotion requires a candidate whose WAL and prepared-transaction state prove that all acknowledged commits are present. If no candidate satisfies that condition, pgshard stops writes instead of risking split brain or acknowledged-data loss.

Managed logical consumers, including public change streams and reshard
materializers, require an eligible physical standby as their normal decoding
source. Loss of every safe standby fences the consumer by default; using the
primary is a separately configured, visible emergency policy. The primary
retains a failover anchor at the last durable consumer checkpoint, and
PostgreSQL 18 automatically synchronizes that anchor to managed promotion
candidates. Promotion can leave `synced = true` visible on the now-writable
primary as a record that the slot originated as a synchronized copy, while the
hot-standby restrictions no longer apply. Because PostgreSQL does not allow a
synchronized logical slot to be decoded on a hot standby, normal standby
decoding uses a distinct standby-local slot. Promotion and source changes are
catalog-fenced and may replay events, but they must never skip an event. See
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
