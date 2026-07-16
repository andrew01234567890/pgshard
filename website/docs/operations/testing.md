---
title: Testing strategy
description: Unit, integration, KIND, Jepsen/Elle, observability, backup, and performance tests.
---

# Testing strategy

:::info Current boundary
Foundation unit, contract, policy and documentation checks exist. A live
PostgreSQL 18 smoke corpus checks positive DML examples and records known syntax
the candidate parser accepts but PostgreSQL rejects. Parser regressions exercise
deep delimiter, data-type, binary-expression, and set-operation shapes on a
64 KiB thread stack, including destruction of both admitted and rejected trees.
Optimized CI repeats the small-stack and parser-log redaction regressions, while
trivia-padding tests and benchmarks verify that shallow queries do not acquire a
large AST stack reserve. The live `shardschema` fixture drives the production
LISTEN and periodic-refresh loop, observes a committed catalog change appear in
the lock-free cache before its deliberately long polling deadline, ignores a
malformed hint, and separately recovers a trigger-bypassed epoch through
polling. It also interrupts a lock-blocked initial load, shuts down cleanly, and
proves the unsupervised driver treats forced connection loss as terminal. A
second blocked initial load and a later refresh each reach their operation
deadline and prove exact phase classification, unchanged cache state, and
backend exit. The same PostgreSQL 18 fixture exercises the logical-consumer
registry's core trigger and ownership-fenced checkpoint CAS paths through its
migration principal. A negative bootstrap test first proves that a non-superuser
principal is rejected before object creation. Upgrade coverage embeds the exact
v0.49 migration bytes and installs them through `SET ROLE`, first proving that a
non-superuser `CREATEROLE` owner and its real PostgreSQL 18 grantor chain are
rejected rather than elevated. An exact fixture installed as PostgreSQL's
bootstrap superuser proves its non-delegable runtime memberships and their
`INHERIT` and `SET` options survive takeover without a same-grantor revoke. A
second eligible superuser-owned fixture moves the released catalog to the
dedicated NOLOGIN owner, re-homes those memberships under the bootstrap
grantor, removes explicit fixed-role memberships held by the old owner,
preserves the epoch, and proves the deprivileged old owner has no residual
catalog access and can be dropped. Hostile reader/admin schema, table, column,
function, procedure, type, and grant-option ACLs, including PUBLIC procedure
execution, are cleared before the documented boundary is rebuilt, and a
standalone composite type proves complete ownership transfer. Rollback-only
cases cover unsafe fixed-role attributes, delegable memberships, unexpected
reader/admin/owner inheritance, owner members, fixed-role schema ownership,
missing released roles, mixed object ownership, unsupported schema object
classes, external executable or referential triggers, a same-identity released
trigger with altered predicate and arguments, and unexpected default privileges
without advancing the catalog epoch or retaining fixture roles. A concurrent
case holds an uncommitted external trigger on a target relation and requires the
migration's `NOWAIT` lock pass to fail promptly with `55P03`. After that DDL
commits, a complete migration retry must reject the trigger. The migration
client starts with a `REPEATABLE READ` session default, proving the migration
explicitly selects `READ COMMITTED` before its first snapshot; the migration's
requirements pass checks the live transaction setting, so removing the override
makes this regression fail before its expected lock result. Every migration
attempt, cancellation, and rollback is bounded, and teardown awaits both
connection drivers after aborting them. A successful lock pass retains every
trigger/FK-capable relation lock through ownership transfer, ACL reset, and
trigger recreation. A
separate rollback-only smoke path creates the registry allocation set through
the restricted catalog-admin role. It also verifies that privileged functions
place the temporary schema last in their fixed search paths and that replaying
the migration cannot resurrect a retired restore incarnation or advance the
catalog epoch. The live test owns a disposable catalog-schema lifecycle so a
second invocation starts clean and removes its objects afterward. Before the
clean install contract, it recreates the pre-receipt probe table and v0.49
lifecycle trigger, reaches `active` through the historical `allocated → active`
transition, proves that receiptless row blocks upgrade with SQLSTATE `55000`,
then retires it through `active → retiring → retired` under the old trigger. It
finally proves allocated and retired history upgrade in place, reapplies the
migration, and validates both new constraints. The
trigger suite rejects slot names that
do not encode their complete UUID generation, activation without both the
primary anchor and selected source, an attachment that invents a restore
incarnation, snapshot completion behind either slot's consistent point or
two-phase boundary, a checkpoint source-lineage mismatch, readiness with a
snapshot-required checkpoint, checkpoint regression or progress without an
ordinal advance, progress before source activation or after source retirement,
each selected-source and anchor activation boundary in isolation,
two-phase-boundary mutation, generation-incompatible name reuse and retired
generation rebinding, restore rotation while a checkpoint or non-retired source
attachment exists, retirement before dependent slots, and a stale checkpoint
advance waiting behind a concurrently committed owner fence. It also rejects
fencing a ready owner without advancing its ownership fence and consumer
creation under draining and retired logical databases, cleans up an activated
slot after attachment
activation fails, completes the ordered fence, slot, attachment, checkpoint,
consumer, and restore-incarnation rotation path, and proves the retired records
remain
immutable. Unit coverage separately holds the cache publication lock through
expiry and proves no late snapshot is installed, bounds retained epochs,
rejects a cache-only snapshot at the hard age ceiling, refreshes that age on an
authoritative replay, checks safe connector-cause classification, and proves a
successful internally bounded publication wins a simultaneous outer deadline
timer, while a boundary error remains a timeout.
The supervisor scenario
then kills its live backend, deliberately blocks the next connection attempt,
observes repeated bounded connection timeouts, verifies readiness survives only
inside the configured stale-cache grace, observes readiness expire at the exact
age boundary, releases reconnection, proves a fresh authoritative load restores
readiness, interrupts another blocked reconnect during shutdown, and externally
aborts a connected supervisor to prove readiness fails immediately and its
backend exits. Pooler unit tests exercise the real HTTP router, keep health
independent from fail-closed catalog readiness, preserve maximum 64-bit status
values as decimal JSON strings, and validate bounded phase, readiness, and
failure labels in Prometheus exposition. Runtime tests compose the production
supervisor with pre-bound HTTP and PostgreSQL listeners, observe a failed
catalog connection enter retry without becoming ready, and prove coordinated
shutdown marks catalog state stopped. Fault injection additionally proves a
terminal PostgreSQL-listener failure closes the HTTP listener and simultaneous
component errors remain available in deterministic order. Runtime tests also
prove a catalog-ready process stays application-unready. An HTTP regression holds a partial request under a
one-connection test policy and proves shutdown force-closes it after a bounded
drain; the production policy also bounds headers and connection lifetime.
Injected acceptor tests prove an accept error can recover, cancellation of an
in-flight wait retains its deadline and exponential retry state, unusable
listener descriptors fail immediately, continuous failures exhaust a bounded
outage budget, pending Linux connection errors do not consume that budget, a
quiet accept interval resets the failure streak even when the outer supervisor
cancels and recreates the pending accept future, interleaved connection errors
cannot clear a resource-failure streak, and shutdown can interrupt the capped
retry backoff. Agent listener tests independently prove the same cancellation
and interleaving invariants, transient and spaced failures recover, a permanent
descriptor or exhausted zero budget is terminal after one attempt, and shutdown
interrupts its retry wait. A Linux subprocess test loads
real environment variables, a CLI override, and file configuration, serves health,
returns an exact PostgreSQL `FATAL`/`57P03` startup rejection, receives
`SIGTERM`, and exits successfully. A second subprocess starts with no DSN in
explicit bootstrap mode, proves liveness stays independent while readiness and
status report `catalog_not_configured`, observes no connection attempt, rejects
PostgreSQL startup, and exits cleanly on `SIGTERM`. PostgreSQL listener tests
refuse sequential GSS and SSL requests, decode the bounded rejection, close
cancellation without a response, time out an incomplete startup, and drain on
shutdown. Configuration tests require exactly one explicit catalog path: local
mode with a regular DSN file, or credential-free `bootstrap-unavailable` mode.
They open DSN files nonblockingly, reject a FIFO without waiting for a writer,
bound the read and timing values, reject remote plaintext or session-policy
overrides, and prove invalid DSN contents do not escape through errors. A
separate raw-wire PostgreSQL 18 test creates every protocol 3.0, 3.2, and
negotiated 3.99 outbound packet with the production startup encoder. It
validates four-byte protocol 3.0 and
32-byte protocol 3.2 server cancellation keys, zero-copy `BackendKeyData` and
`ParameterStatus`, typed `AuthenticationOk`, and a real protocol 3.99 to 3.2
negotiation that returns the requested unsupported `_pq_.` option. It also
reconstructs each live startup-phase `AuthenticationOk`,
`ParameterStatus`, `BackendKeyData`, `NegotiateProtocolVersion`, and
`ReadyForQuery` frame through the production encoders and requires exact byte
equality. The live connections pass through the same linear startup proof
used to require the exact negotiated version, ordered option sequence, and
protocol-specific key length. Unit tests prove that missing, duplicate,
unexpected, version-mismatched, reordered, omitted, and added negotiation data
fails closed, and that a rejected response consumes its validator. The live
fixture also checks real `Describe`, `ParameterDescription`, empty completion,
`ReadyForQuery`, and `Close` messages through the production framing and body
decoders. Unit and public-API tests round-trip fixed SSL/GSS requests, maximum
regular startup packets, ordered duplicate and empty parameters, and
protocol-proof-bound four- and 32-byte cancellation requests; reject invalid or
oversized input without mutation; and redact parameter values and keys from
errors. Separate unit tests require an encoder-issued phase proof before
enforcing PostgreSQL 18's tighter SCRAM frame bound,
decode absent, empty, truncated, negative, and trailing initial responses,
redact all mechanism and exchange bytes, and round-trip the closed SCRAM
advertisement plus continue/final encoders. Separate unit tests require minimal
client-facing `ErrorResponse` encoding to preserve PostgreSQL 18's `S`, `V`,
`C`, `M`, and terminal-field order; accept the exact 30,000-byte startup
ceiling and an explicit larger authenticated-session bound; reject excessive
caller limits, every invalid SQLSTATE position, empty or zero-containing
messages, oversizing, and short output without mutation; keep rejected payloads
out of errors. Source-aligned `pgoutput` unit tests cover protocol
v1-v4 option
combinations, XLogData and keepalive envelopes, buffered, streamed, and
two-phase transaction controls, every truncated prefix, feature mismatches,
reserved flags, strict booleans, authoritative client/server UTF-8, maximum
prepared-transaction GIDs, persistent slot two-phase state across a later false
request, custom logical Message flags, prefix encoding, binary lengths,
explicit `messages` option gating, streamed top-level-XID matching,
zero-copy borrowing, debug redaction, and exact fixed-size Standby Status Update
frames with ordered progress validation. All-or-nothing progress-state tests
reject write, flush, and apply regression without mutating the last accepted
sample.
Four dependency-free decoder surfaces also run under `cargo-fuzz`: startup,
all frontend phases and typed bodies, backend frames and typed bodies, and
stateful `pgoutput` message sequences across protocol versions one through four,
all twelve streaming/two-phase/logical-message behaviors, and both requested
and persistent-slot two-phase provenance paths. Successful messages are
traversed through their fallible borrowed iterators, and any decoded-message
invariant error fails the fuzz run. Each target has a committed minimized valid
corpus that keeps its deep typed-message and iterator paths reachable from a
clean checkout. Pgwire-affecting pull requests and main-branch pushes run
10,000 inputs per target in four parallel jobs; the scheduled workflow raises
this to 100,000. The tooling is pinned to `cargo-fuzz` 0.13.2,
`libfuzzer-sys` 0.4.13, and `nightly-2026-06-24`.
Pure orchestrator lease tests separate acquisition bookkeeping from execution
authority. They inject descheduling after clock sampling, forward and backward
wall steps, a pause combined with a backward step between paired wall samples,
mutex contention across expiry, renewal, expiry, and higher-epoch replacement.
An acquisition handle must revalidate the exact
installed term, process-local monotonic deadline, catalog epoch, and fencing
epoch at dispatch; expired and superseded handles fail closed. The receiving
target still has to enforce that epoch because the local guard is an
instant-in-time observation rather than a duration guarantee.
Pure orchestrator tests also exercise the standby-decoder attachment contract. They
require non-nil catalog generations encoded in slot names and matched exactly,
an opaque test-only replay floor bound to the exact source identity, the
current enabled two-phase mode and activation boundaries; a bounded
hot-standby-feedback interval
with a fixed scheduling margin; a slot-sync success from the current
direct-primary connection generation; exact live receiver, primary walsender,
and physical-slot-owner correlation; and a second complete invariant check
after the caller reports that a replication backend acquired the local slot.
The second check rejects a changed source or current mode, slot progress racing beyond the
durable checkpoint, a different reported active backend PID, and a reported
start other than that checkpoint before producing a non-authorizing report.
Pure values cannot prove which PID and start LSN an actual socket used. These
tests cover observation contracts only; the catalog allocation lifecycle is
tested separately. A dedicated live PostgreSQL 18 suite now exercises bounded
creation, exact postflight verification and receipt-authorized deletion of both
a primary failover anchor and a standby-local decoder. The standby case
consumes a fresh correlated primary/receiver/slot-sync-worker proof, rechecks
the live receiver timeline, requires bounded feedback and configured continuous
slot synchronization, and triggers the required standby snapshot record from
the primary. The primary case proves an inherited restricted replication role
and a bounded advisory-lock fence survive the whole mutation, while an
oversized fence is rejected before dispatch. The primary fixture waits for the
continuous worker to materialize the exact synchronized standby copy, then
requires that copy to disappear after primary deletion. It also proves a
PostgreSQL 17 pre-dispatch drop rejection returns the receipt before retrying
cleanup on PostgreSQL 18. The same fixture gates the drop after inactive-slot
preflight, starts a real logical-replication session, and proves PostgreSQL 18's
exact `object_in_use` rejection preserves that receipt until the stream exits
and a later bounded drop succeeds. Unit tests cover the exposed known-versus-unknown classification,
a blocked preflight and delayed CREATE preparation crossing proof expiry,
expired proof and operation-deadline boundaries that never poll the dispatch callback,
unsigned high-bit receiver timelines,
and deterministic role, receiver lineage, feedback interval, synchronization,
degraded receiver-free cleanup, progress and two-phase-boundary checks.
Mutation fixtures use per-session advisory fences and an aggregate bound sized
for their sequential one-minute phases. Snapshot-triggered standby creation has
no detached mutation task: cancellation aborts connection ownership, the outer
fixture is reaped, and cleanup waits for the recorded PostgreSQL backend to exit
before it treats an absent target as final.
The same primary/standby fixture now drives one permanent `shardschema`
slot-sync probe through `allocated`, `active`, `retiring`, and `retired`. It
starts from genesis epoch zero when necessary, requires the allocation commit to
produce a nonzero source identity, creates the exact primary failover slot,
waits for the continuous worker's non-temporary synchronized copy, and permits
catalog activation only from the creation receipt. Cleanup must return the exact
drop/absence receipt carrying the persisted create-attempt ID, and the
synchronized copy must disappear before permanent retirement. The fixture
retains the final drop's connection-bound target fence, starts a same-name
managed create whose mutation connection deliberately targets another database,
and observes that task retrying the busy hidden `shardschema` fence across
catalog COMMIT. Releasing the fence must let that task finish with a pre-dispatch
rejection because its exact durable generation is now retired; the test reaps
the task and proves the target remains absent. This cross-database case proves
that all mutation databases share the canonical registry for cluster-wide slot
names. A second fixture buffers the exact successful COMMIT response beyond the
original retirement deadline without closing the absence-fence backend. It
releases the response inside the fresh post-COMMIT verification window and
requires the original deadline outcome-unknown result, rather than
`TargetFenceLost`, before reloading the committed retirement. A third fixture
gates target-server preflight after the creation attempt
commits, terminates that catalog backend, and requires the permanent pending row
to fence both shard and probe retirement. It then lets creation finish, activates
the exact receipt, and removes both the primary slot and synchronized copy before
retiring the durable allocation. Catalog tests also require raw consumer-slot
and ownership lifecycle writes to fail fast while the same target lock is held,
reject ownership and attachment transitions during a pending create, reject
creation on a draining shard, and preserve the valid
active-slot/staged-attachment drop path. Repeating
allocation and every completed transition is checked as a read-only idempotent
result. An unrelated epoch advance must fence a stale activation token before
any lifecycle write, after which an exact reload can continue. A TCP fault proxy
acknowledges that its frame parser is armed on the idle authenticated
connection, closes the client response half when it receives the exact COMMIT
frame, forwards that frame, and then independently requires PostgreSQL's
`CommandComplete` and `ReadyForQuery`. The client must classify
`OutcomeUnknown`, reload the exact durable row, and safely replay the same held
receipt and boundary. The same fault is injected into typed consumer-slot
activation through a unique ephemeral catalog-admin login: the test proves the
committed state before replay, reuses the same opaque receipt idempotently, and
always drops the login. Its complete consumer hierarchy is inserted in one
transaction so setup failure cannot strand undiscoverable parent rows. Fence
tests also replace a dead owner PID with a live PID while retaining the stale
backend generation, release a real fence before acquiring a transaction
advisory lock, deny the untrusted catalog reader access to fence acquisition and
state, and row-lock the hidden registry through release. PID reuse and
transaction locks must not restore authority; release must remain bounded, its
backend must exit, and the stale row must then be reclaimable. A two-session
test retains `cluster_state` opposite same-target first insertion and requires
`55P03` within one second. A reverse-wait test then proves with
`pg_blocking_pids` that an uncommitted different-target insertion is blocked by
that exact `cluster_state` holder while an established target still locks within
one second. Every setup, timeout, and error branch attempts rollback or forced
termination, reaps both connection drivers, and observes both backend exits
before it reports accumulated cleanup failures.
Always-run
bounded cleanup retires the catalog row only
after both primary-slot removal and synchronized standby-copy disappearance
succeed; cleanup owns a separately deadline-bounded connection with PostgreSQL
statement, lock, and transaction timeouts. It then retires the attachment,
consumer shard, consumer, and logical database created by the fixture and proves
that no live hierarchy remains. A failed absence check preserves the live or
retiring row for diagnosis and reconciliation.
Automatic crash reconciliation remains a separate future test.
Injected post-dispatch slot-mutation socket loss and cancellation races are not
yet exercised. A complete post-dispatch slot-outcome ledger, automatic
reconciliation after an unknown outcome, connection-bound command proof,
quarantined COPY-BOTH
attachment, and connection-bound pooler stream ownership remain future work.
A lower-level correlation suite independently mutates every sampled path class.
It accepts an unproven non-temporary physical slot only as raw evidence and
rejects reversed or stale collection windows; database, source, role, WAL-level,
feedback, slot-sync, a missing or changed before/after worker generation, a
post-query worker outside its completed-cycle wait, replay-floor coverage,
receiver, gate, physical-slot,
retention, invalidation, PID, application-name, activity, and peer-reply
mismatches. It independently mutates both failover-anchor endpoints and rejects
an absent or misidentified row, wrong database/plugin/flags, temporary state,
disabled or mismatched two-phase decoding, an active primary anchor,
invalidation, missing or lost WAL retention, missing or unsafe progress, and a
synchronized copy ahead of its primary. It accepts transient slot-sync worker
ownership only on the synchronized copy and accepts the `synced = true` flag
that PostgreSQL 18 can retain after promoting that copy to a writable primary.
It also proves that the output carries
the coherent standby control-file checkpoint as an opaque source-bound replay floor while preserving
raw `catalog_xmin`, physical restart LSN, persistence classification, backend
generation, and peer reply only as non-authorizing tokens.
The `Orchestrator catalog / PostgreSQL 18` CI job applies the real migration,
constructs a ready reshard-materializer fixture, and loads its exact
restore-bound checkpoint, ownership fence, anchor, and member-local decoder in
one repeatable-read transaction. It also proves another member cannot inherit
the allocation, a committed ownership fence removes the policy from subsequent
reads, exact singleton values are retained, and corrupt ready rows with an
unfinished snapshot, seed ordinal, or missing attachment/slot fail closed. A
dedicated login starts with hostile function shadows ahead of `pg_catalog` in
its role-default `search_path`; the reader proves those shadows are initially
effective, then resets the session, pins and verifies an empty path, reads the
real PostgreSQL 18 requirements, and loads the real catalog policy. A
delayed singleton lock followed by a still-held owner lock proves the remaining
server timeout is recomputed against one absolute client deadline before each
PostgreSQL statement. The test
observes the reader backend idle with no transaction while the owner lock is
still held, then proves the same connection can retry after that statement
cancellation and rollback. A unit test classifies an elapsed absolute client
deadline as terminal and a completed statement cancellation as retryable; it
does not simulate PostgreSQL's transaction-timeout error or a closed socket. A
shared golden contract keeps the Rust reader and Go operator's
member physical-slot names identical. This covers
catalog-to-validator loading only; it does not
create, consume, synchronize, or drop a live replication slot.
The same PostgreSQL 18 job initializes logical WAL and prepared transactions,
then exercises the separate local slot observer against persistent failover
anchors, persistent non-failover decoders, a session-temporary slot, a missing
target, and a physical slot occupying a requested logical name. A hostile
startup `search_path` cannot replace the observer's built-ins because it pins an
empty path before the first catalog read. A separately held `pg_database` lock
proves that the operation remains bounded, aborts its owned connection driver,
and removes the backend while the blocker remains held. The fixture attempts
final cleanup of every persistent slot and hostile schema after the fixture
task completes, and reports cleanup failures alongside fixture failures. The
current live case does not deliberately inject a fixture error or panic to
exercise that fallback. The observer preserves exact request order and typed
built-in state, records the non-atomic collection interval, treats every
non-temporary public-view row's persistence as unproven, and reports ownership
as unknown; this is local catalog observation, not a multi-server eligibility
proof or creation attestation. The same consumed connection also brackets a
preceding PostgreSQL 18 prerequisite query and a following worker query. The live primary fixture verifies
its control-file system identifier and checkpoint timeline, writable role,
logical WAL level, enabled feedback and slot-sync settings, exact one-second
receiver interval, configured test physical-slot name, absent replay position,
and absent WAL receiver; settings on that writable server are not behavioral
evidence. The job takes a physical base backup through a dedicated replication
role, creates a primary failover anchor after that backup but before starting its
standby, and therefore keeps both the anchor's required WAL and catalog horizon
available for synchronization. The standby starts on a separate port and must
report recovery, replay, a streaming receiver using the managed physical slot,
mandatory feedback, and continuous slot synchronization. The test waits past
PostgreSQL's temporary synchronized-slot state for a
non-temporary synchronized copy and observes the local slot-sync worker at its
post-cycle `ReplicationSlotsyncMain` wait boundary with a nonzero PID and
backend-start identity. The slot query is bracketed by before/after samples of
that same process generation, and the complete local monotonic order is checked.
That identity is compared only for equality; it does not turn the server wall
clock into a freshness source or date the completed cycle. A primary login without
effective `pg_read_all_stats` privileges is rejected before redacted
auxiliary-worker rows can be classified as absent. The fixture grants that role
with `INHERIT FALSE`, proving that membership alone is insufficient. On the
standby, the same restricted login sees only the receiver PID, and the observer
returns its PID-specific details-unavailable error. Exact
primary-side coverage also requires the bounded plain synchronized-slot list to
contain the managed physical slot, observes its nonzero `catalog_xmin`, retained
restart LSN and active PID, and joins that PID to a streaming walsender with the
expected managed `application_name`, nonzero backend generation, and a
peer-supplied reply timestamp. That same bounded primary statement reads the
exact failover anchor. The live case requires its catalog-selected name,
database, `pgoutput` plugin, primary flags, non-temporary inactive state,
two-phase activation boundary, retained WAL, and confirmed-flush progress; an
absent requested anchor remains absent rather than producing a synthetic row.
The final correlation compares that row with the continuously synchronized
standby copy and requires compatible bounded progress. The live topology
confirms that WAL replay is paused, issues a new primary checkpoint, records the standby control-file floor,
and uses the new checkpoint record as the catalog requirement. The snapshot
taken while replay remains paused fails correlation because its floor is behind
that requirement. A bounded cleanup path always attempts to resume replay and
fails the test if it cannot. The test waits for the raw replay end pointer to
move strictly past the checkpoint record's start,
requests a bounded standby checkpoint, and then waits for the coherent
control-file floor to advance. The same catalog checkpoint passes, and the
returned opaque replay floor matches the advanced checkpoint LSN and timeline.
The raw standby replay position is used only to synchronize this test transition;
it remains unpaired and cannot
supply product evidence. The test also verifies the same database and source
components, both control-file checkpoint timelines, the live receiver, and the
primary's current WAL insertion timeline all agree, followed by the
exact physical path and raw
horizon, unproven persistence, and peer token without
claiming that the sampled endpoints were directly connected or granting
attachment authority. An absent ungated slot returns no synthetic rows.
The reply timestamp and 32-bit transaction ID are equality-only raw values;
feedback freshness and horizon coverage,
new-anchor reconciliation on already-running standbys, and a timestamped
successful source-bound slot-sync cycle, authenticated upstream adjacency,
logical-slot ownership, and server-attested generation remain part of the later
runtime suite.
The same fixture proves that a session-owned temporary physical slot is active
without inventing a walsender row, that it disappears within a bounded cleanup
window after its backend exits, and that a temporary logical slot occupying the
physical name produces a typed collision before bounded cleanup.
The same job runs the observer against PostgreSQL 17 and requires the typed
minimum-version rejection before any PostgreSQL 18-only setting is queried.
Agent unit tests reject unsafe, incompatible, symlinked, structurally incomplete,
or role-aware recovery state, including base-backup markers and CRC-backed
`shut down in recovery` or `in archive recovery` control states with both signal
files deliberately absent; require a shutdown signal budget that includes its
bounded initial reap; reject external WAL directories and user
tablespaces; enforce directory owner and mode policy; recursively reject unsafe
directories, symlinks, and special-file entries throughout PGDATA; bound
fixed-policy file reads; revalidate path identities immediately before spawn;
and abandon a genuinely
blocked preparation or revalidation worker when shutdown wins before process
creation. They hold one cross-process supervisor lock per PGDATA, reject a
symlinked external PID file without modifying its target, preserve an exact
validated regular PID file or bounded PostgreSQL-created `0600`/`0640` startup
transient through final pre-spawn revalidation, and atomically replace a stale
agent-thread lock only after durably writing its new PID line plus the preserved
shared-memory/orphan fields. Separately, the PostgreSQL 18 source ordering
establishes its data and socket locks before overwriting the external PID file.
Shutdown tests exercise smart-to-fast escalation, forced reaping, a
signal-ignoring postmaster descendant, nested PID-namespace group identifiers,
a leader that exits before its descendant, unreaped pidfd observation,
non-UTF-8 Linux process names, and cancellation of the supervision future. A
deterministic cleanup-state test injects a transient process-table inspection
failure and requires a later absence proof not to be mislabeled as a surviving
descendant. Cancellation also reaps the direct child before the PGDATA fence can
be reacquired. An isolated production-binary test creates a stopped `setsid()`
descendant outside the postmaster process group, crashes the postmaster, and
requires the Linux child subreaper to pidfd-kill and reap the descendant before
a replacement agent can acquire the same PGDATA.
Linux subprocess coverage proves the control listener binds before child
creation, reports the quarantined process state and metrics, remains unready
without a lease, bounds HTTP drain even when a client holds partial headers,
propagates postmaster crash and `SIGTERM`, and leaves no child behind. A separate
local-image CI job initializes a real PostgreSQL 18 data directory, rejects
standby, archive, incomplete base-backup, and missing-signal recovery control
state before process creation,
rejects directory mounts at both `pg_wal` and `base/1` plus a bind-mounted
regular `PG_VERSION` file, and proves a configuration-file data-directory
redirect cannot escape the validated path. The real PostgreSQL 18 run also
stops a normal child whose `NSpgid` proves it called `setsid()`, crashes the
postmaster, and requires the agent log to identify that exact adopted child
before the container exits.
It also proves a second container cannot supervise the same PGDATA and that an
external PID symlink is rejected while its target remains byte-identical.
Destructive recovery settings are overridden, TCP is closed, and the private
Unix listener rejects all SQL through an immutable HBA policy. The test kills
the postmaster and requires the agent to fail terminally,
restarts through crash recovery with the same data and socket volumes, then
verifies clean shutdown removes `postmaster.pid` while a pending archive WAL
segment and its `.ready` marker remain retained. Persistent physical and logical
slots must remain valid across the idle timeout, WAL growth, recovery, and clean
shutdown. PostgreSQL's own CRC-checking control-data tool remains the authority
for interpreting control-file contents. Test
images and volumes remain runner-local and are not published.
The live PostgreSQL 18 fixture sends the production feedback frame through a
real COPY-BOTH connection, drains a catch-up keepalive, observes three distinct
positions in `pg_stat_replication`, receives the subsequent requested keepalive,
and completes COPY cleanly. It also creates a two-phase logical slot and proves
that protocol v1 with a
later `two_phase=false` request still emits and decodes Begin Prepare and
Prepare plus exact Relation, Insert, Update, Delete, and Truncate metadata from
the live `bRIUDIRTP` sequence. A second live slot decodes nontransactional and
streamed custom Message records with opaque binary contents. It proves that a
Message emitted inside a savepoint retains the top-level XID while the Relation
record carries the savepoint XID. Schema, row, and custom-message unit tests
cover buffered versus streamed layouts, distinct top-level and subtransaction
XIDs, nested/unmatched stream controls, every truncated prefix, tuple markers
and lengths, replica identity, reserved flags, UTF-8, zero-copy iteration, and
redaction. Complete transaction ordering, relation caching, feedback scheduling
and durable-checkpoint restart integration, replay, and cross-shard stream tests
are still absent. Targeted KIND tests verify operator PVC deletion and
same-name recreation, survival of the ownerless protected live PVC when its
anchored credential tombstone is deleted, garbage collection of a late PVC
create carrying that deleted Secret owner fence, and release of the exact
protected PVC when an explicit delete precedes Retain cluster finalization.
Fault-injection tests additionally fail the PVC UID checkpoint, delete that
unprotected exact outcome, and prove Retain records abandonment without issuing
a replacement create. CRD-only tests cover rejection of unsupported topology
and storage transitions, and server-side-apply field pruning after PostgreSQL
parameter, Service annotation, and OTEL configuration shrinkage. A delayed rollout keeps
the old immutable PostgreSQL ConfigMap available until the workload reports the
new revision, then proves it is pruned. The same suite covers both fresh creation
and a pre-SSA whole-object Update upgrade. The legacy fixture proves type-aware
alignment prunes stale desired fields while preserving API-assigned Service
cluster/IP-family fields exactly. An external Apply-manager annotation makes
migration fail before a write and remains intact until that manager explicitly
relinquishes it. The fixture also proves the crash-detectable managed-field
migration then completes without an Update co-owner. A stale-cache conflict
fixture proves that newly authoritative operator-owned marker-plus-Apply
ownership stops the legacy Update path even when scale and external Apply
managers are visible. A
historical Create-plus-Apply fixture proves markerless operator Apply ownership
does not bypass alignment: keys retained only by the create-time Update owner
are removed before that owner is migrated away.
Interrupted and completed upgrades from the earlier whole-Deployment HPA
handoff are reconciled and reduce `pgshard-hpa-scale` ownership to
`spec.replicas` only. Interrupted and completed handoffs whose CR returns to
fixed scaling preserve the configured capacity and remove that manager and its
legacy annotation entirely. Fixed-mode race tests inject both a late scale
write before the first read and another after a relinquishment conflict; each
must reestablish the configured value and operator ownership before removing
the old manager. HPA handoff rereads the authoritative Deployment, uses UID and
resource-version preconditions, preserves current capacity, and retries
concurrent controller updates within a fixed bound. It owns only
`spec.replicas` against the real Kubernetes 1.36 API server for both initial
HPA configuration and fixed-to-HPA transitions. A full-Reconcile real-API
fixture gives the client cache a false HPA absence while the uncached reader
still sees it, and proves the HPA is deleted in a separate pass before fixed
replicas are claimed. A separate KIND
job builds local images, installs the real manager with self-managed admission
certificates, proves the generated serving chain and injected CA bundles,
observes semantic validation reject an unsafe synchronous singleton, waits for
leader-elected reconciliation, observes a restart-free etcd quorum, keeps
rejection-only pooler and persistence-free orchestrator containers running
without restarts while they remain unready, and proves the three-member sample
has no PostgreSQL workload or ready application endpoint. The same job creates
a restricted two-shard, one-member sample, waits for both PostgreSQL 18
primaries, proves shard passwords differ, executes SQL across an internal shard
Service from an authorized restricted probe client using the destination-specific
Secret, proves an omitted storage class was resolved and checkpointed on the
resulting Bound PVC, then restarts one primary StatefulSet and verifies its
PVC-backed row survives. A unit Create interceptor separately proves the
credential UID, detached credential-Secret creation fence, and resolved storage
class are durably checkpointed before PVC dispatch; the live test confirms the
resulting Bound PVC exactly matches that checkpoint, is ownerless with the
operator's data-protection finalizer, and owns the credential tombstone by its
exact UID before any workload is published. Lost-response tests cover each
protection, detachment, and tombstone-anchoring update boundary. A delete request
cannot free the same claim name while its workload still exists. Outcome-unknown
tests cover both an uncommitted
timeout and a committed create whose response is lost, including the
`AlreadyExists` recovery path. They also mutate the live size, class, and policy
after provisioning and prove finalization uses only the stored snapshot. It also
reads the durable cluster-UID and shard identity marker
from the running data directory. Foreground deletion of the cluster then proves
the default-retained PVCs are detached, keep their exact recorded UIDs, and have
their credential tombstones removed. A behavioral init test supplies a marker
from another cluster or shard and proves bootstrap refuses it before entering
initialization; the validated fast path repeats the filesystem durability
barrier before exit. It does not
claim uninterrupted traffic during that restart. A unit
regression gives the informer cache a false absence while the authoritative API
reader still sees an owned PVC, and proves that finalization continues waiting.
Operator unit tests also prove deterministic common, primary, and per-standby
configuration rendering; self-excluding primary candidate sets; `ANY 1` versus
asynchronous role selection; mandatory
standby feedback and slot synchronization; and slot-capacity bounds for steady
primary, steady standby, and promotion-overlap states on one-, three-, and
five-member shards.
The broader runtime, integration, KIND,
Jepsen/Elle and PgBouncer comparison suites below remain required Milestone 1
work; see
[implementation status](../project/status.md).
:::

One GitHub Actions workflow fans out into independent jobs and ends in one required aggregate result. Pull requests run focused checks; scheduled invocations expand Kubernetes versions, fault duration, fuzzing, and performance samples.

The currently implemented planner and private router-core suites combine a
fixed malformed-SQL corpus with randomized properties over bounded Unicode SQL,
canonical parameter shapes, shard-key encodings, routing hashes, key-range
selection, malformed widths, and resolved-Bind parameter selection. This is
component coverage only; it does not make the absent SQL data plane an
end-to-end routing implementation.

The standard image job builds four Linux/amd64 Docker-compatible image archives
without a registry output, loads each archive locally, and verifies its
platform, build labels, numeric non-root identity, and binary entrypoint under
a read-only root filesystem. A separate change-filtered lifecycle job builds
only the agent and combines it with PostgreSQL 18 for the real lifecycle test.
The archives are discarded with the runner; CI does not upload or publish them.

## Test layers

| Layer | Focus |
|---|---|
| Unit and property | Parser, hash/range coverage, routing, epochs, buffers, tuning, state machines |
| Fuzz | Implemented for PostgreSQL startup, frontend/backend frames, typed bind bodies, stateful `pgoutput`, tuples and iterators; SQL, catalog snapshots and resume tokens remain planned |
| Integration | PostgreSQL 18 replication, 2PC crash points, failover slots, pgBackRest, missed notifications |
| KIND end-to-end | Bootstrap, services, scaling, failover, backup/restore, DDL, resharding, observability |
| Jepsen and Elle | Histories under process failure, partitions, primary changes, and resharding |
| Performance | Pooler versus PgBouncer and change-stream overhead |

Jepsen/Elle check the guarantees actually offered: atomic final cross-shard outcomes and `READ COMMITTED`. A history is not evaluated as serializable when the product does not claim serializability. Expected phase-two read skew remains documented and tested separately from atomicity violations.

## Required end-to-end environments

- MinIO for pgBackRest S3 backup and restore, including corruption and interruption cases.
- Prometheus, OpenTelemetry Collector, Grafana, and Tempo for metric/trace assertions.
- HPA and fixed pooler deployments.
- PostgreSQL and orchestrator failures at every durable 2PC boundary.
- Delayed `PREPARE TRANSACTION` commands before, during and after each
  participant fence acknowledgement and the coordinator abort CAS.
- Operation replay across executor crashes for same ID/same canonical envelope,
  plus stable conflict for same ID with each individual field changed and a
  delayed replay after terminal-state retention boundaries.
- Active DDL, reshard, and CDC streams during failover.
- Standby-first public streams and reshard materializers, including operator
  configuration of physical and logical slots, `sync_replication_slots`,
  `synchronized_standby_slots`, `primary_slot_name`, and mandatory
  `hot_standby_feedback`; disabling feedback, setting its report interval to
  zero or inside the fixed scheduling margin, or stalling feedback must fence
  the decoder.
- Embedded stream runtime topology: the operator creates no standalone stream
  Deployment or Pod. Selectorless query and stream Services resolve only their
  respective healthy named ports through operator-owned EndpointSlice entries
  on non-terminating pooler Pods; an
  owning `pgshard-pooler` stream-worker sidecar holds each dedicated native
  replication connection to the catalog-selected standby-local decoder, and no
  steady-state consumer walsender decodes from the primary anchor. The query
  container cannot read replication or checkpoint-mutation credentials, the
  sidecar accepts no PostgreSQL client sessions, and their UID-authenticated IPC
  rejects untyped and over-budget requests. Independently failing or stopping
  either container removes only its matching EndpointSlice entry; sidecar
  failure leaves a healthy SQL endpoint available even when kubelet marks the
  aggregate Pod unready. Kill the active operator before and after query/stream
  endpoint add, health removal, Pod UID/IP replacement, and Pod deletion. After
  leader recovery, each slice must idempotently converge, remove every stale
  address before advertising its replacement, and use the standard Service-name
  plus `endpointslice.kubernetes.io/managed-by=pgshard-operator` ownership labels.
- Fixed-size reconciliation and real HPA behavior are tested separately. The
  rendered `query-router` container has `resources.requests.cpu: 250m`. The
  `autoscaling/v2` HPA uses its `ContainerResource` CPU metric with
  `averageUtilization: 70`, a `Pods` metric named
  `pgshard_stream_queue_fill_percent` with `averageValue: 70`, and an `External`
  `pgshard_query_admission_pressure_percent` metric scoped by cluster with
  `value: 70`. KIND installs metrics-server and Prometheus Adapter; the adapter
  derives both Prometheus metrics from the pooler endpoints. Before load, the
  test requires `AbleToScale=True`, `ScalingActive=True`, and all three current
  metrics in HPA status. Query CPU, stream queue fill, and external query
  pressure must each independently drive an HPA scale-up. During sidecar
  failure, sustained SQL load must remain routed through the query EndpointSlice
  and the external metric must still cause bounded scale-up even though kubelet
  marks that Pod unready. After all metrics fall below threshold, an
  HPA-selected scale-down Pod transfers fenced stream ownership only after its
  bounded pre-stop drain. The test may not substitute a direct Deployment
  resize. Durable-checkpoint replay must remain at least once, while stream
  backpressure and memory limits leave SQL routing ready.
- Ownership-takeover faults partition or stop the old worker before and after
  each durable-intent, quarantined-connect, pidfd-registration,
  generation-revalidation and activation transition for COPY-BOTH and
  independent exported-snapshot holders. No PostgreSQL side effect may precede
  registration. Its local lease deadline must stop emission and close all
  sessions; a successor must rotate the durable fence, reject stale checkpoint
  CAS, mark incomplete snapshots for resnapshot, resolve every intent, signal
  each exact process through an agent-held pidfd, prove all are absent and the
  slot inactive, and only then resume. Exit and PID-reuse races are injected
  around pidfd open and identity revalidation. Failure to prove any intent or
  step remains fenced.
- HPA termination during snapshot copy either completes and durably spools
  `SnapshotComplete` inside the drain bound or marks the generation
  `ResnapshotRequired`. A replacement Pod must never resume a mid-copy cursor
  against a different exported snapshot.
- Independent standby-local slots plus synchronized primary failover anchors
  across source loss and promotion, with no consumption of a synchronized slot
  before promotion and no cross-consumer or cross-database checkpoint or slot
  ownership.
- Slot-sync health from one SQL-capable connection database on the exact direct
  primary, including acceptance when that database differs from a logical
  slot's database and rejection of a missing `dbname`, wrong primary, nil or
  changed worker connection generation, success attributed to another
  connection generation, and a stale or absent successful cycle.
- Exact correlation of the standby's live WAL-receiver slot with the primary's
  member walsender and physical-slot `active_pid`; configured names without
  matching live ownership, a wrong application name, and an inactive or
  differently owned physical slot all fail closed.
- A quarantined `START_REPLICATION` race suite that advances slot progress
  after preflight, substitutes another active backend PID, changes every
  preflight invariant in turn, mismatches the actual `BackendKeyData` PID or
  encoded start LSN from the pure report, and proves no decoded record or
  acknowledgement escapes before the future connection-owned authorization.
- Managed slot lifecycle tests that prove the restricted operator path never
  uses `ALTER_REPLICATION_SLOT`, recreate on an operator-requested mode change
  or lost lifecycle attestation, and reject a changed visible `two_phase_at` or
  attempted reuse of a retired name or generation. A direct privileged
  true-to-false-to-true round trip documents the trust boundary: restored
  visible state alone is indistinguishable, so external superuser mutation is
  unsupported rather than falsely claimed as detectable.
- Switchover and former-primary rejoin while same-named primary anchors remain:
  synchronization and promotion eligibility stay fenced until checkpoint-safe
  managed-slot cleanup or a rebuild, managed cleanup leaves unrelated user slots
  untouched, unknown collisions fail closed, and the recovered stream has no
  gaps.
- Source attachment rejects a same-name and same-OID database from another
  PostgreSQL system identifier, and rejects a physical restore that reuses the
  system identifier and OID without a fresh shard restore-incarnation UUID.
- CDC reconnect/replay before every chunk and terminal event, including a
  transaction exactly at and one byte/event above configured flow limits, a
  checkpoint emitted while the data window is exactly full, and individual
  snapshot/relation/reshard events at and above their byte limit.
- CDC token tampering, future-token acknowledgement, cross-stream/configuration
  replay, duplicate and stale acknowledgements, and signing-key rotation.
- Replay of an old resume token after a different-system reinitialization and
  after a same-system physical restore with a new shard incarnation; both must
  return `SOURCE_INCARNATION_CHANGED` without advancing checkpoint or slot state.
- CDC snapshot initialization with failures and delayed DDL, 2PC recovery,
  reshard activation, topology publication, and semantic configuration mutation
  at every barrier boundary.
- CDC disconnect and gateway crash at every snapshot relation/chunk boundary,
  plus snapshot-holder, slot, and primary loss during row copy; resume must use
  the retained snapshot or require a clean resnapshot without a silent gap.
- CDC managed DDL activation, relation swap/drop, and holder failure during row
  copy; copy-lifetime relation locks must prevent mixed schemas or storage.
- CDC disconnect and gateway crash after `SnapshotComplete` but before its
  checkpoint acknowledgement, including durable-spool write failure and
  retention expiry; replay must come from the old spool or require resnapshot.

## Pooler performance

The pooler and PgBouncer run alternately on the same runner with identical PostgreSQL, CPU, TLS, pool size, and prepared-statement configuration. Seven or more warmed 60-second samples report throughput, p50/p95/p99, CPU, and RSS. The initial fast-path target is at least 85% of PgBouncer throughput with no more than 1.15× its p95 latency; base-branch regression gates account for confidence intervals.
