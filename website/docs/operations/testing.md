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
migration principal; a separate rollback-only smoke path creates the registry
allocation set through
the restricted catalog-admin role. It also verifies that privileged functions
place the temporary schema last in their fixed search paths and that replaying
the migration cannot resurrect a retired restore incarnation or advance the
catalog epoch. The trigger suite rejects slot names that
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
expiry and proves no late snapshot is installed. The supervisor scenario
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
shutdown marks catalog state stopped. They also prove a catalog-ready process
stays application-unready. An HTTP regression holds a partial request under a
one-connection test policy and proves shutdown force-closes it after a bounded
drain; the production policy also bounds headers and connection lifetime.
Injected acceptor tests prove an accept error can recover, cancellation of an
in-flight wait retains its deadline and exponential retry state, and shutdown
can interrupt the capped retry backoff. A Linux subprocess test loads real
environment variables, a CLI override, and file configuration, serves health,
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
errors. Separate unit tests enforce PostgreSQL 18's tighter SCRAM frame bound,
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
Pure orchestrator tests exercise the standby-decoder attachment contract. They
require non-nil catalog generations encoded in slot names and matched exactly,
the current enabled two-phase mode and activation boundaries; a bounded
hot-standby-feedback interval
with a fixed scheduling margin; a slot-sync success from the current
direct-primary connection generation; exact live receiver, primary walsender,
and physical-slot-owner correlation; and a second complete invariant check
after the caller reports that a replication backend acquired the local slot.
The second check rejects a changed source or current mode, slot progress racing beyond the
durable checkpoint, a different reported active backend PID, and a reported
start other than that checkpoint before producing a non-authorizing report.
Pure values cannot prove which PID and start LSN an actual socket used. These
tests cover observations only; the catalog allocation lifecycle is tested
separately, while the live probe, controlled PostgreSQL slot lifecycle,
connection-bound command proof, quarantined COPY-BOTH attachment, and stream
owner are not implemented.
The `Orchestrator catalog / PostgreSQL 18` CI job applies the real migration,
constructs a ready reshard-materializer fixture, and loads its exact
restore-bound checkpoint, ownership fence, anchor, and member-local decoder in
one repeatable-read transaction. It also proves another member cannot inherit
the allocation, a committed ownership fence removes the policy from subsequent
reads, exact singleton values are retained, and corrupt ready rows with an
unfinished snapshot, seed ordinal, or missing attachment/slot fail closed. A
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
proof or creation attestation.
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
a leader that exits before its
descendant, unreaped pidfd observation, non-UTF-8 Linux process names, and
cancellation of the supervision future; cancellation also reaps the direct
child before the PGDATA fence can be reacquired, and the complete PostgreSQL
process group must be dead.
Linux subprocess coverage proves the control listener binds before child
creation, reports the quarantined process state and metrics, remains unready
without a lease, bounds HTTP drain even when a client holds partial headers,
propagates postmaster crash and `SIGTERM`, and leaves no child behind. A separate
local-image CI job initializes a real PostgreSQL 18 data directory, rejects
standby, archive, incomplete base-backup, and missing-signal recovery control
state before process creation,
rejects directory mounts at both `pg_wal` and `base/1` plus a bind-mounted
regular `PG_VERSION` file, and proves a configuration-file data-directory
redirect cannot escape the validated path.
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
are still absent. A targeted KIND test verifies operator PVC deletion and
same-name recreation against real Kubernetes 1.36 controllers. A separate KIND
job builds local images, installs the real manager with self-managed admission
certificates, proves the generated serving chain and injected CA bundles,
observes semantic validation reject an unsafe synchronous singleton, waits for
leader-elected reconciliation, observes a restart-free etcd quorum, keeps
rejection-only pooler and persistence-free orchestrator containers running
without restarts while they remain unready, proves application Services have no
ready endpoints, and verifies no PostgreSQL workload exists. A unit
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
| Fuzz | PostgreSQL frames, SQL, bind messages, `pgoutput`, tuples, catalog snapshots, resume tokens |
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
