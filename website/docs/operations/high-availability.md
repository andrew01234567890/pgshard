---
title: High availability
description: Replication, leases, fencing, promotion, restarts, and buffering.
---

# High availability

:::info Milestone 1 design contract
The Rust agent/orchestrator health surfaces and fail-closed in-memory fencing
models exist. The operator creates one empty `coordination.k8s.io/v1` Lease
envelope for orchestrator leadership and one empty, role-neutral writable-term
Lease envelope per physical cell. It checkpoints every cell Lease UID in
`PgShardCluster` status and refuses missing, recreated, foreign-owned, or
malformed envelopes. The orchestrator's dedicated ServiceAccount is restricted
to `get` and `update` on that exact Lease name; the Rust process connects through
the Kubernetes API server's in-cluster TLS path and never connects directly to
the control plane's private etcd. Claims and renewals are full
resource-version-conditional replacements bound to the exact fleet owner UID,
Lease UID, Pod UID, and process incarnation. `/readyz` succeeds after a current
authoritative observation of that identity. All healthy replicas may be ready,
but only the current holder reports `pgshard_orch_leader 1`.

A follower does not trust the holder's wall clock. It can take over only after
observing the same holder and renewal record unchanged for a complete locally
measured Lease duration, following CloudNativePG's conservative observation
pattern. A clean empty release can be claimed immediately. A monotonic local
deadline is anchored before each conditional replacement is dispatched, so a
delayed successful API response consumes rather than extends leadership.
Readiness is removed after API loss or a process pause. Routine runtime Lease
renewals do not enqueue a full cluster resource reconciliation; create, delete,
ownership, deletion-transition, and operator-owned envelope changes still do.
Recreating the Lease changes its UID, so existing processes remain fail closed
until a bounded orchestrator rollout establishes a new coordination universe.
This Lease proves only orchestrator availability and exclusive leadership. It
does not persist operation or shard-term records, provide target-side
PostgreSQL fencing, or enable automated failover.

Every cell has a separate role-neutral ServiceAccount with token automount
disabled. Its Role and RoleBinding allow only `get` and `update` on that cell's
exact Lease name. The default direct PostgreSQL runtime does not mount this
identity. The explicit quarantine integration selects it and uses a bounded
projected token rather than ambient API credentials.

The opt-in agent has a separate per-cell Lease transport. Its holder identity
combines the stable member name, exact Pod UID, and a fresh random process
incarnation, and every claim or renewal pins the fleet UID, Lease UID, and
resource version. A container restart must wait out and take over its
predecessor's record, advancing the Lease transition counter rather than
reusing the old fencing term. A candidate times an unchanged foreign record
locally for a full Lease duration instead of comparing the holder's clock. A
successful response is anchored to the monotonic instant before the request was
dispatched, so API latency consumes authority and wall-clock jumps cannot extend
it. The wall-clock expiry exposed by status is diagnostic only: a backward wall
step on a same-term renewal is clamped, while only the later monotonic deadline
can preserve authority. A configured standalone postmaster waits until the exact term remains valid
beyond the complete fencing margin. Shutdown before acquisition leaves no
PostgreSQL process, and the supervisor checks the monotonic margin again at the
final user-space boundary while also rejecting an observed shutdown. A stale
notification or pause during validation cannot authorize startup, and a
shutdown already observable at that boundary leaves PostgreSQL absent.

The same private authority carries an exact cell generation: cluster name and
UID, physical-cell ordinal, Lease namespace/name/UID, holder identity, and
transition term. Before creating a postmaster, the agent first matches the
operator's canonical `.pgshard-bootstrap-complete` cluster-and-cell identity.
It then publishes `.pgshard-writable-generation` through a fixed `.next`
record, file flush, atomic rename, and PGDATA directory flush. An interrupted
staging file has its ownership, mode, mount, size, and inode identity validated
before it is discarded under the exclusive PGDATA lock. Exact replay completes
the same durability barrier, and a higher requested term can
replace a lower durable term. A durable higher term, foreign Lease universe,
malformed record, or same term with a different holder blocks startup. The
agent samples the attempt-private authority and shutdown state again after the
flush, so a slow storage barrier cannot authorize an expired or changed term.
The record is cell-scoped rather than member-scoped: a later member may advance
it only by holding a higher term from the same exact cell Lease.

After the postmaster is created and tracked by pidfd, it remains
`StartingQuarantined`. The HBA permits only the operating-system `postgres`
identity to connect as `postgres` to the `postgres` database over the private
0700 Unix socket; every other local connection and every replication connection
is rejected, and TCP remains disabled. In a fixed `pg_catalog` search path with
bounded transaction, statement, lock, and idle timeouts plus
`synchronous_commit=on`, the agent locks a singleton row in an owned,
WAL-logged `pgshard_internal.writable_generation` table. It accepts only an
empty record, exact replay, or a higher term in the same Lease universe. The
attempt-private authority must still exactly match immediately before commit.
If the commit response is lost, a fresh connection accepts the exact requested
row as committed; the exact old or empty state may retry only while authority
still matches. Malformed, foreign, conflicting, higher, or otherwise changed
state fences the postmaster. The same fence applies to unknown reread timeout,
shutdown, Lease loss, publication timeout, and child exit. Only a committed or
reconciled row followed by one final exact authority check advances the process
to `RunningQuarantined`.

A private opt-in publication mode exercises the next durability boundary against
a disposable PostgreSQL 18 primary and physical standby. It commits with
`synchronous_commit=remote_apply`, then captures a primary flush barrier and
accepts exactly one canonical managed standby identity only while its walsender
is streaming through its same-named active physical slot and selected as `sync`
or `quorum`. Both the standby flush and
replay positions must cover that barrier before the primary row and
attempt-private authority are rechecked. Missing, duplicate, asynchronous,
lagging, unknown, disconnected, or changed evidence remains fail closed. The
live test pauses standby replay, observes the primary publication blocked in
PostgreSQL's synchronous-replication wait, resumes replay, and requires the exact
row to be readable on the standby before publication returns. The operator and
agent runtime do not select this mode yet.

With writable coordination enabled,
every agent shutdown clears local term evidence and immediately enters the
PostgreSQL process-tree fence, skipping the smart and fast waits. An absolute
monotonic renewal cutoff stops awaiting an in-flight Kubernetes API request
rather than letting response latency consume the fencing margin. The write
might already have committed; resource-version CAS prevents a later stale
overwrite, and a candidate restarts the full unchanged-record observation
window from the changed Lease. Startup rejects a Lease/shutdown combination
unless the post-renewal margin strictly exceeds the configured immediate-stop
and normal cleanup budget. The default operator runtime remains direct. The
explicit `--postgresql-runtime=agent-quarantine` integration mode selects the
exact per-cell ServiceAccount, mounts a rotating 600-second projected API token
with the namespace CA/name, injects the checkpointed Lease UID and downward-API
Pod identity, and runs the agent against the role-neutral PGDATA. Its readiness
probe stays failed. Singleton quarantine keeps PostgreSQL TCP closed; the
multi-member replication-bootstrap source described below uses a replication-only
listener while remaining non-serving. If coordination fails,
the agent clears the term, fully fences PostgreSQL, keeps HTTP liveness
available, and retries with bounded backoff while remaining unready. Recovery
uses a fresh process incarnation, waits behind any old holder, advances the
term, and starts the quarantined postmaster again in the same container. On a
requested process shutdown, coordination stops renewal and retains only the
latest exact resource-version release receipt while PostgreSQL enters its
complete process-tree fence. The writable supervisor emits the matching half
of a single-use, non-cloneable absence proof only after cleanup succeeds; that
proof is required to consume the receipt. The conditional replacement clears
only the exact holder and does not advance the transition counter. Pair
creation, both supervisor lifetimes, and release are private to one composed
runtime operation, so callers cannot retain a proof across attempts. The
coordinator publishes monotonic startup authority only on that attempt's
identity-tagged private channel. Cloneable agent status is observational and
cannot authorize a concurrent attempt. A mismatched proof, conflict, timeout,
lost response, or response mismatch is not retried with stale evidence, so the occupied-record
expiry protocol remains the safety fallback. Controller reconciliation accepts
only the pristine envelope, a complete occupied term, or this complete released
term; partial released history and empty-string holders fail closed.
Runtime selection is fixed before the workload is created and checkpointed in
cluster status before credentials or storage. A manager flag mismatch fails
even after the StatefulSet and Pod are deleted. Before planning, the controller
also authoritatively classifies both the `OnDelete` StatefulSet template and
the live Pod, including when an earlier template already differs from its Pod.
Changing modes requires a future explicitly fenced replacement workflow.
These durable records are only non-serving startup floors. The WAL-backed row
has a tested synchronous-replay proof primitive, but this runtime still disables
physical replication and therefore provides no runtime synchronous-replica
durability guarantee.
PostgreSQL SQL write and prepare hooks do not yet reject stale request
generations, standby copies are not yet reconciled as promotion evidence, and
promotion proof plus serving activation remain absent. This is therefore not
yet a serving-primary fence or an HA claim.

The pre-Lease development layout's three etcd data claims are retired
automatically. The operator first prunes the old cluster-owned StatefulSet,
uses an uncached namespace-wide Pod list to prove that no Pod of any identity
references any retained claim, validates the exact owner, labels, 2 GiB storage
contract, and fixed claim names, and then deletes those claims with UID and
resource-version preconditions. A mismatch or mount blocks deletion. No
PostgreSQL data claim participates in this migration.

On `SIGTERM`, the process removes readiness before notifying its workers,
stops awaiting any in-flight Kubernetes API request, limits best-effort Lease
release to one second, and limits HTTP plus coordination drain to ten
seconds. The operator explicitly gives orchestrator Pods a 30-second Kubernetes
termination grace. Lease expiry remains the cleanup backstop when revocation
cannot complete.

The in-memory models bound authenticated lease lifetimes and atomically reject
an operation unless its catalog epoch, fencing epoch and deadline match at
execution. An opt-in local lifecycle boundary can structurally preflight
required PostgreSQL 18 files, supervise one direct postmaster child with client
TCP and replication ingress disabled, propagate unexpected exit, and perform
bounded smart/fast/immediate signal escalation and HTTP drain. Final cleanup is
deliberately fail-closed rather than time-bounded: the PGDATA fence stays held
until the direct child is reaped, its original process group is empty, and the
dedicated Linux child subreaper has reaped every adopted descendant, including
when supervision is cancelled. A filesystem-backed exclusive
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

The operator's direct one-member development mode is intentionally outside the
HA contract. It creates one writable PostgreSQL 18 Pod per shard and retains its
PVC across a StatefulSet restart, but the shard is unavailable while that Pod
restarts. Before publishing that Pod, the controller protects the checkpointed
PVC UID with its own finalizer, makes the live PVC ownerless, and anchors the
credential tombstone back to it. The exact claim name cannot be reused until
the mounting workload is pruned, so a late empty claim cannot enter bootstrap.

For a three- or five-member resource, the default `direct` runtime creates no
PostgreSQL storage or workload. An explicit `agent-quarantine` manager now
checkpoints one uninitialized source-storage intent for every stable physical
member. Each uses an immutable member bootstrap Secret, exact UID checkpoints,
outcome-unknown create fence, protected ownerless PVC, and Retain/Delete
finalization. Status, Secret, and PVC identity are keyed by shard and member;
the PVCs deliberately have no role label. The controller also stages one
unpredictably named replication credential per shard: it checkpoints an empty
Secret intent, its API UID, and then the exact immutable password digest. Once
those checkpoints are complete, the controller atomically initializes member
zero and runs one role-neutral `replication-bootstrap-primary` agent per shard.
Its immutable HBA rejects ordinary SQL, the Pod remains unready, and no
application Service selects it. Only the init container receives the one-key
replication Secret projection. It verifies the checkpointed digest, creates or
validates the fixed least-privilege SCRAM login, proves the exact password over
the physical-replication protocol, and immediately reserves one exact slot for
each other configured member before publishing the durable HBA. The running
agent has no Secret mount. No standby, PDB, catalog credential,
`primary_conninfo`, or serving endpoint is created. A missing or same-name
recreated Secret or PVC fences the bootstrap-source controller against the
recorded UID; changed
replication material fails closed against its recorded digest. This source is
not evidence of a primary, standby, synchronous replica, or HA availability.

Each managed PostgreSQL Pod is created with a cluster-UID-bound termination
finalizer. Before workload publication, a cluster challenge update proves that
the selected admission path has observed the namespace's immutable fencing
opt-in and returns an HMAC receipt bound to the cluster UID and fresh challenge.
This does not prove simultaneous selector-cache convergence across every API
server. The receipt key is an independent immutable Secret whose SHA-256
continuity fingerprint is anchored in backward-compatible CA Secret metadata.
Fresh-install state is recorded only after the CA, serving, and key Secrets and
webhook trust bundles are empty and no PostgreSQL lifecycle or pending cluster
handshake metadata exists. Recreating both authority Secrets cannot therefore
masquerade as installation.
The last keyless release has a distinct two-phase upgrade: the applied manifest
requests the first key in the new empty Secret, then the manager verifies the
existing CA and serving material, requires both legacy PgShardCluster webhook
trust bundles to contain that CA, and requires every newly introduced receipt
webhook trust bundle to remain empty before recording authorization on the CA.
Only then can it generate and anchor the key. The same proof establishes that
receipt-looking annotations stored by that keyless release were never
authenticated and are not continuity history. An existing initialized but
unanchored key must instead be pinned while the old manager is healthy before a
mixed-version rollout; the new manager
refuses to infer continuity from receipt listings because those listings cannot
fence an older signer. It inspects every single-member cluster handshake and
requires every established cluster handshake and managed terminal Pod receipt
to verify before writing a separate completion marker. Invalid or incomplete
cluster metadata remains repairable only before the lifecycle finalizer or
PostgreSQL bootstrap status exists; PostgreSQL storage and workloads cannot
precede that barrier. Users cannot establish, remove, or replace the handshake.
The controller may establish or repair it only when both its
service-account identity and the final HMAC receipt verify; the pair is
byte-for-byte immutable during deletion. Readiness,
receipt-authenticated admission, and controller key use require the exact key to
match that anchor; recreating an empty or different key fails closed and does not
mint replacement authority.

An upgrade from the keyless default-branch release needs no manual key command,
but it crosses an incompatible admission contract. Freeze `PgShardCluster`
writes and do not opt a resource namespace into Pod fencing during that rollout.
The webhook Service uses a dedicated `9444` client port and selects only
`receipt-v1` manager Pods. The port change prevents the API server from reusing
a cached connection to the keyless manager's `443` client, while the selector
prevents new connections from reaching that manager. Admission therefore fails
closed, and clients may receive retryable webhook-unavailable errors until the
new manager is Ready. Deployment process availability does not make the old
process admission-compatible. Wait for rollout completion before retrying
writes or enabling Pod fencing. A zero-rejection transition needs staged
compatibility tooling and is not implemented. Later compatible rollouts have a
20-second total termination budget: the first five seconds drain stale Service
endpoints before `SIGTERM`, leaving roughly 15 seconds for graceful shutdown. Do
not restart the old image with the new command-line arguments. For a pre-release
development install that already has an initialized unanchored key, record the
current
immutable key before changing the
manager image. Run this while the old manager is healthy:

```console
KEY_UID="$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.metadata.uid}')"
KEY_SHA256="$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.data.hmac\.key}' | base64 -d | sha256sum | awk '{print $1}')"
kubectl --namespace pgshard-system annotate --overwrite secret pgshard-webhook-ca "pgshard.io/pod-fencing-key-sha256=${KEY_SHA256}"
test "$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.metadata.uid}')" = "${KEY_UID}"
```

If the UID check fails, do not roll out the new manager. This explicit
development upgrade boundary has no automated production tooling yet.
Binding admission copies the managed identity and selected Node UID and
boot ID into the Pod atomically with `spec.nodeName`. Both admission stages
authoritatively require that identity's exact PgShardCluster UID to exist and
not be deleting, and a final validator checks the result after every mutator.
The init container reads those annotations
through the Downward API and exits before touching PGDATA if either is absent,
which independently closes a missed-binding-admission path. Status admission
rejects attempts to remove the managed identity, binding identity, or finalizer
through the status subresource. On deletion, it adds a durable HMAC-authenticated
process-terminated condition only to a terminal update from the authenticated
kubelet for that same live Node incarnation, and a final validator rechecks the
post-mutation status. PodGC's control-plane-authored `Failed` phase and copied
condition text cannot create this condition. The managed Pod spec and generation
are immutable across main-resource, ephemeral-container, and resize updates, so
new process intent cannot be introduced after that proof. An uncached
Pod read must then prove absence before PVC protection is released. A Pod that
commits just after that read cannot bind while the cluster is deleting, even if
the selected kubelet still caches an earlier immutable Secret. If admission
is unavailable, the Node is absent or rebooted, or the Node name now identifies
another UID, deletion deliberately remains blocked and the PVC stays protected.
Manually removing the finalizer is not a storage-fencing operation and is
outside the safe lifecycle boundary. Credential-only client Pods do not own PGDATA and do
not block retention after the PostgreSQL process proof; their existing sessions
are severed when that process stops.

The receipt proves only what the authenticated kubelet reported for the pinned
Node incarnation; it does not power off a machine or revoke storage access. Do
not reuse a Node name while its old pgshard Pod may still exist. A lost or
replaced Node requires external machine or storage fencing and explicit
recovery. `ReadWriteOnce`, Pod phase, and an out-of-service taint are not treated
as that fence.
Its init container binds the durable bootstrap marker to the exact
cluster UID and shard and repeats the scoped final-data and parent-directory
publication barrier before
accepting an existing marker, while the running PostgreSQL container does not
receive or mount the bootstrap password. `PostgreSQLPrimariesAvailable=True` reports
process availability, not replication or failover. Zero-downtime restart
evidence requires at least one eligible standby, a fenced switchover, pooler
rerouting/buffering, and a continuous client probe with no failed or
outcome-unknown transactions.
`pg_isready` is used only for readiness. Direct PostgreSQL Pods have no kubelet
startup or liveness kill probe: PID 1 exit is handled by the container runtime,
while slow startup or crash recovery remains unready without being repeatedly
killed by kubelet.

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

## Agent process-fence recovery

The configured smart, fast, immediate, and kernel-kill phases are bounded, but
releasing the PGDATA supervisor lock is not. PostgreSQL 18 children call
`setsid()`, so the postmaster's original process group does not contain every
backend and auxiliary process. Before spawn, the dedicated agent enables Linux
child-subreaper mode and refuses to proceed if it already owns another direct
child. After observing postmaster exit without reaping it, the agent first
empties the original process group while the zombie leader keeps its PID and
PGID reserved. It then reaps the postmaster and repeatedly adopts, pidfd-kills,
and reaps each remaining process-tree root until no direct child remains.
Killing one adopted root can reparent another generation to the agent, so the
absence proof is repeated.

A transient `/proc` inspection error is retried and is not reported as a
surviving descendant unless a live member was actually observed. A persistent
inspection or reap error deliberately leaves the Pod terminating and the volume
fenced; another `SIGTERM` does not bypass that proof. Crossing the bounded
cleanup interval is reported only after complete absence is eventually proven.

If an agent logs that it cannot prove the PostgreSQL process tree is dead:

1. Do not start or force-attach another PostgreSQL process to the same PGDATA.
2. Cordon and fence the affected node, then restore `/proc` and container-runtime
   visibility or stop the old container cgroup from outside the Pod.
3. Verify that the old process group, adopted descendants, and every process
   with the volume open are absent before force-deleting the Pod or moving the
   volume.

Sending `SIGKILL` to the agent or allowing Kubernetes' final kill releases the
userspace flock without proving descendant death, so it is not a safe recovery
shortcut. The Pod termination grace period must cover the configured shutdown
budget and HTTP drain; a node-level fence is still required when the kernel can
no longer supply process-table evidence.

The agent installs its `SIGTERM` and `SIGINT` streams before reading
configuration, walking PGDATA, binding HTTP, or spawning PostgreSQL. If signal
registration fails, startup exits before it can own a postmaster. PGDATA is
walked once during offline preparation and again immediately before spawn to
close the preparation-to-exec mutation window. Each walk has a 30-second
deadline and a timeout fails before process creation; operators should validate
that their largest intended data directory and storage class meet this startup
contract.

The target default is one primary and two physical streaming replicas per shard,
spread across failure domains. PostgreSQL will use `synchronous_commit=on` with
`ANY 1` synchronous standby acknowledgement. An explicit asynchronous policy is
a durability downgrade and must be surfaced as such.

## Primary fencing

The primary must eventually hold an operator-owned
`coordination.k8s.io/v1` Lease for its physical cell. Components access that
Lease through the Kubernetes API server; they never connect directly to the
control plane's private etcd. The Lease record is short-lived coordination, not
durable authority: topology, operations, fencing generations, and transaction
decisions remain in PostgreSQL. The operator now creates and UID-checkpoints an
empty Lease envelope per cell with an unmounted exact-name API identity, and the
opt-in Rust agent implements the exact claim, renewal, takeover-observation, and
monotonic-deadline transport. A configured writable term forces every shutdown
through immediate process-tree fencing, and unsafe Lease/fence timing pairs are
rejected. The operator's default runtime remains direct. For the supported
single-member integration, explicit `agent-quarantine` injects that runtime
into a non-serving PostgreSQL Pod. For multi-member resources it now selects
the agent's `replication-bootstrap-primary` role for member zero after every
source-storage, Lease, and replication-credential checkpoint is complete. The
agent's default quarantine role keeps TCP disabled. The bootstrap role requires
the same exact writable Lease,
accepts only the fixed `pgshard_replication` SCRAM role over TCP, rejects every
ordinary database connection, and explicitly permits up to four configured
member senders and slots plus one bounded bootstrap-repair sender and slot of
headroom. Logical consumers remain uncomposed here. The role deliberately
clears synchronous standby selection and uses local commit while bootstrapping,
so it is neither a durable nor serving primary. A separate uncomposed
`replication-standby` role requires
an exact protected `standby.signal`, a recovery control-file state, a canonical
member slot, and a runtime-owned `0400` passfile outside PGDATA and the socket
directory. The passfile must contain one bounded record for the exact configured
primary and fixed replication role; its identity, metadata, and contents are
snapshotted again immediately before spawn. Its command contains no password,
keeps TCP closed, and uses the private peer-authenticated socket to prove both
recovery and an initially streaming WAL receiver before reporting the distinct
running state. A valid postmaster may remain sealed in the starting state for
unbounded crash recovery or while its upstream is unavailable, avoiding a
restart loop. Once running, it continuously repeats the recovery proof; recovery
ending, an unknown query result, timeout, or local connection loss immediately
fences the complete PostgreSQL process tree. A later upstream outage alone does
not kill a server that remains safely in recovery. Writable Lease authority is
forbidden for this role. The operator does not select the standby role yet. It
checkpoints one per-shard replication password through an empty intent, exact
API UID, immutable update, and material digest. The source init container alone
projects it, proves its digest and physical-protocol authentication, creates the
fixed replication role, and reserves the exact member slots; the main agent has
no credential mount. There is no base-backup completion protocol, TLS wiring,
standby Pod, standby-specific Service, or standby-specific NetworkPolicy.
The role-neutral source can be selected by the existing shard headless Service and
cluster PostgreSQL NetworkPolicy, but its unready state, absent serving role, and
replication-only HBA keep it outside application routing. The temporary standby
conninfo therefore explicitly disables TLS and is not a serving deployment.
SQL and
prepare target-side generation enforcement, replicated promotion evidence,
peer isolation, promotion, and serving activation remain absent. The
fleet-level orchestrator leadership Lease remains
a separate control-plane mutex and is deliberately not a shard term.

As in CloudNativePG, a candidate observes a competing holder's Lease record
locally and may take it over only after the same holder and renewal record stay
unchanged for a full lease duration. Resource-version conditional updates pick
one winner without trusting the old holder's wall clock. The agent translates
each proven response into a monotonic local deadline and stops renewing at a
separate earlier deadline; the deadline race stops awaiting any in-flight Lease
request. An unknown commit cannot overwrite a later resource version, and a
visible commit restarts a candidate's full unchanged-record observation window.
A coordination failure clears that evidence and requests immediate process-tree
fencing. That local mechanism is necessary but not sufficient: the durable
record is only a pre-start floor, not target-side SQL or prepare enforcement.
The serving-primary lifecycle, peer-isolation policy, replicated promotion
evidence, and promotion ordering remain unimplemented, and a Kubernetes Lease
alone cannot fence an isolated process or node.

The existing in-memory state machines model the later boundary. Both the
orchestrator authority and receiving agent reject expired or overlong leases;
configured TTL bounds are enforced by the state machines, not merely logged.
The in-process orchestrator retains Unix expiry only for request validation and
reporting and uses a separate monotonic deadline for live ownership, so a
wall-clock step after installation cannot change the local term. During
acquisition it pairs two wall reads with preceding monotonic samples, uses the
greater wall value for admission, installs the earlier of the two translated
monotonic deadlines, and proves admission against a final monotonic sample. A
pause combined with a backward step between the wall reads can therefore only
shorten or reject the candidate term, never extend it. A renewal extends the
installed monotonic deadline only by its requested Unix-expiry delta and cannot
move that deadline beyond the current TTL policy.

A lease-acquisition outcome reports an in-memory mutation; it is not execution authority. The returned grant must be revalidated against the exact installed term, monotonic deadline, catalog epoch, fencing epoch, and operation immediately before dispatch. The validation clock is sampled while holding the shared state lock after term inspection and again after epoch checks, so mutex contention or a pause during validation cannot carry a stale timestamp past expiry. Even that local guard is an instant-in-time check: every target operation must carry the epoch and atomically reject a stale epoch. Poolers route writes only to the primary identity and term currently authorized by those target-side fences.

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
