# pgshard operator

This module contains the Go control plane for the namespaced `PgShardCluster`
API. The controller now reconciles the safe supporting-resource slice:

- generated topology and immutable, content-addressed, resource-derived
  PostgreSQL configuration ConfigMaps, including common plus primary and
  standby role profiles for every member;
- CNPG-style `<cluster>-rw`, `<cluster>-ro`, and `<cluster>-r` application
  Services, each targeting its own pooler listener;
- one internal headless Service per shard;
- for an explicit one-member asynchronous topology, one digest-pinned
  PostgreSQL 18 primary StatefulSet and retained data PVC per shard, with a
  generated per-shard immutable bootstrap credential and restricted Pod
  security;
- an idempotently migrated `shardschema` database on shard-0000 containing the
  complete immutable shard inventory and an initial restore incarnation for
  each shard;
- a status-only `PgShardRestore` controller that verifies a signed PostgreSQL
  18 backup manifest and rejects any explicit destination shard-count, hash,
  ordinal, or range mismatch before creating restore targets;
- etcd, orchestrator, and pooler workload specifications, topology spread,
  security contexts, PodDisruptionBudgets, HPA or fixed pooler scaling, and an
  etcd ingress NetworkPolicy;
- same-cluster-only PostgreSQL ingress on port 5432 for pooler and orchestrator
  Pods, with PostgreSQL-to-PostgreSQL traffic restricted to the same shard;
- an internal pooler HTTP Service plus fail-closed readiness and independent
  liveness probe contracts; the control Service retains unready endpoints for
  outage diagnostics while application Services continue filtering them; and
- controller ownership, update pruning, and finalizer-based deletion pruning.

Planned supporting resources use server-side apply. Each generated PostgreSQL
credential and standalone data PVC has a cryptographically random name. The
name and API-assigned UID are checkpointed separately in status before any
workload can reference that child. After the credential UID is checkpointed,
the Secret is detached from cluster garbage collection and that transition is
checkpointed before the first PVC create. Every PVC create is owned by that
exact detached Secret UID. Once the PVC UID is checkpointed, the controller
adds its data-protection finalizer, detaches that exact live PVC, and anchors
the Secret tombstone to the PVC. Workloads are published only after that
ownership inversion is complete. Deleting the Secret therefore cannot cascade
to current data, while deleting the protected PVC cascades to the credential
tombstone and reserves the claim name until every mounting workload has been
pruned. Original timed-out PVC creates carry the tombstone owner but never the
data-protection finalizer, so deleting the tombstone garbage-collects any such
create that arrives late. During Retain finalization, an authoritative absent
read before the PVC UID checkpoint is recorded as an abandoned creation intent.
The controller never manufactures replacement storage for that state and keeps
any later outcome on the Secret owner fence for deletion. A missing or replaced
UID-checkpointed child requires explicit recovery. A one-time upgrade path
first aligns objects created by the earlier whole-object Update controller, preserving
Service allocations and API defaults, then establishes the operator's Apply
field set and removes the legacy Update co-owners. The completion annotation is
written only by the final Apply, so a crash at any intermediate boundary safely
repeats migration. Every alignment attempt uses an uncached read and stops the
legacy Update path only when both its operator-owned durable marker and
matching Apply ownership have appeared. Apply ownership without the marker can
still coexist with create-time Update ownership that retains omitted stale
fields. Because
legacy whole-object ownership cannot identify fields
added by another Apply manager, such an object fails migration without a write;
that manager must relinquish its top-level field set before reconciliation can
continue. The operator recognizes its own earlier `pgshard-hpa-scale` manager
only on the pooler Deployment. HPA mode rewrites that release's possible
whole-Deployment field set to `spec.replicas` alone. HPA-to-fixed transitions
delete an authoritatively observed owned HPA and stop that reconciliation pass;
only a later uncached absence observation permits fixed replica ownership.
Fixed mode verifies the configured capacity and operator replica ownership on
every authoritative read, resource-version-fenced reapplies them after a late
scale write, then relinquishes the old HPA field set entirely. HPA scale
handoff rereads the Deployment through the
uncached API, checks its UID and resource version, retries concurrent updates
within a fixed bound, and transfers only `spec.replicas` to a dedicated field
manager.

This is not yet a working sharded database endpoint. An explicit
`membersPerShard: 1`, `durability: Asynchronous` resource creates one direct
PostgreSQL 18 primary per shard. The operator derives its configuration from
the resource budget, listens on the internal shard Service, runs as UID/GID
999 under the restricted Pod Security profile, and retains its data claim
across StatefulSet restarts. The readiness probe uses `pg_isready`. PostgreSQL
has no kubelet startup or liveness kill probe: PID 1 exit is handled by the
container runtime, while slow startup or crash recovery remains unready without
being killed. Storage is at least 4Gi per shard, `max_wal_size` cannot exceed
one quarter of that claim, and topology, durability, storage class, and size
are immutable until their explicit transition workflows exist. A resource
stored by an earlier release may still specify between 1Gi and 4Gi. Because
those releases created no PostgreSQL data claim, it has one safe migration:
increase the value directly to at least 4Gi before this controller will plan
new children. No later resize is accepted.
`PostgreSQLPrimariesAvailable=True` means only that all of those single-member
primaries have passed the StatefulSet's minimum-ready window. It does not claim standby
replication, failover, routing, or zero-downtime restart. Three- and five-member
resources continue to create no PostgreSQL Pods until bootstrap, replication,
fencing integration, promotion, and recovery exist.
PostgreSQL StatefulSets use `OnDelete` updates. A controller image, bootstrap
image, script, or generated-configuration change therefore updates the desired
template without concurrently deleting every singleton primary. Until a
readiness-gated shard-at-a-time upgrade controller exists, an operator must
delete one selected Pod at a time and accept that shard's documented outage.
This also applies to a singleton StatefulSet created by an earlier operator:
publishing the catalog-aware init container does not run it against the
existing Pod until that Pod is explicitly replaced.

Generated bootstrap credentials are unique per shard, immutable, and stable
across reconciles. Each data PVC is likewise bound to its recorded name, UID,
capacity, storage class, and creation-time deletion policy. When the spec omits
a class, the operator authoritatively selects the same default StorageClass
Kubernetes would choose and checkpoints that exact class before it dispatches
the PVC create. An explicit empty class is preserved for static provisioning;
every later class change remains fenced. Child names and the resolved storage
class are checkpointed before either API create; API-assigned UIDs are
checkpointed before a workload can consume them. The controller periodically
revalidates both identities and the live PVC's protection finalizer, and fails
closed instead of silently replacing a credential or empty data volume.
PostgreSQL initializes in a disposable staging
directory and atomically renames only a complete cluster into the final data
path. Its durable marker records the exact PgShardCluster UID and shard, so an
interrupted `initdb` cannot publish a partial `PG_VERSION` and a reused volume
cannot silently start for another cluster or shard. Initial publication flushes
the changed marker, access configuration, and staging, parent, and volume-root
directory entries. The validated restart path repeats that complete publication
barrier before PostgreSQL starts, so interruption after the atomic rename cannot
skip it on the next init pass. These flushes are limited to the cluster's data
path; bootstrap never issues a node-wide filesystem sync that could couple Pod
startup or termination to unrelated mounts.
Before reading or creating PGDATA, the init process requires its `postgres`
binary to report the operator's exact supported major, currently 18. It also
checks an existing `PG_VERSION` and the staging directory produced by `initdb`.
An immutable image digest identifies bytes; it is not accepted as evidence that
those bytes contain PostgreSQL 18.
On shard-0000 only, the same init boundary starts PostgreSQL with network
listening disabled and a private mode-0700 Unix socket. It loads the same
resource-derived primary configuration as the main server so durable logical
slots and prepared transactions remain startable during the init pass. Before
applying SQL, it creates the UTF8 `shardschema` database when absent only when
PGDATA carries the exact durable catalog-genesis intent written before the
fresh data directory was atomically published. That intent binds the cluster
UID, configured shard count, and migration digest. A fresh install requires the
reserved `pgshard_catalog` schema to be absent or empty.
An upgrade requires a complete supported object, sequence, column, constraint,
index, type, routine-signature, rewrite-rule, user-trigger, internal-FK-trigger,
and policy fingerprint: released v0.49, current-after-v0.49-upgrade, or
current-fresh-install. Fingerprint rendering pins the session search path and
identifier-quoting mode. A partial or structurally altered catalog is rejected
because `IF NOT EXISTS` DDL cannot repair it. For a supported pre-existing
catalog it checks home-shard identity, canonical shard state, permanent restore
history, and an exact immutable-CR shard inventory before migration. Missing or
extra active shards are a configuration conflict and are never repaired by
bootstrap. Only a catalog carrying that exact genesis intent receives the
complete canonical shard inventory, with one active restore incarnation per
shard, in a transaction that checks its own postcondition. The intent remains
durable across a crash after migration or during inventory. Retry accepts only
the migration-created shard-0000 row or the complete atomic inventory; any
other partial subset is a permanent topology conflict. The intent is removed
only after the final catalog and credential checks, serving-HBA publication,
and a clean temporary PostgreSQL stop with synchronous commit forced on. An absent
or empty catalog on any other pre-existing PGDATA is rejected rather than
inferred from the requested shard count. Every bootstrap SQL client has a five-second lock
timeout, a 30-second statement timeout, and a 120-second whole-transaction
timeout, so catalog locks fail the init pass for retry instead of hanging every
restart. The primary starts only after the applicable migration and
fresh-inventory transaction succeed. An
occupied reserved schema, unsupported catalog shape, conflicting identity or
lineage, malformed inventory, failed migration, or failed inventory transaction
keeps the primary unready. A released v0.49 catalog with an already exact shard
inventory and an interrupted genesis carrying its exact intent are supported
retry inputs.
Before its first seed statement, the migration drops every validated user
trigger so a replaceable pre-existing trigger-function body cannot execute as
the bootstrap principal; it recreates the canonical functions and triggers in
the same transaction. Fresh inventory is checked before that transaction can
commit, so a suppressed insert cannot report success. Reapplying an unchanged
migration and exact inventory does not advance the catalog epoch.
Shard-0000 bootstrap also creates one fixed `pgshard_pooler_catalog` login with
only the catalog reader role, proves that the immutable Secret password matches
its SCRAM verifier, and replaces restored or edited `pg_hba.conf` rules with an
operator-owned order that rejects plaintext catalog access and every other use
of that login. The verifier is generated in the bootstrap image from bounded
stdin and installed as an extended-protocol bind value; neither the plaintext
password nor verifier appears in PostgreSQL query text. Bootstrap sessions also
force and verify `synchronous_commit=on`, `zero_damaged_pages=off`, and
`ignore_checksum_failure=off` above restored database and role defaults before
the first catalog scan. The running shard-0000 PostgreSQL process serves the ready-only
catalog Service over TLS 1.3. Poolers receive only the password and CA
certificate; PostgreSQL receives only the serving certificate and key; the
bootstrap init container temporarily receives both retained projections so it
can verify their checkpointed SHA-256 values before touching PGDATA. The CA
private key is discarded after issuance and is never stored in Kubernetes.
Application Services still target the rejection-only pooler and must not be
treated as usable endpoints. `Ready=False` with reason `DataPlaneUnavailable`
for the single-member slice, or `PostgreSQLHAUnavailable` for an HA topology,
remains authoritative. Backup execution and ServiceMonitor reconciliation also
remain unimplemented. The ingress NetworkPolicies allow only selected
same-cluster Pods. The pooler-to-`shardschema` path is TLS 1.3 plus SCRAM with
required channel binding, but etcd client/peer and general PostgreSQL shard
traffic still lack authenticated TLS; the independent
`TransportSecurityReady=False` condition reports the remaining gap. Etcd uses independent 2Gi PVCs on `storage.storageClassName` with
a bounded backend quota. Its default image is digest-pinned and the Pod command
selects that image contract's `/usr/local/bin/etcd` executable explicitly;
custom `--etcd-image` values must provide the same path. Scale transitions
retain those claims during scaling. On cluster deletion, both storage policies
keep each live PostgreSQL PVC ownerless and independently protected, with its
API-identified credential tombstone anchored back to that exact PVC. The
finalizer first prunes every mounting controller, then resolves each possible
PVC-create outcome while the credential tombstone still exists. A visible
outcome is validated against the provisioned snapshot and its API-assigned UID
is checkpointed. `Retain` (the default) makes that exact PVC ownerless and keeps
it protected during the remaining barriers. If no outcome is visible before
the UID checkpoint, status records that creation intent as abandoned; no PVC is
created during finalization, and any later outcome remains bound to the Secret
tombstone for deletion.

After all storage outcomes are closed, the controller deletes and observes
authoritative absence of every exact credential tombstone. It then lists Pods
through the uncached API reader and proves absence of every Pod that mounts a
checkpointed data PVC. Each managed PostgreSQL Pod starts with a
cluster-UID-bound termination finalizer. In a namespace labelled
`pgshard.io/pod-fencing=enabled`, the controller first completes a challenge
and authenticated receipt update through a webhook carrying the same namespace
selector as Pod binding. The receipt is bound to the exact cluster UID and a
fresh random challenge; matching caller-supplied annotation text is not an
acknowledgement. Admission then makes the enabled label immutable for the
lifetime of the namespace. The fail-closed binding mutator copies the exact
managed identity plus the selected Node UID and boot ID into the same API update
that assigns `spec.nodeName`. Both the mutator and final validator use the
uncached API reader to require the annotation's exact PgShardCluster UID to
still exist and not be deleting; the validator also rejects any later mutation
of the bound evidence. The PostgreSQL init container consumes those Node
annotations through the Downward API and refuses to touch PGDATA when either is
absent. This independent startup gate is the final data-path barrier if another
API server has not yet observed the namespace selector; the cluster handshake
alone is not a proof that every API-server admission cache has converged.

Status mutation rejects removal of managed identity, binding identity, or the
termination finalizer, and permits an authenticated terminal status update to
add the durable `pgshard.io/PostgreSQLProcessTerminated` condition only when the
request is from `system:node:<spec.nodeName>` in `system:nodes` and the live Node
still has that binding-time UID and boot ID. A final validating status webhook
then verifies the post-mutation object. The condition carries an HMAC receipt
bound to the exact Pod UID, generation, terminal phase, and binding-time Node
incarnation. PodGC, another status writer, or copied condition text cannot
create it. Validating admission also makes the managed Pod spec and generation
immutable across ordinary, ephemeral-container, and in-place-resize updates, so
later mutation cannot invalidate or outgrow the terminal receipt. The controller cryptographically verifies the receipt before
releasing the finalizer, or permits a deleting Pod that was never assigned;
Kubernetes serializes binding against deletion for the latter case. A webhook
outage, missing Node, reboot, or same-name replacement therefore leaves the Pod
and PVC fenced. Credential-only
clients do not own PGDATA and cannot keep a session after the PostgreSQL process
has stopped, so they do not block this storage barrier. A Pod committed before
credential deletion remains visible to the PVC barrier. A Pod committed after
the controller's final list cannot bind once the cluster is deleting, even if a
kubelet still caches the immutable bootstrap Secret. Only after the credential,
authenticated-process, and Pod-absence barriers does `Retain`
release its own PVC protection finalizer and mark the data retained. If a
retained PVC was explicitly deleted, the controller releases only its own
protection finalizer and waits for authoritative absence instead of replacing
it. `Delete` requests
deletion only for status-recorded PVC UIDs and releases the protection
finalizer after deletion is accepted. A same-name claim cannot reach bootstrap
while a workload exists. The CR finalizer
uses the checkpointed creation-time policy and waits for the selected result to
be observed through the uncached Kubernetes API reader. Finalization never
creates replacement storage; every visible uncheckpointed outcome is validated
against the checkpointed storage snapshot before it can be retained or deleted.
`Retain` does not
override an explicit PVC deletion and cannot preserve a namespaced PVC when its
namespace is deleted. Automated
defragmentation is not implemented. PostgreSQL
`archive_mode` remains off until a real archival pipeline is reconciled and
verified, so the generated configuration cannot silently fill `pg_wal`.

This lifecycle receipt is not physical node fencing. Do not delete and recreate
a Node under the same name while a bound pgshard Pod may still exist on the old
machine. If the bound Node disappears or its boot ID changes, fence the machine
or storage externally and use an explicit recovery procedure; the MVP will not
infer safety from Pod phase, `ReadWriteOnce`, a replacement Node, or an
administrator-added condition. Removing the Pod finalizer or the admission
configuration is outside the safe lifecycle contract.

NetworkPolicy selectors are traffic controls, not workload authentication. A
principal allowed to create Pods in a cluster namespace can forge the selected
labels, so each PgShardCluster namespace is currently a trusted administrative
boundary. Mutually untrusted tenants require namespace-scoped operator installs
and an admission-reserved workload identity that are not implemented yet.

The PostgreSQL ConfigMap sizes every member for the largest slot footprint it
can carry during promotion: primary physical slots and failover anchors,
standby-synchronized anchor copies and independent standby-local decoding
slots, plus the physical slots a promoted decoder must create before old local
slots can be retired. Each member's primary profile excludes itself, names the
other members' managed physical slots, and selects `ANY 1` for synchronous
durability. Each standby profile fixes that member's own `primary_slot_name` and
requires `hot_standby_feedback = on`, a positive
one-second feedback interval, and `sync_replication_slots = on`. These are
configuration plans, not evidence that replication is running. Authenticated
orchestration must still write a `primary_conninfo` containing a valid database
name and the profile's exact `application_name`, create the physical slots,
observe feedback health, activate exactly one role profile, and remove unhealthy
candidates from the synchronized set before logical consumers or clean primary
shutdown are allowed.

Rejoining a former primary is a separate fenced transition, not a direct switch
to its standby profile. PostgreSQL 18 refuses slot synchronization when a
same-named local slot exists but is not already a synchronized copy. The
orchestrator must stop slot users, durably transfer and verify every managed
consumer checkpoint, classify slots by their catalog ownership, and remove
obsolete primary-owned anchors and physical slots before it enables
`sync_replication_slots`. An orderly switchover performs that cleanup while the
old primary is fenced and before its role changes. After an unplanned failover,
the old member is never restarted writable merely to clean slots; it is
reinitialized from the new primary and verified free of stale slot state. An
unknown or user-owned collision requires operator intervention and is never
deleted automatically. Only a collision-free member can regain decoder or
promotion eligibility.

The default orchestrator and pooler image values are expected development
channel names, not a publication guarantee. The Rust pooler has a control-only
executable that composes catalog state with its HTTP endpoints and a
rejection-only PostgreSQL read-write handshake listener. It accepts no SQL
session, has no connection pool, and deliberately remains application-unready
even when its catalog is usable. For a supported single-member topology, the
operator first checkpoints a non-consumable creation intent containing an
unpredictable permanent Secret name. It creates only an empty mutable Secret at
that name, checkpoints the API-assigned UID, then conditionally updates that
exact resource once with immutable credential and TLS material. Separate client
and server digests are checkpointed after the material update. A lost Create
response is resolved by the permanent name while the Secret is still empty.
Each lost status-checkpoint response is resolved by rereading durable status.
A lost material-Update response is resolved by rereading the exact UID and
validating the resulting immutable material; the original update remains
conditional on the previously observed resource version. The operator selects
`operator-tls` only after status holds the exact UID and both material digests.
The connector constructs a fixed catalog connection,
requires TLS 1.3 hostname validation, SCRAM channel binding, and a writable
primary, and never accepts a remote DSN. Missing or replaced Secret identity,
unexpected keys, unsafe role state, wrong credentials, wrong certificates, or
insufficient certificate lifetime fail closed. Cluster finalization deletes the
exact intent-recorded Secret with UID and resource-version preconditions, then
observes its absence before releasing the cluster finalizer. Unsupported HA topologies still
select credential-free `bootstrap-unavailable` and create no PostgreSQL data
plane. Override the defaults with `--orchestrator-image`,
`--pooler-image`, `--etcd-image`, and `--postgresql-image` when concrete images
exist. The privileged `--postgresql-bootstrap-image` deliberately has no remote
default and is required for the single-member slice. Non-development values
must carry an immutable `@sha256:` digest. The exact local
`pgshard/postgres-agent:dev` exception uses `imagePullPolicy: Never` and must be
loaded into every node before creating a cluster. The complete non-development
reference is parsed using the container-distribution grammar before durable
bootstrap state is created; a digest-shaped suffix cannot make an invalid image
name safe. Image pull or runtime readiness is reported by the relevant observed
workload condition, never inferred from planned objects. A custom PostgreSQL
image must preserve the pinned official image contract: PostgreSQL 18,
UID/GID 999, `initdb` and `bash` on `PATH`, compatibility with the Docker
entrypoint for the main process, and the `/var/lib/postgresql/18/docker` data
layout.
The separate bootstrap image must provide the same PostgreSQL 18 command-line
tools as UID/GID 999 plus the source migration at
`/usr/share/pgshard/migrations/0001_shardschema.sql`; it is never used as the
main database process and receives the superuser Secret only while the init
container runs. The operator verifies the binary's exact major and compares the
migration bytes with its release-coupled SHA-256 value before touching PGDATA.
On every managed PGDATA it rejects active `postgresql.auto.conf` settings,
recovery signals, external WAL, and tablespaces before the main server starts.
Catalog bootstrap then uses a private quarantined postmaster. A pre-existing
catalog must match the configured shard inventory exactly; missing shards are
seeded only while the exact durable first-boot genesis intent remains. An
absent or empty catalog without that intent is rejected. The running primary receives
`allow_alter_system=off`, so authoritative operator configuration cannot be
bypassed by a persistent `ALTER SYSTEM` override.

The catalog serving certificate is deliberately static in this development
slice. It is valid for five years and is rejected once less than 180 days
remain. PostgreSQL cannot consume a rotated Secret directly with the required
private-key ownership and mode, and no writable-file rotation sidecar plus
reload controller exists yet. Zero-downtime certificate rotation is therefore
not implemented. Near expiry marks the cluster degraded and stops the entire
resource plan/apply reconciliation before it can silently rotate or publish
new workloads; already-running workloads are not stopped. Use an explicit
recovery procedure rather than expecting silent regeneration.

`PgShardRestore` is currently a mutation-free preflight. Its v1 manifest is a
versioned binary signed projection containing the source database, backup-set identity,
PostgreSQL major, routing hash version and seed, shard count, ordered ordinals,
and complete half-open key ranges. The verification key must be the only value
in an immutable Opaque Secret and is bound into status by API UID. An omitted
destination topology proves nothing; an explicit caller expectation must
match. CRD validation rejects five-to-three, and the fail-closed validating
webhook rejects same-count boundary drift with `RestoreTopologyMismatch`, while
the controller independently verifies every boundary after signature
validation. Because the controller does not yet load
authoritative destination catalog state, a valid request stays
`Pending/Ready=False` with `DestinationTopologyResolverUnavailable`: no
pgBackRest restore, PVC, Job, catalog mutation, or serving endpoint is created
by this slice.

The module is pinned to Go 1.26.5, controller-runtime 0.24.1, and Kubernetes
libraries 0.36.0. Only the Linux container deployment is supported.

## Certificate-free development manager

`config/development` installs the CRD, a least-privilege manager identity, and
the real operator Deployment from local `pgshard/*:dev` images. The manager
runs as a numeric non-root user with a read-only root filesystem, namespace-
scoped leader-election Lease access, bounded probes, zero-unavailable rollouts,
and no metrics listener.
The command defaults to admission webhooks enabled, but this focused debugging
overlay passes `--webhook-enabled=false` and deliberately omits the generated
webhook configurations. Use `config/admission` to exercise self-managed
certificates and admission. OpenAPI validation still applies here, and the
reconciler repeats all semantic safety validation before creating children,
but this is not a production admission setup and must not manage direct
PostgreSQL Pods.

After building and loading the operator, orchestrator, and pooler `:dev` images
into a local KIND cluster:

```console
kubectl apply -k operator/config/development
kubectl rollout status --namespace pgshard-system deployment/pgshard-controller-manager
kubectl create namespace pgshard-development
kubectl apply --namespace pgshard-development -f operator/config/samples/pgshard_v1alpha1_development.yaml
kubectl get --namespace pgshard-development pgshardcluster development
```

The sample proves only real manager reconciliation and fail-closed supporting
processes. Each orchestrator Pod becomes ready only while it owns a unique,
cluster-UID-bound etcd lease; the pooler remains unready, application Services
have no ready endpoints, no PostgreSQL workload is created, and the cluster
reports `Ready=False` with `PostgreSQLHAUnavailable`. Orchestrator readiness is
process-incarnation evidence only: durable operation records, shard-term
authority, and automated failover are not implemented. The named backup PVC is
only validated configuration; no backup job or repository is created.

For direct PostgreSQL lifecycle development, first install `config/admission`,
label the resource namespace `pgshard.io/pod-fencing=enabled`, and then apply
the separate `pgshard_v1alpha1_single_member.yaml` sample. It creates two
independent single-member primaries and retained PVCs. Query them through their internal
`<cluster>-shard-0000` and `<cluster>-shard-0001` Services or by executing
`psql` in their Pods; the `<cluster>-rw`, `-ro`, and `-r` Services are not yet
usable. Restarting a primary preserves its PVC data but interrupts that shard,
so this sample must not be used as zero-downtime evidence.

## Self-managed admission manager

`config/admission` extends the same local-image install with four generated
mutating webhooks and six generated validating webhooks. A manager Pod created
by this overlay has a 20-second total termination budget. Its `preStop` hook
uses the first five seconds as an endpoint-propagation drain before `SIGTERM`,
leaving roughly 15 seconds for graceful shutdown. The webhook Service uses the
dedicated `9444` contract port and selects only Pods labelled for the
`receipt-v1` admission contract. Changing the client port prevents the API
server from reusing a cached connection to the last keyless manager, while the
selector prevents new connections from reaching it. Together they make that
one-time contract upgrade fail closed until a new manager is ready. Later
`receipt-v1`-compatible rollouts retain an old compatible endpoint through the
surge and drain it before termination. The binding mutator, binding final-state
validator, and cluster-handshake webhook
are scoped to namespaces labelled `pgshard.io/pod-fencing=enabled`; the status
and metadata webhooks are scoped to
operator-managed PostgreSQL Pods. Both binding and status use final validating
webhooks so a later mutator cannot remove or replace their authenticated
evidence. Namespace admission makes the opt-in label
sticky across ordinary, status, and finalize updates; delete the namespace to
retire that admission boundary. It pre-creates empty,
operator-labeled Secrets and grants the manager exact-name `get` and `update`
access only in `pgshard-system` for webhook certificate and fencing-key
initialization. The
reconciler also has cluster-wide Secret `get`, `create`, `update`, and `delete`
because it generates one randomly named credential per shard in each resource
namespace, inverts its owner after the data UID checkpoint, and removes the
credential tombstone only after the storage outcome is resolved or durably
abandoned;
Kubernetes RBAC cannot restrict that permission to names derived from arbitrary
custom resources. It has no Secret list, watch, or patch permission, and the
controller reads credentials through the uncached client
rather than placing all Secret data in its informer cache. This remains a
documented multi-tenant trust boundary: a compromised cluster-scoped manager
could read, create, change metadata on, or delete unrelated Secrets, so a
future namespace-scoped install mode is required for mutually untrusted
tenants. Finalization also requires cluster-wide Pod `list` and `delete`
because Kubernetes RBAC cannot restrict either permission to Pods that
reference controller-generated resource names. The reconciler uses an
authoritative namespace list, acts only on a Pod mounting an exact checkpointed
shard PVC, validates the expected StatefulSet identity, labels, and controller
reference before deletion, and fails closed on a collision. Admission needs
Pod `get` to inspect a binding, Node `get` to bind and revalidate the selected
Node UID and boot ID, and Pod `patch` to remove only an authenticated termination
finalizer. It has no Pod `watch`, `create`, or `update` permission, and no Node
permission other than `get`. A separate
ClusterRole permits `get` and `patch` only on pgshard's two exact webhook-
configuration names. Kubernetes RBAC cannot restrict a patch to individual
fields, so the provisioner validates the full Service target and existing
trust state before changing only CA bundles.

Before the webhook listener starts, each manager Pod creates an ECDSA P-256 CA,
a TLS 1.3 serving key pair, and a separate random 256-bit fencing key in those
Secrets, validates the Service references, injects the CA bundle, and writes the
serving files into a private memory-backed `emptyDir`. The fencing key Secret is
made immutable by the same update that initializes it. A SHA-256 continuity
fingerprint annotation is stored separately on the CA Secret, followed by a
completion annotation on the key Secret. These metadata-only additions preserve
the data shapes accepted by the previous manager. This is Secret-format
compatibility only; it does not make the new manifest and image contract safe to
roll back. Fresh-install key generation requires empty CA, serving, and key Secrets,
empty webhook trust bundles, and no PostgreSQL lifecycle or pending cluster
handshake metadata; the CA records authorization before key bytes are
generated. Recreating both authority Secrets around an existing install
therefore fails closed.

The last keyless admission release upgrades through a separate two-phase path.
The new manifest creates an empty key Secret carrying a versioned request. The
manager verifies the already initialized CA and serving material, requires the
two legacy PgShardCluster webhooks to trust that CA, and requires every newly
introduced receipt webhook trust bundle to remain empty. It then records
pending upgrade authorization on the CA Secret before generating and anchoring
the first key. That proof also establishes that receipt-looking annotations
stored by the keyless release were never authenticated, so they are not treated
as continuity history. An install that already has an initialized but
unanchored key is not this keyless state and is never adopted during a
mixed-version rollout: pin its fingerprint in the CA Secret while the old
manager is healthy, before changing the manager image, or reinstall the
pre-release development cluster.
Once pre-anchored, every handshake on a single-member cluster is inspected and
every established cluster handshake and managed terminal Pod receipt must
verify before the completion marker is written. Invalid or incomplete cluster
metadata remains repairable only until the controller has published its
lifecycle finalizer or PostgreSQL bootstrap status; no PostgreSQL storage or
workload can precede that barrier. Users cannot establish,
remove, or replace cluster handshake metadata. The controller may establish or
repair it only when its service-account identity and the final HMAC receipt both
verify; during deletion the pair is byte-for-byte immutable. Once complete,
loss of the fingerprint fails closed instead of re-entering adoption. Startup, readiness,
receipt-authenticated admission paths, and controller reconciliation all require
the exact immutable key to match that anchor, so an empty, mutable, oversized,
or different replacement cannot silently invalidate outstanding receipts.
The key authenticates cluster-handshake and Pod-termination receipts
independently of certificate renewal or CA encoding; its bytes are never stored
in resource annotations.
The 90-day serving certificate is checked hourly and renewed 30
days before expiry. Controller-runtime reloads a renewed key pair without a Pod
restart, and readiness fails if the local certificate becomes untrusted,
incorrectly named, or expires, or if the durable fencing-key Secret is missing
or malformed. Existing non-empty malformed Secrets,
foreign CA bundles, and incorrectly targeted webhook configurations stop
startup instead of being overwritten.

The keyless release on the default branch needs no manual key command, but its
webhook contract is not availability-compatible with `receipt-v1`. Freeze
`PgShardCluster` writes and leave every resource namespace without the
`pgshard.io/pod-fencing=enabled` label while applying the updated manifest and
immutable manager image. The dedicated Service port gives the API server a new
webhook client identity, and the Service contract selector excludes the old
manager. Admission requests can therefore fail with a retryable
webhook-unavailable error until the new Pod is Ready. `maxUnavailable: 0`
preserves the old process; it cannot
make that process serve routes or validation it does not implement. Wait for the
manager rollout before retrying writes or enabling Pod fencing. Do not restart
the old image with the new command-line arguments. A zero-rejection upgrade
between incompatible webhook contracts requires staged compatibility tooling
that is not implemented. For a pre-release development install that already has
an initialized unanchored key, perform this explicit first phase while the old
manager is still healthy, then roll out the new manager image:

```console
KEY_UID="$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.metadata.uid}')"
KEY_SHA256="$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.data.hmac\.key}' | base64 -d | sha256sum | awk '{print $1}')"
kubectl --namespace pgshard-system annotate --overwrite secret pgshard-webhook-ca "pgshard.io/pod-fencing-key-sha256=${KEY_SHA256}"
test "$(kubectl --namespace pgshard-system get secret pgshard-webhook-fencing-key -o jsonpath='{.metadata.uid}')" = "${KEY_UID}"
```

If the UID check fails, do not continue the rollout. The new manager verifies
the pinned value and established receipts; it does not infer continuity from a
live receipt scan. Automated production upgrade tooling for this pre-release
boundary is not implemented.

Automatic CA rotation is not implemented. The generated CA is valid for about
ten years, and startup fails once it can no longer safely issue a full 90-day
leaf certificate. This explicit boundary avoids an unsafe one-step CA swap;
overlapping trust and rollout proof are required before automated CA rotation
is added. Automatic fencing-key rotation is also not implemented. Restore the
original Secret after accidental loss. Changing the key or its CA-Secret
continuity anchor is an explicit recovery operation that invalidates outstanding
cluster and termination receipts and is unsafe while any fenced lifecycle is
unresolved.

After loading the three local images, install the admission path with:

```console
kubectl apply -k operator/config/admission
kubectl rollout status --namespace pgshard-system deployment/pgshard-controller-manager
```

This remains a development/source-validation install. It proves fail-closed
admission and manager reconciliation and can run the explicit single-member
primary sample, but it does not provide a usable routed application endpoint.

Run the local checks from this directory:

```console
go test -race ./...
go vet ./...
go build ./...
go tool govulncheck ./...
```

Regenerate API objects and install manifests with controller-tools 0.21.0:

```console
go tool controller-gen object paths=./...
go tool controller-gen crd:allowDangerousTypes=false paths=./... output:crd:artifacts:config=config/crd/bases
go tool controller-gen rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
go tool controller-gen webhook paths=./... output:webhook:artifacts:config=config/webhook
```

To verify that all checked-in generated output is reproducible, run the four
generation commands above in a clean checkout, then run:

```console
git diff --exit-code -- api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac config/webhook
```

The repository `make go-check` target and the Go operator CI job run formatting,
module tidy/verification, `go vet ./...`, `go test -race ./...`,
`go build ./...`, `go tool govulncheck ./...`, and the generation-and-diff
sequence above. No helper shell scripts or `hack` directory are required.

CI creates separate digest-pinned Kubernetes 1.36 KIND clusters. One exercises
StatefulSet/PVC creation, supervised deletion, and same-name recreation against
real Kubernetes controllers. Another builds and loads local images, installs
the self-managed admission manager, proves semantic admission rejection and
certificate trust, preserves the fail-closed three-member boundary, and starts
two restricted single-member PostgreSQL 18 primaries. It exercises TCP through
a shard Service from an authorized restricted probe client, publishes a changed
PostgreSQL configuration without restarting either `OnDelete` Pod, deletes only
shard-0000, proves shard-0001 remains untouched, and verifies shard-0000's data,
catalog snapshot, prepared transaction, and logical slot survive recreation. It
also requires the pooler's operator-provisioned TLS 1.3 and SCRAM catalog
connection to publish a ready catalog in exact status and Prometheus metrics
while overall application readiness remains false.
The test does not claim uninterrupted traffic. These targeted tests are not yet
the full Milestone 1 KIND suite.
