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
workload can reference that child. The name is a durable creation intent, so a
reconcile after an outcome-unknown create reads and validates that exact child
before checkpointing its API-assigned UID. A missing or replaced UID-checkpointed
child requires explicit recovery. A one-time upgrade path
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
are immutable until their explicit transition workflows exist.
`PostgreSQLPrimariesAvailable=True` means only that all of those single-member
primaries have passed the StatefulSet's minimum-ready window. It does not claim standby
replication, failover, routing, or zero-downtime restart. Three- and five-member
resources continue to create no PostgreSQL Pods until bootstrap, replication,
fencing integration, promotion, and recovery exist.

Generated bootstrap credentials are unique per shard, immutable, and stable
across reconciles. Each data PVC is likewise bound to its recorded name, UID,
capacity, API-defaulted storage class, and creation-time deletion policy. A
same-UID nil-to-default StorageClass assignment made later by Kubernetes is
checkpointed once only after the exact referenced StorageClass is read from the
API and verified as default; an explicit empty class, a non-default class, or any
later class change remains fenced. Child names are checkpointed before either API create; API-assigned UIDs are
checkpointed before a workload can consume them. The controller periodically
revalidates both identities and fails closed instead of silently replacing a
credential or empty data volume. PostgreSQL initializes in a disposable staging
directory and atomically renames only a complete, marked cluster into the final
data path, so an interrupted `initdb` cannot publish a partial `PG_VERSION`.
Application Services still target the rejection-only pooler and must not be
treated as usable endpoints. `Ready=False` with reason `DataPlaneUnavailable`
for the single-member slice, or `PostgreSQLHAUnavailable` for an HA topology,
remains authoritative. Backup execution and ServiceMonitor reconciliation also
remain unimplemented. The ingress NetworkPolicies allow only selected
same-cluster Pods, but etcd client/peer and PostgreSQL shard traffic still lack
authenticated TLS; the independent `TransportSecurityReady=False` condition
reports that gap. Etcd uses independent 2Gi PVCs on `storage.storageClassName` with
a bounded backend quota. Its default image is digest-pinned and the Pod command
selects that image contract's `/usr/local/bin/etcd` executable explicitly;
custom `--etcd-image` values must provide the same path. Scale transitions
retain those claims during scaling. On cluster deletion,
`storage.deletionPolicy: Retain` (the default) leaves PostgreSQL data PVCs
ownerless from creation, then marks the exact status-recorded UIDs as retained
before other owned resources are removed. `Delete` must be selected explicitly
at creation to prune the workloads first and then delete only those exact
status-recorded UIDs, allowing pvc-protection to complete without deadlocking
the finalizer. The CR finalizer
uses the checkpointed creation-time policy and waits for the selected result to
be observed through the uncached Kubernetes API reader. `Retain` does not
override an explicit PVC deletion and cannot preserve a namespaced PVC when its
namespace is deleted. Automated
defragmentation is not implemented. PostgreSQL
`archive_mode` remains off until a real archival pipeline is reconciled and
verified, so the generated configuration cannot silently fill `pg_wal`.

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
even when its catalog is usable. Its catalog connector is
deliberately local-only until authenticated TLS exists, while this operator
does not yet provision a catalog DSN Secret or a compatible local shardschema
endpoint. The operator therefore selects the pooler's explicit
`bootstrap-unavailable` mode: the process exposes liveness and bounded status
without a credential or connection attempt, while catalog and application
readiness fail closed. Override the defaults with `--orchestrator-image`,
`--pooler-image`, `--etcd-image`, and `--postgresql-image` when concrete images
exist. Image pull or runtime readiness is reported by the relevant observed
workload condition, never inferred from planned objects. A custom PostgreSQL
image must preserve the pinned official image contract: PostgreSQL 18,
UID/GID 999, `initdb` and `bash` on `PATH`, compatibility with the Docker
entrypoint for the main process, and the `/var/lib/postgresql/18/docker` data
layout.

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
but this is not a production admission setup.

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
processes. Its pooler and orchestrator Pods run but remain unready, application
Services have no ready endpoints, no PostgreSQL workload is created, and the
cluster reports `Ready=False` with `PostgreSQLHAUnavailable`. The named
backup PVC is only validated configuration; no backup job or repository is
created.

For direct PostgreSQL lifecycle development, apply the separate
`pgshard_v1alpha1_single_member.yaml` sample. It creates two independent
single-member primaries and retained PVCs. Query them through their internal
`<cluster>-shard-0000` and `<cluster>-shard-0001` Services or by executing
`psql` in their Pods; the `<cluster>-rw`, `-ro`, and `-r` Services are not yet
usable. Restarting a primary preserves its PVC data but interrupts that shard,
so this sample must not be used as zero-downtime evidence.

## Self-managed admission manager

`config/admission` extends the same local-image install with the generated
mutating and validating webhook configurations. It pre-creates empty,
operator-labeled Secrets and grants the manager exact-name `get` and `update`
access only in `pgshard-system` for webhook certificate mutation. The
reconciler also has cluster-wide Secret `get` and `create` because it generates
one randomly named credential per shard in each resource namespace;
Kubernetes RBAC cannot restrict that permission to names derived from arbitrary
custom resources. It has no Secret list, watch, update, patch, or delete
permission, and the controller reads credentials through the uncached client
rather than placing all Secret data in its informer cache. This remains a
documented multi-tenant trust boundary: a compromised cluster-scoped manager
could read or create unrelated Secrets, so a future namespace-scoped install
mode is required for mutually untrusted tenants. A separate
ClusterRole permits `get` and `patch` only on pgshard's two exact webhook-
configuration names. Kubernetes RBAC cannot restrict a patch to individual
fields, so the provisioner validates the full Service target and existing
trust state before changing only CA bundles.

Before the webhook listener starts, each manager Pod creates an ECDSA P-256 CA
and TLS 1.3 serving key pair in those Secrets, validates the Service references,
injects the CA bundle, and writes the serving files into a private memory-backed
`emptyDir`. The 90-day serving certificate is checked hourly and renewed 30
days before expiry. Controller-runtime reloads a renewed key pair without a Pod
restart, and readiness fails if the local certificate becomes untrusted,
incorrectly named, or expires. Existing non-empty malformed Secrets,
foreign CA bundles, and incorrectly targeted webhook configurations stop
startup instead of being overwritten.

Automatic CA rotation is not implemented. The generated CA is valid for about
ten years, and startup fails once it can no longer safely issue a full 90-day
leaf certificate. This explicit boundary avoids an unsafe one-step CA swap;
overlapping trust and rollout proof are required before automated CA rotation
is added.

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
a shard Service from an authorized restricted probe client, restarts a primary
StatefulSet, and verifies its data survives.
The test does not claim uninterrupted traffic. These targeted tests are not yet
the full Milestone 1 KIND suite.
