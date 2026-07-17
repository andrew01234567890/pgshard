---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no usable sharded routing endpoint yet. The current operator can run a
limited PostgreSQL development slice: an explicit one-member
asynchronous resource creates one PostgreSQL 18 primary and retained PVC per
shard. Each shard receives a distinct immutable bootstrap Secret and a 4Gi
minimum data claim. Before the shard-0000 server is allowed to start, its init
container creates the dedicated `shardschema` database over a private Unix
socket, applies the transactional catalog migration, and records every
configured shard plus its initial restore incarnation. No other shard receives
that database. When `storage.storageClassName` is omitted, the operator
resolves the current Kubernetes default and checkpoints that exact class before
creating any claim; later default-class rotation does not change existing data
intent. An object stored by an earlier release with a 1Gi-to-4Gi size must be
updated once to at least 4Gi before new children are planned; those releases
created no PostgreSQL data claim, and later resizing remains unsupported.
Admission records the selected Node UID and boot ID atomically with Pod
binding and validates the final Binding after all mutators run. The PostgreSQL
init container refuses to touch PGDATA unless that evidence reaches the Pod.
Admission then attaches an HMAC-authenticated process-stop condition only to a
terminal status update from that exact authenticated kubelet, and validates the
final status after all mutators run. PodGC, copied annotation or condition text,
a missing or recreated Node, and a webhook outage cannot produce that receipt,
so cluster deletion fails closed with the PVC protected. This is lifecycle protection, not
HA takeover or physical node fencing. It has no standby, promotion, backup
execution, or connection pool. The pooler supervises `shardschema` over an
operator-provisioned TLS 1.3 and SCRAM connection and, while that catalog is
ready, the `-rw` Service relays raw PostgreSQL sessions to the singleton
shard-0000 writer. PostgreSQL performs the application SCRAM exchange end to
end. This path is plaintext, has no routing, and deliberately blocks the
`shardschema` database and replication connections. Restarting a primary interrupts that shard even though
its data survives. Three- and five-member resources continue to fail closed
without PostgreSQL Pods.

The source also includes the Go custom-resource API, fail-closed Rust
agent/orchestrator foundations, a bounded shard-zero compatibility relay, and local-only test
image builds. The single-member path is useful for operator and storage testing;
it is not a sharded database installation or zero-downtime evidence.

## Validate the current source

The supported development environment is Linux with Rust 1.97, Go 1.26,
Node.js 22 and Buf 1.71. From a checkout:

```console
make check
```

This validates contracts, core and runtime foundations, generated Kubernetes
resources, release policy and documentation. CI separately exercises the
local-only lifecycle image with a disposable PostgreSQL 18 data directory. The
operator KIND job also starts two single-member primaries, verifies the
shard-0000 catalog inventory and initial restore identities, proves the other
shard has no `shardschema` database, observes the pooler's authenticated catalog
connection through `/status` and `/metrics`, queries one primary directly and
through the read-write compatibility Service from an authorized restricted
probe client using shard-0000's credential, proves that Service blocks
`shardschema`, restarts shard-0000, and verifies both application persistence and an unchanged catalog
epoch and restore-incarnation mapping. The restart fixture also leaves one
prepared transaction and logical slot durable across the init pass. Neither
test proves a
sharding runtime. Follow
[implementation status](./project/status.md) for the first version with a real
cluster quickstart.

The catalog migration has a separate opt-in live contract test against a
disposable PostgreSQL 18 `shardschema` database. See the
[`pgshard-catalog` README](https://github.com/andrew01234567890/pgshard/tree/main/crates/pgshard-catalog)
for its preconditions. CI runs that test with an ephemeral service database;
the operator additionally applies the same bytes before a managed shard-0000
primary starts. Database creation is recoverable; the migration and shard
inventory update each run in their own transaction. PostgreSQL remains
unavailable until both succeed and the final catalog invariants pass. An empty
database left before migration is safe to retry only when the same PGDATA also
carries the exact cluster-bound catalog genesis intent; a released v0.49
catalog is safe to retry after its structural and topology checks pass.
A fresh install requires the reserved catalog schema to be absent or empty.
Upgrade accepts only the exact structural fingerprint of the released v0.49
catalog or a current catalog produced by a fresh install or v0.49 upgrade. The
fingerprint includes identity-sequence parameters and ownership, rewrite rules,
row-security metadata, and enabled internal foreign-key triggers under
canonical rendering settings. Bootstrap separately rejects database-wide event
triggers and unsafe effective identity-sequence progress because migration
cannot recreate arbitrary behavior, columns, or constraints lost from an
existing relation. An occupied reserved schema, partial or altered
catalog, conflicting home-shard or shard-state identity, a configured shard
count that does not exactly match a pre-existing catalog, or missing, orphaned,
or conflicting permanent restore lineage is rejected
before migration can rewrite it. Catalog clients use bounded lock, statement,
and whole-transaction timeouts so a conflicting prepared lock fails the init
pass for retry instead of hanging a Pod restart.

Before that private postmaster starts, bootstrap also rejects active settings in
restored `postgresql.auto.conf`, recovery signals, external WAL, and user
tablespaces. The temporary server pins safe callbacks, preload libraries,
durability settings, search path, trigger mode, and table access method; the
normal server disables `ALTER SYSTEM` so unsupported persistent overrides
cannot appear between restarts.

## Exercise the development manager

Build the local-only images with an archive-capable Buildx builder, load the
operator, orchestrator, pooler, and PostgreSQL bootstrap `:dev` images into a disposable KIND cluster,
then install the manager:

```console
PGSHARD_IMAGE_TARGETS="operator orchestrator pooler postgres-agent" make images
docker load --input artifacts/images/pgshard-operator.tar
docker load --input artifacts/images/pgshard-orchestrator.tar
docker load --input artifacts/images/pgshard-pooler.tar
docker load --input artifacts/images/pgshard-postgres-agent.tar
kind create cluster --name pgshard-development \
  --image kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  --wait 90s
kind load docker-image pgshard/operator:dev pgshard/orchestrator:dev \
  pgshard/pooler:dev pgshard/postgres-agent:dev --name pgshard-development
kubectl apply -k operator/config/admission
kubectl rollout status --namespace pgshard-system deployment/pgshard-controller-manager
kubectl create namespace pgshard-development
kubectl label namespace pgshard-development pgshard.io/pod-fencing=enabled
kubectl apply --namespace pgshard-development -f operator/config/samples/pgshard_v1alpha1_development.yaml
```

The local bootstrap tag is the only mutable reference accepted for this source
workflow and its Pod pull policy is `Never`; a missing node-local image fails
closed instead of consulting a registry. Non-development operator deployments
must configure `--postgresql-bootstrap-image` with an immutable SHA-256 digest.
The operator parses the complete digest-pinned reference before creating
bootstrap credentials or storage. At runtime the init process still verifies
that image's `postgres` binary and both existing and newly initialized PGDATA
report major 18 before publication.

The admission overlay provisions an ECDSA serving chain and a separate immutable
256-bit fencing key into exact-name operator-managed Secrets, injects the CA
bundle, and anchors the key's SHA-256 continuity fingerprint in the independent
CA Secret's metadata, preserving the Secret data shape accepted by the previous
manager. This is data-format compatibility, not a supported manifest or image
rollback. Fresh-install authority requires all three material
Secrets and webhook trust bundles to be empty and no established PostgreSQL
lifecycle. The last keyless release upgrades automatically only after a
versioned manifest request and independent proof of its existing CA, serving
material, and legacy webhook trust are recorded in a first phase. An existing
initialized key without an anchor is refused and must be pinned while the old
manager is healthy before the new image is rolled out. Established receipt
history is then verified before a separate completion marker is written. See
[High availability](operations/high-availability.md) for the pre-anchor
development upgrade command. That one-time keyless-to-`receipt-v1` transition
is fail closed rather than zero rejection: freeze cluster writes, leave Pod
fencing disabled in resource namespaces, and wait for the new manager rollout
before retrying admission requests. Later compatible manager Pods drain stale
Service endpoints for five seconds within a 20-second total termination budget.
Startup, readiness,
receipt-authenticated admission, and reconciliation reject an empty, mutable,
incorrectly sized, or different replacement key. The leaf certificate renews without a Pod restart;
automatic CA or fencing-key rotation is not yet implemented. Expect the sample
to remain `Ready=False`, its pooler to remain unready, and its application
Services to have no ready endpoints. The three orchestrator Pods become ready
only while their unique cluster-UID-bound etcd incarnations are renewed; this
does not enable durable operations, shard-term authority, or failover. This path
is for source validation only.

To exercise the direct PostgreSQL slice in the same cluster:

```console
kubectl create namespace pgshard-single-member
kubectl label namespace pgshard-single-member \
  pgshard.io/pod-fencing=enabled \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=latest
kubectl apply --namespace pgshard-single-member \
  -f operator/config/samples/pgshard_v1alpha1_single_member.yaml
kubectl wait --namespace pgshard-single-member --for=condition=Ready \
  pod/single-member-shard-0000-0 \
  pod/single-member-shard-0001-0 --timeout=180s
kubectl exec --namespace pgshard-single-member \
  single-member-shard-0000-0 -- \
  psql -X -U postgres -d postgres -c \
  "SELECT current_setting('server_version'), pg_is_in_recovery();"
kubectl exec --namespace pgshard-single-member \
  single-member-shard-0000-0 -- \
  psql -X -U postgres -d shardschema -c \
  "SELECT shard_id, shard_number, state FROM pgshard_catalog.shards ORDER BY shard_number;"
```

The one-member development resource also exposes the shard-zero compatibility
relay through `single-member-rw`. Wait for the pooler and port-forward the
Service:

```console
kubectl rollout status --namespace pgshard-single-member \
  deployment/single-member-pooler --timeout=180s
kubectl port-forward --namespace pgshard-single-member \
  service/single-member-rw 15432:5432
```

In another terminal that has `psql`:

```console
SHARD_ZERO_SECRET="$(kubectl get --namespace pgshard-single-member \
  pgshardcluster/single-member \
  -o jsonpath='{.status.postgresqlBootstraps[?(@.shard==0)].secretName}')"
SHARD_ZERO_PASSWORD="$(kubectl get --namespace pgshard-single-member \
  secret/"${SHARD_ZERO_SECRET}" -o jsonpath='{.data.password}' | base64 --decode)"
PGPASSWORD="${SHARD_ZERO_PASSWORD}" PGSSLMODE=disable \
  psql -X -h 127.0.0.1 -p 15432 -U postgres -d postgres \
  -c "SELECT current_setting('server_version'), pg_is_in_recovery();"
```

This proves only native PostgreSQL authentication and session relay to
shard-0000. It does not select a shard from SQL, pool connections, or provide
HA. The relay closes sessions after observed catalog-readiness loss but does not
fence each query against a catalog epoch or replay work. The generated
shard-zero credential is a PostgreSQL superuser credential, so this development
path is not safe for untrusted clients even though catalog and replication
startup modes are blocked.

The `pgshard.io/pod-fencing=enabled` namespace label is mandatory for managed
PostgreSQL Pods. Before publishing a workload, the controller completes an
authenticated admission challenge through the same namespace selector used for
Pod binding. The label then remains admission-immutable until the namespace is
deleted. The handshake confirms the admission path used for that update; it is
not a claim that every API-server selector cache has converged. Binding
admission copies the managed identity and selected Node UID and boot ID into
each Pod atomically with `spec.nodeName`, and final validation rejects a later
mutator that changes them. The init container exits before accessing PGDATA if
the binding evidence is absent, including when binding admission was skipped;
omitting the label therefore prevents PostgreSQL startup. Status admission also
rejects removal of the managed identity, binding identity, or termination
finalizer and cryptographically authenticates the durable terminal receipt.
Managed Pod specs and generations are immutable through ordinary, ephemeral-
container, and in-place-resize updates so that receipt cannot become stale.
This receipt is lifecycle evidence,
not physical node fencing. Do not
reuse a Node name while its old machine may still run a bound Pod; externally
fence that machine or its storage before recovery.

The internal `single-member-shard-0000` and
`single-member-shard-0001` Services exist for operator lifecycle tests, not as
open application endpoints. Their NetworkPolicies admit only same-namespace
Pods labelled for cluster `single-member` and component `pooler` or
`orchestrator` (plus the matching PostgreSQL shard), and remote SCRAM also
requires that shard's generated Secret. The operator does not yet distribute
those credentials to applications. Use the in-Pod `kubectl exec`, local
port-forward, or repository restricted-client KIND checks above.
`single-member-rw` relays only to shard-0000; `single-member-ro` and
`single-member-r` have no listener and no ready endpoint. PostgreSQL
StatefulSets use `OnDelete`; desired image or configuration changes do not
automatically restart every shard. Until staged upgrades exist, delete one Pod
at a time and expect an outage for that single-member shard. This includes
singleton StatefulSets created before catalog bootstrap was added: their
existing Pods do not run the new init container until explicitly replaced. Delete the
disposable cluster with `kind delete cluster --name pgshard-development`.
