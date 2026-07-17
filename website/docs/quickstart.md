---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no usable pgshard routing endpoint yet. The current operator can run a
limited direct-PostgreSQL development slice: an explicit one-member
asynchronous resource creates one PostgreSQL 18 primary and retained PVC per
shard. Each shard receives a distinct immutable bootstrap Secret and a 4Gi
minimum data claim. When `storage.storageClassName` is omitted, the operator
resolves the current Kubernetes default and checkpoints that exact class before
creating any claim; later default-class rotation does not change existing data
intent. Admission records the selected Node UID and boot ID atomically with Pod
binding and validates the final Binding after all mutators run. The PostgreSQL
init container refuses to touch PGDATA unless that evidence reaches the Pod.
Admission then attaches an HMAC-authenticated process-stop condition only to a
terminal status update from that exact authenticated kubelet, and validates the
final status after all mutators run. PodGC, copied annotation or condition text,
a missing or recreated Node, and a webhook outage cannot produce that receipt,
so cluster deletion fails closed with the PVC protected. This is lifecycle protection, not
HA takeover or physical node fencing. It has no standby, promotion,
backup execution, `shardschema`
bootstrap, or SQL pooler. Restarting a primary interrupts that shard even though
its data survives. Three- and five-member resources continue to fail closed
without PostgreSQL Pods.

The source also includes the Go custom-resource API, fail-closed Rust
agent/orchestrator foundations, a rejection-only pooler, and local-only test
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
operator KIND job also starts two single-member primaries, queries one from an
authorized restricted probe client using that destination shard's credential, restarts it, and
verifies persistence. Neither test proves a
sharding runtime. Follow
[implementation status](./project/status.md) for the first version with a real
cluster quickstart.

The catalog migration has a separate opt-in live contract test against a
disposable PostgreSQL 18 `shardschema` database. See the
[`pgshard-catalog` README](https://github.com/andrew01234567890/pgshard/tree/main/crates/pgshard-catalog)
for its preconditions. CI runs that test with an ephemeral service database;
the operator does not bootstrap it yet.

## Exercise the development manager

Build the local-only images with an archive-capable Buildx builder, load the
operator, orchestrator, and pooler `:dev` images into a disposable KIND cluster,
then install the manager:

```console
PGSHARD_IMAGE_TARGETS="operator orchestrator pooler" make images
docker load --input artifacts/images/pgshard-operator.tar
docker load --input artifacts/images/pgshard-orchestrator.tar
docker load --input artifacts/images/pgshard-pooler.tar
kind create cluster --name pgshard-development \
  --image kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  --wait 90s
kind load docker-image pgshard/operator:dev pgshard/orchestrator:dev \
  pgshard/pooler:dev --name pgshard-development
kubectl apply -k operator/config/admission
kubectl rollout status --namespace pgshard-system deployment/pgshard-controller-manager
kubectl create namespace pgshard-development
kubectl label namespace pgshard-development pgshard.io/pod-fencing=enabled
kubectl apply --namespace pgshard-development -f operator/config/samples/pgshard_v1alpha1_development.yaml
```

The admission overlay provisions an ECDSA serving chain and a separate immutable
256-bit fencing key into exact-name operator-managed Secrets, injects the CA
bundle, and anchors the key's SHA-256 continuity fingerprint in the independent
CA Secret's metadata, preserving the Secret data shape accepted by the previous
manager for rollback. Fresh-install authority requires all three material
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
automatic CA or fencing-key rotation is not yet implemented. Expect the sample to remain `Ready=False`, its
pooler and orchestrator Pods to remain unready, and its application Services to
have no ready endpoints. This path is for source validation only.

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
  pod/single-member-shard-0000-primary-0 \
  pod/single-member-shard-0001-primary-0 --timeout=180s
kubectl exec --namespace pgshard-single-member \
  single-member-shard-0000-primary-0 -- \
  psql -X -U postgres -d postgres -c \
  "SELECT current_setting('server_version'), pg_is_in_recovery();"
```

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
those credentials to applications, so use the in-Pod `kubectl exec` check above
or the repository's restricted-client KIND test. The `single-member-rw`, `-ro`,
and `-r` Services still lead to the rejection-only pooler. Delete the
disposable cluster with `kind delete cluster --name pgshard-development`.
