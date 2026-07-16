---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no usable pgshard routing endpoint yet. The current operator can run a
limited direct-PostgreSQL development slice: an explicit one-member
asynchronous resource creates one PostgreSQL 18 primary and retained PVC per
shard. It has no standby, promotion, fencing, backup execution, `shardschema`
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
operator KIND job also starts two single-member primaries, queries one through
its shard Service, restarts it, and verifies persistence. Neither test proves a
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
kubectl apply --namespace pgshard-development -f operator/config/samples/pgshard_v1alpha1_development.yaml
```

The admission overlay provisions an ECDSA serving chain into exact-name
operator-managed Secrets, injects the CA bundle, and keeps semantic validation
fail closed. Its leaf certificate renews without a Pod restart; automatic CA
rotation is not yet implemented. Expect the sample to remain `Ready=False`, its
pooler and orchestrator Pods to remain unready, and its application Services to
have no ready endpoints. This path is for source validation only.

To exercise the direct PostgreSQL slice in the same cluster:

```console
kubectl create namespace pgshard-single-member
kubectl label namespace pgshard-single-member \
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

Use the internal `single-member-shard-0000` and
`single-member-shard-0001` Services for direct test traffic. The
`single-member-rw`, `-ro`, and `-r` Services still lead to the rejection-only
pooler. Delete the disposable cluster with
`kind delete cluster --name pgshard-development`.
