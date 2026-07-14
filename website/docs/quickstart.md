---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no installable pgshard database cluster yet. The source includes the Go
custom-resource API and safe supporting-resource reconciler plus fail-closed
Rust agent/orchestrator foundations and a local-only pooler catalog control
executable plus local-only test image builds. The agent can structurally
preflight PostgreSQL 18 state and supervise its postmaster with client TCP and
replication ingress disabled and all local SQL rejected for lifecycle testing,
but there is no bootstrap,
role-aware recovery, replication, client activation,
operator-managed PostgreSQL workload, executable SQL pooler, chart, or full
database KIND environment. A self-managed admission manager manifest exercises
the real operator, fail-closed webhooks, and supporting processes in KIND, but
it creates no PostgreSQL workload and is not a database installation. A cluster
quickstart will appear only after those end-to-end tests pass.

## Validate the current source

The supported development environment is Linux with Rust 1.97, Go 1.26,
Node.js 22 and Buf 1.71. From a checkout:

```console
make check
```

This validates contracts, core and runtime foundations, generated Kubernetes
resources, release policy and documentation. CI separately exercises the
local-only lifecycle image with a disposable PostgreSQL 18 data directory; the
quickstart does not start PostgreSQL or prove a sharding runtime. Follow
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
have no ready endpoints. This path is for source validation only. Delete the disposable cluster with
`kind delete cluster --name pgshard-development`.
