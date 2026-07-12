---
title: Quickstart
sidebar_position: 2
description: Create a local pgshard cluster on KIND.
---

# Quickstart

The Milestone 1 quickstart targets a local [KIND](https://kind.sigs.k8s.io/) cluster. Commands shown here describe the intended public interface; mark incomplete commands clearly until their implementation lands.

:::info Implementation status
The project is being built incrementally. The repository's release notes identify the first version in which each command becomes available.
:::

## Prerequisites

- A container runtime
- `kubectl`
- KIND
- Helm
- PostgreSQL `psql` 18

## Create the cluster

```bash
kind create cluster --name pgshard
helm install pgshard ./deploy/charts/pgshard-operator \
  --namespace pgshard-system --create-namespace
kubectl apply -f examples/quickstart.yaml
kubectl wait --for=condition=Ready pgshardcluster/quickstart --timeout=10m
```

The example creates multiple shards with three PostgreSQL members per shard and two pooler replicas. For local development, reduce replicas only through the cluster resource; do not edit generated StatefulSets.

## Connect

```bash
kubectl port-forward service/quickstart-rw 5432:5432
psql 'postgresql://app@localhost:5432/app?sslmode=require'
```

Use the service that matches the workload:

- `quickstart-rw` supports reads and writes through shard primaries.
- `quickstart-ro` is read-only and uses replicas without primary fallback.
- `quickstart-r` is read-only, prefers replicas, and can fall back to a primary.

## Define a sharded table

Table placement is declarative. A sharded table has one immutable shard-key column, and every primary or unique key includes that column.

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardTable
metadata:
  name: accounts
spec:
  clusterRef: quickstart
  database: app
  schema: public
  table: accounts
  shardKey: account_id
```

Continue with [SQL compatibility](./reference/sql-compatibility.md) before moving an existing application.
