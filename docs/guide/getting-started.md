# Getting started

This guide takes an empty Kubernetes cluster to a running sharded PostgreSQL
you can connect to with `psql`.

## Prerequisites

- Kubernetes 1.29+ with a default StorageClass.
- `kubectl` and, to build images locally, Docker and Go 1.26+.
- The container images. CI pushes them to GHCR
  (`ghcr.io/andrew01234567890/pgshard-postgres:{18,19}`, `pgshard-router`,
  `pgshard-admin`); build your own with `docker buildx bake postgres`,
  `make operator-image admin-image` and
  `docker build -f Dockerfile.router .`.

## Install the operator

Install the CRDs and the operator:

```sh
make deploy    # CRDs (make install), namespace, RBAC, operator Deployment
```

For development, `make kind-up` (via `hack/kind/up.sh`, used by the e2e
suites) creates a kind cluster with the images preloaded, and the operator
can be run locally with `go run ./cmd/pgshard-operator run` against the
current kubeconfig.

## Create a cluster

A `PgShardCluster` describes the whole system: the catalog group, the shard
groups, the router tier and the admin UI. Minimal example:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardCluster
metadata:
  name: demo
spec:
  postgresql:
    major: 18            # 18 or 19
    profile: oltp        # oltp | mixed | analytics (drives automatic tuning)
  shards: 2
  replicasPerShard: 3    # minimum 3 for HA
  catalog:
    replicas: 3
    storage:
      size: 10Gi
  storage:
    size: 50Gi
  resources:
    requests:
      cpu: "2"
      memory: 8Gi
    limits:
      cpu: "2"
      memory: 8Gi
  durability:
    synchronousCommit: "on"
    minSyncStandbys: 1
  router:
    minReplicas: 2
    maxReplicas: 10
```

```sh
kubectl apply -f demo.yaml
kubectl get psc demo             # Shards, Ready, Age
kubectl get psc demo -o jsonpath='{.status.conditions}'
```

The operator provisions one replication group for the catalog and one per
shard (each: pods with `pgshard-agent` as PID 1 and a `pgshard-pooler`
sidecar, PVCs, `-rw`/`-ro` Services, PodDisruptionBudgets), derives the
PostgreSQL configuration from `spec.resources` and the profile
([tuning.md](../tuning.md)), deploys the router behind
`<cluster>-router:5432` with an HPA, and the admin UI at
`<cluster>-admin:8081` unless `spec.admin.enabled: false`.

The cluster is usable when the `Ready` condition is `True` and
`RouterReady` is `True`.

## Connect

The router speaks the ordinary PostgreSQL wire protocol and authenticates
with SCRAM-SHA-256 against roles stored in the catalog
(`pgshard.roles`). Create an application database and role through the
catalog first — see [Defining sharding](sharding.md) — then:

```sh
kubectl port-forward svc/demo-router 5432:5432
psql "host=127.0.0.1 port=5432 dbname=app user=app_rw"
```

Inside the cluster, applications connect to
`demo-router.<namespace>.svc:5432`. TLS on the client side is enabled by
`spec.router.tls.secretRef` (a Secret with `tls.crt`/`tls.key`).

The catalog database itself is routable: `dbname=pgshard` connects a session
to the catalog, which is how you edit the desired-state tables with plain
SQL.

## Local development without Kubernetes

`hack/compose/docker-compose.yml --profile router` starts a catalog, one
shard, its pooler and a router on port 6432. `pgshard-router dev-bootstrap`
migrates the catalog and registers a database, a role and the shard's pooler
endpoint (password from `PGSHARD_DEV_PASSWORD`).

## Where to next

- [Defining sharding](sharding.md) — declare databases, tables, shard keys.
- [Queries, DDL and DCL](queries.md) — what runs where, and what is refused.
- [Transactions](transactions.md) — single-shard and two-phase commit.
- [Backups and restore](backup-restore.md) — object stores, PITR, barriers.
- [Admin UI](admin-ui.md) — topology, migrations, backups, streams.
