# Getting started

This guide takes an empty Kubernetes cluster to a running sharded PostgreSQL
you can connect to with `psql`.

## Prerequisites

- Kubernetes 1.29+ with a default StorageClass, or Docker and `kind` for a
  local one.
- `kubectl`, Docker and Go 1.26+.
- The container images. **CI publishes only the PostgreSQL images**
  (`ghcr.io/andrew01234567890/pgshard-postgres:{18,19}`); the router, admin,
  operator and controller images are built locally:

  ```sh
  hack/dev/build-images.sh dev 18 postgres control controller
  ```

## The whole thing on kind, in one command

```sh
make dev-up    # images, kind cluster, images loaded, operator, sample cluster
```

`hack/dev/up.sh` does what the steps below do, in order, and points the
operator at the images it just built — without that last part every cluster
it creates would pull a router image that was never published. Tear it down
with `make kind-down`.

## Or step by step, on a cluster you already have

Install the CRDs and the operator:

```sh
make deploy IMG=<your operator image>   # CRDs, namespace, RBAC, operator Deployment
```

`make deploy` substitutes the operator's own image only. The operator
creates the router and admin workloads from `--router-image` and
`--admin-image`, which default to the published tags, so a locally built
router needs those flags set on the operator Deployment as `hack/dev/up.sh`
does.

`make kind-up` creates the kind cluster and nothing else — no images are
built or loaded, which is what the e2e suites want because they load their
own. The operator can also be run outside the cluster with
`go run ./cmd/pgshard-operator run` against the current kubeconfig.

## Create a cluster

A `PgShardCluster` describes the whole system: the catalog group, the shard
groups, the router tier and the admin UI. Minimal example:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardCluster
metadata:
  name: demo
spec:
  internalTLS:
    insecure: true
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

```sh
docker compose -f hack/compose/docker-compose.yml --profile router up --build
```

starts a catalog, one shard, its pooler and a router on port 6432. `pgshard-router dev-bootstrap`
migrates the catalog and registers a database, a role and the shard's pooler
endpoint (password from `PGSHARD_DEV_PASSWORD`).

## Where to next

- [Defining sharding](sharding.md) — declare databases, tables, shard keys.
- [Queries, DDL and DCL](queries.md) — what runs where, and what is refused.
- [Transactions](transactions.md) — single-shard and two-phase commit.
- [Backups and restore](backup-restore.md) — object stores, PITR, barriers.
- [Admin UI](admin-ui.md) — topology, migrations, backups, streams.
