# pgshard-operator

`pgshard-operator run` reconciles `PgShardCluster` objects into the catalog
group, the shard groups, the admin UI and the router tier. This page covers
the pieces not described in [ha.md](ha.md), [catalog.md](catalog.md) and
[admin.md](admin.md).

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--admin-image` | `ghcr.io/andrew01234567890/pgshard-admin:latest` | Image of the admin UI Deployment. |
| `--router-image` | `ghcr.io/andrew01234567890/pgshard-router:latest` | Image of the router Deployment. |
| `--leader-elect` | off | Run with leader election. |

## Member pods

Every member pod runs two containers from the same `pgshard-postgres` image:

| Container | Command | Ports |
|-----------|---------|-------|
| `postgres` | `pgshard-agent run --config /etc/pgshard/<member>.json` (PID 1, supervises PostgreSQL) | 5432, 8080 (HTTP), 9090 (gRPC) |
| `pooler` | `pgshard-pooler run --listen :9091 --pg-socket-dir /tmp --catalog-dsn <catalog -rw> --shard-set <default|catalog> --shard-id <n> --insecure-dev` | 9091 (gRPC) |

The agent pins `unix_socket_directories` to `/tmp`; the two containers share
that directory through an emptyDir so the pooler reaches PostgreSQL over the
Unix socket. The pooler's readiness probe is a TCP check on 9091, so a pod
is Ready only when both the agent and the pooler are. `--insecure-dev` is the
plaintext development mode; mTLS between router and pooler is a later step.

## Shard groups and resharding

The serving shard groups are `shard-<id>` for the shard count the catalog
holds (`status.effectiveShards`; `spec.shards` only until the catalog
exists). Once the catalog group is ready and migrated the operator
materializes the serving shard set, and a different `spec.shards` becomes a
reshard: a pending shard set, a `PgShardReshard` record and non-serving
target groups `shard-<id>-g<generation>`. See [resharding.md](resharding.md).

## Router

For every cluster the operator owns `<cluster>-router`:

| Object | Content |
|--------|---------|
| ServiceAccount | Identity of the router pods. |
| Deployment | `serve --listen=:5432 --catalog-dsn=host=<cluster>-catalog-rw.<ns>.svc port=5432 user=postgres dbname=postgres --catalog-pooler=<cluster>-catalog-rw.<ns>.svc:9091 --insecure-dev`; the superuser password arrives as `PGPASSWORD` from the `<cluster>-superuser` Secret. Replicas start at `spec.router.minReplicas` and are then owned by the HPA. |
| Service | ClusterIP on 5432, the endpoint applications connect to. |
| HorizontalPodAutoscaler | `autoscaling/v2`, CPU utilization target `spec.router.hpa.cpuUtilization` (default 70) between `minReplicas` and `maxReplicas`. |
| PodDisruptionBudget | `minAvailable: 1`. |

When `spec.router.tls.secretRef` is set the Secret is mounted read-only at
`/etc/pgshard-tls` and `--tls-dir=/etc/pgshard-tls` is appended to the args.

The router reaches the catalog pooler through `--catalog-pooler
<cluster>-catalog-rw.<namespace>.svc:9091` and discovers every shard's pooler
through `pgshard.shard_status.primary_endpoint`, which the operator publishes
as the primary member's pooler (`<member>.<group>-peers.<namespace>.svc:9091`),
not its PostgreSQL port. Every `-rw` Service exposes both 5432 (`postgres`)
and 9091 (`pooler-grpc`). Router and poolers still speak plaintext
(`--insecure-dev`); mTLS between them is a later step.
