# 6. Keeping coordination in Kubernetes and the catalog, not in etcd

Status: accepted

## Context

pgshard needs a place for leader election, the serving shard map, promotion
epochs, the two-phase decision log, workflow state and role verifiers. Every
comparable system reaches for a consensus store; Vitess uses one, and etcd
is what a Kubernetes-native project would deploy.

## Decision

No separate consensus store. Coordination lives in two places that are
already in the deployment:

- **Kubernetes** — CRDs for desired infrastructure, Leases for operator and
  controller leadership and for the per-shard promotion mutex.
- **The catalog group**, a PostgreSQL cluster running the `pgshard`
  database — the shard map and its generation, promotion epochs, the
  decision log, workflows, migrations, streams, sequences and role
  verifiers.

The decision log is why this is not just economy. A two-phase commit's
decision must be durable at the same instant the participants are asked to
prepare, and it must survive the coordinator. Writing it to PostgreSQL puts
it in a transaction with the same durability guarantee as the data — the
same `synchronous_commit`, the same failover, the same backups, the same
point-in-time recovery. In a separate store it would be a second thing to
back up and a second thing that can be restored to a different point than
the shards it decides for.

Routers watch the catalog with `LISTEN`/`NOTIFY` plus a periodic reload,
not the Kubernetes API. A router does not need to see pods; it needs to see
the serving map and the generation it is stamped with, and those are rows.

## Consequences

- The catalog group is on the availability path for every write that needs
  a new decision, so it is a replicated PostgreSQL cluster with the same HA
  as any shard, not a single instance.
- A cluster-consistent restore covers coordination state and data together,
  because they are in the same kind of thing. Reconciling `pg_prepared_xacts`
  against the restored decision log is possible precisely because both were
  captured at the same barrier.
- Leadership and the promotion mutex use Kubernetes Leases, so pgshard does
  not run outside Kubernetes today. That is a deliberate narrowing: the
  operator is the single decision maker for promotion, and it needs a mutex
  the API server enforces.
