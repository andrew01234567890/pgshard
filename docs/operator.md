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
Unix socket. The pooler's readiness probe is `/healthz` on its metrics port,
which it starts serving only once its gRPC listener is up, so a pod is Ready
only when both the agent and the pooler are — and the probe stays on a port
the member NetworkPolicy leaves open. The rendered shape of a member pod is
part of the template hash, so an operator that renders it differently rolls
existing members once, member by member, through the usual zero-downtime
path: pods are immutable, and a cluster created by an older operator would
otherwise keep the older shape indefinitely.

Setting `spec.internalTLS.secretRef` to a Secret holding `tls.crt`,
`tls.key` and `ca.crt` turns on mutual TLS between routers and poolers: the
pooler serves with the certificate and refuses any client whose certificate
does not chain to `ca.crt`, and the router dials with the same material.
Plaintext is never a fallback: without a `secretRef` the spec must set
`spec.internalTLS.insecure: true` (unsupported outside development, and only
tolerable behind a NetworkPolicy restricting port 9091 to router pods) or
the API server rejects the cluster.

**Upgrading a cluster made before this rule.** `spec.internalTLS` is
required, so a manifest that predates it stops applying the moment the new
CRD is installed -- and a GitOps controller reapplying it will report the
rejection rather than drift. A cluster already running keeps running; it is
the next apply that fails. Add one of the two to the manifest before or with
the CRD upgrade:

```yaml
spec:
  internalTLS:
    secretRef: {name: demo-internal-tls}   # or: insecure: true
```

The same applies to the two other required-unless-you-say-otherwise rules
this project has adopted since: `spec.networkPolicy.clients` when the policy
is enabled (see [Network policy](#network-policy)) and
`objectStore.encryption.secretRef` on a remote backup repository (see
[backup.md](backup.md#encryption)). Each rejects on write rather than
changing what a running cluster does, and each has an explicit opt-out for
the case where the plain thing is what was wanted.
`pg_hba.conf` admits only the control plane over TCP: the superuser,
`pgshard_router` and `pgshard_controller` for the router's and the
controller's catalog connections (both of which exist only where the catalog
schema does), and `pgshard_replication`, which exists on every group.
Everything else is rejected, so an application role reaches a shard through
the pooler's unix socket and the router, which is where shard-key routing,
the write fences and the coordination of a multi-shard write happen.

The pooler opens its change-stream connections as `pgshard_pooler`, also on
every group: `REPLICATION` for the logical decoding connection and the slot
a stream exports its snapshot from, `pg_read_all_data` for the tables that
snapshot is then copied out of, and nothing that writes. Its password is
generated into `<cluster>-pooler` and mounted at `/etc/pgshard/pooler`. With
that and the router role for the catalog, the pooler container carries
**no** `PGPASSWORD` and no superuser Secret at all — libpq applies that
variable to every connection lacking a password of its own, so one variable
there would hand both connections the same identity, and the identity it
used to hand them was direct write access to every shard.

A standby streams as `pgshard_replication`, not as the superuser:
`primary_conninfo` is written into every standby's `postgresql.auto.conf`
and travels in every clone, so the credential that reaches the most places
is the one that can do the least. It may stream, create its own physical
slot, and execute the four functions `pg_rewind` calls on a source it is not
superuser on -- and nothing else. The operator creates it on each group's
primary and physical replication carries it to that group's standbys; the
password is generated per cluster into `<cluster>-replication` and mounted
at `/etc/pgshard/replication`, from which the agent writes a `.pgpass` entry
rather than putting it on a `pg_basebackup` command line.

The switch is staged over two rolls, and has to be. `pg_hba` rejects an
identity it does not list, members roll one at a time with the primary
**last**, and the rollout holds as soon as the sync set is too small: a pass
that pointed every standby at the new role while the primary still ran the
old pod would restart a standby that cannot stream, drop the sync set, and
hold the roll before reaching the primary that would have admitted it. So a
member's `primary_conninfo` names the role only once **every** member pod of
the group carries `pgshard.io/replication-login`, and names the superuser
until then. Every pod, not just the primary's: a failover mid-roll, or a
demoted former primary still on the old pod, would otherwise flip it while
an old-shape pod is still there, and that pod would then be told to read a
Secret it does not mount.

Maintaining the role is skipped on a primary whose writes are paused --
`CREATE ROLE`, `ALTER ROLE` and `GRANT` are writes, and a paused primary
refuses them with 25006 -- and is never fatal to the pass. The password is
reapplied only when it differs from the one the role holds: `ALTER ROLE`
draws a fresh SCRAM salt every time, so doing it unconditionally would write
`pg_authid` and its WAL on every pass for every group.

The controller reaches the catalog as `pgshard_controller`: `pgshard_system`
membership for the schema it drives, plus `pg_read_all_stats` and the
`pg_create_restore_point`, `pg_switch_wal`, `pg_control_checkpoint`,
`pg_reload_conf` and `ALTER SYSTEM ON PARAMETER
default_transaction_read_only` grants the barrier needs on the catalog
group. Its password is generated per cluster into
`<cluster>-controller-login` and mounted at `/etc/pgshard/controller`, read
through `--catalog-password-file` rather than passed in argv, where
`/proc/<pid>/cmdline` would expose it.

`PGPASSWORD` in that container is still the superuser's, and two paths use
it. The shard and subscription DSNs: DDL, replication and finishing another
session's prepared transaction are superuser work on a shard. And
`--catalog-role-dsn`, which is role and DCL work on the *catalog group* —
`CREATE ROLE` needs `CREATEROLE`, a membership grant needs `ADMIN` on the
role, and comparing a SCRAM verifier means reading `pg_authid`, none of
which a least-privilege login has. Both are tracked separately from this.

The agent's own gRPC port (9090) requires a per-cluster token on every RPC,
so reaching the port is not enough to drive failovers. The token is
generated by the operator and mounted into every member from its own
Secret; it is not derived from the superuser password, so holding that
password does not let a caller drive a failover.

## Network policy

`spec.networkPolicy.enabled: true` renders `<cluster>-members`, a
NetworkPolicy selecting the cluster's **member** pods — the ones carrying
`pgshard.io/group-kind`, so the routers and the admin UI are left alone: a
router serves clients on 5432 and probes itself on that port, and the admin
UI listens on 8081, which no rule here opens.

| Rule | Ports | From |
|------|-------|------|
| restricted | 5432 (PostgreSQL), 9090 (agent gRPC), 9091 (pooler gRPC) | pods labelled `pgshard.io/cluster: <cluster>` (members, routers, admin) plus every peer in `spec.networkPolicy.clients` |
| open | 8080 (probes), 9127 (pooler metrics) | anywhere |

This is the layer under `pg_hba`, which already refuses an application role
over TCP: with the policy the port is unreachable rather than reachable and
refused, and it covers whatever else listens on a member.

Three things about it are deliberate.

**It is off by default, and turning it on requires naming the control
plane.** A NetworkPolicy is enforced by the CNI or silently ignored by it,
and one that fails to name a client of a member's PostgreSQL takes that
client off the cluster with nothing to read but a refused connection. The
operator dials the agent and the pooler from its own namespace and the
controller dials the catalog, so `spec.networkPolicy.clients` must not be
empty while the policy is enabled — the API server refuses the cluster
otherwise. Entries are ordinary `NetworkPolicyPeer`s (`podSelector`,
`namespaceSelector`, `ipBlock`); list anything that reaches a member from
outside the cluster's own pods, including a `pgshard-controller` you deploy
yourself and any migration job.

```yaml
networkPolicy:
  enabled: true
  clients:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pgshard-system   # the operator
    - podSelector:
        matchLabels:
          app.kubernetes.io/name: pgshard-controller
```

**The probe and metrics ports stay open to every source.** The kubelet is not
a pod, so no pod or namespace selector matches it; a rule that leaves it out
fails every readiness probe on a CNI that enforces policies.

**Egress is not restricted.** Members archive WAL to object storage, resolve
DNS and replicate to each other; the policy declares `Ingress` only.

Enforcement itself is not covered by the e2e suites: kind's default CNI does
not implement NetworkPolicies, so a test asserting a blocked connection would
pass for the wrong reason. The rendered object is asserted field by field in
envtest instead.

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
| Deployment | `serve --listen=:5432 --catalog-dsn=host=<cluster>-catalog-rw.<ns>.svc port=5432 user=pgshard_router dbname=postgres --catalog-pooler=<cluster>-catalog-rw.<ns>.svc:9091 --insecure-dev`; the password arrives as `PGPASSWORD` from the `<cluster>-router` Secret. That role is the catalog's own least-privilege login (see [router.md](router.md#startup-and-authentication)) -- not the superuser, whose password `pg_hba` accepts over TCP from anywhere and is therefore direct write access to every shard. Replicas start at `spec.router.minReplicas` and are then owned by the HPA. |
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
and 9091 (`pooler-grpc`). Router and poolers speak mutual TLS when
`spec.internalTLS.secretRef` is set; plaintext (`--insecure-dev`) is rendered
only under the explicit `spec.internalTLS.insecure: true` opt-in.
