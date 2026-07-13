# pgshard operator

This module contains the Go control plane for the namespaced `PgShardCluster`
API. The controller now reconciles the safe supporting-resource slice:

- generated topology and resource-derived PostgreSQL configuration ConfigMaps;
- CNPG-style `<cluster>-rw`, `<cluster>-ro`, and `<cluster>-r` application
  Services, each targeting its own pooler listener;
- one internal headless Service per shard;
- etcd, orchestrator, and pooler workload specifications, topology spread,
  security contexts, PodDisruptionBudgets, HPA or fixed pooler scaling, and an
  etcd ingress NetworkPolicy;
- an internal pooler HTTP Service plus fail-closed readiness and independent
  liveness probe contracts; the control Service retains unready endpoints for
  outage diagnostics while application Services continue filtering them; and
- controller ownership, update pruning, and finalizer-based deletion pruning.

This is not a working PostgreSQL cluster. The controller intentionally creates
no PostgreSQL Pods or data PVCs because bootstrap, replication, fencing
integration, promotion, and recovery are not implemented. Application Services
therefore must not be treated as usable endpoints. `Ready=False` with reason
`PostgreSQLLifecycleUnavailable` remains authoritative even if the supporting
workloads become available. Backup execution and ServiceMonitor reconciliation
also remain unimplemented. The etcd NetworkPolicy allows only selected
same-cluster Pods, but client and peer traffic is still unauthenticated
plaintext; the independent `TransportSecurityReady=False` condition reports
that TLS gap. Etcd uses independent 2Gi PVCs on `storage.storageClassName` with
a bounded backend quota. Scale transitions retain those claims; cluster
deletion keeps the CR finalizer until UID-owned StatefulSets and PVCs are
observed absent through the uncached Kubernetes API reader, preventing informer
lag from allowing same-name recreation to mount stale etcd state. Automated
defragmentation is not implemented. PostgreSQL
`archive_mode` remains off until a real archival pipeline is reconciled and
verified, so the generated configuration cannot silently fill `pg_wal`.

The default orchestrator and pooler image values are expected development
channel names, not a publication guarantee. The Rust pooler crate contains
catalog-derived HTTP handlers but no executable, PostgreSQL listener, or
connection pool. Override the defaults with `--orchestrator-image` and
`--pooler-image` when concrete images exist. `--etcd-image` is also
configurable. Image pull or runtime readiness is reported only through
`SupportingWorkloadsAvailable`, never as database readiness.

The module is pinned to Go 1.26.5, controller-runtime 0.24.1, and Kubernetes
libraries 0.36.0. Only the Linux container deployment is supported.

Run the local checks from this directory:

```console
go test -race ./...
go vet ./...
go build ./...
go tool govulncheck ./...
```

Regenerate API objects and install manifests with controller-tools 0.21.0:

```console
go tool controller-gen object paths=./...
go tool controller-gen crd:allowDangerousTypes=false paths=./... output:crd:artifacts:config=config/crd/bases
go tool controller-gen rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
go tool controller-gen webhook paths=./... output:webhook:artifacts:config=config/webhook
```

To verify that all checked-in generated output is reproducible, run the four
generation commands above in a clean checkout, then run:

```console
git diff --exit-code -- api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac config/webhook
```

The repository `make go-check` target and the Go operator CI job run formatting,
module tidy/verification, `go vet ./...`, `go test -race ./...`,
`go build ./...`, `go tool govulncheck ./...`, and the generation-and-diff
sequence above. No helper shell scripts or `hack` directory are required.

CI also creates a digest-pinned Kubernetes 1.36 KIND cluster and exercises StatefulSet/PVC
creation, supervised deletion, and same-name recreation against real
Kubernetes controllers. This targeted safety test is not yet the full
Milestone 1 KIND suite.
