# pgshard operator

This module contains the Go control-plane foundation for the namespaced
`PgShardCluster` API. It currently validates and defaults the API and reports a
truthful `Ready=False` status. It does not yet create PostgreSQL workloads.

The module is pinned to Go 1.26.5, controller-runtime 0.24.1, and Kubernetes
libraries 0.36.0. Only the Linux container deployment is supported.

Run the local checks from this directory:

```console
go test ./...
go vet ./...
```

Regenerate API objects and install manifests with controller-tools 0.21.0:

```console
go tool controller-gen object paths=./...
go tool controller-gen crd:allowDangerousTypes=false paths=./... output:crd:artifacts:config=config/crd/bases
go tool controller-gen rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
go tool controller-gen webhook paths=./... output:webhook:artifacts:config=config/webhook
```

CI regenerates these files in a clean checkout and fails on any diff. No helper
shell scripts or `hack` directory are required.
