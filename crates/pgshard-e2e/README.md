# End-to-end test root

This package runs only scenarios backed by a real disposable cluster. The
currently executable `operator-api-safety` scenario requires an existing KIND
context with the pgshard CRD installed. It verifies the current context before
running the operator's Kubernetes API safety tests with skip gates forced open.
The CI job owns KIND creation, CRD installation, diagnostic capture, and
unconditional cluster deletion.

Bootstrap routing, transaction failover, backup/restore, DDL/resharding, and
observability scenarios remain unimplemented and are deliberately not exposed
as passing matrix entries.

```console
PGSHARD_E2E_KUBE_CONTEXT=kind-pgshard-system-e2e \
  cargo run --locked -p pgshard-e2e -- --scenario operator-api-safety
```
