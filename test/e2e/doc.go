// Package e2e contains end-to-end tests that run against a Kubernetes cluster.
//
// Build with -tags e2e. The cluster is selected by KUBECONFIG, falling back to
// the kind cluster named by KIND_CLUSTER_NAME (default pgshard-e2e). Diagnostics
// are written to E2E_ARTIFACTS (default ./artifacts).
package e2e
