#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
name="${KIND_CLUSTER_NAME:-pgshard-e2e}"
if kind get clusters 2>/dev/null | grep -qx "$name"; then
  echo "kind cluster $name already exists"
else
  kind create cluster --name "$name" --config "$here/config.yaml" --wait 120s
fi
kubectl --context "kind-$name" wait --for=condition=Ready nodes --all --timeout=180s
kubectl --context "kind-$name" get nodes -o wide
