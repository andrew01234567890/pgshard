#!/usr/bin/env bash
set -euo pipefail
CHAOS_MESH_VERSION="${CHAOS_MESH_VERSION:-2.8.4}"
helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null
helm repo update chaos-mesh >/dev/null
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace chaos-mesh --create-namespace \
  --version "$CHAOS_MESH_VERSION" \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --set dashboard.create=false \
  --wait --timeout 10m
kubectl -n chaos-mesh rollout status deploy/chaos-controller-manager --timeout=5m
