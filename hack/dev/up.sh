#!/usr/bin/env bash
# Takes an empty machine to a running pgshard cluster on kind: builds every
# image, creates the cluster, loads them, deploys the operator pointed at
# the local images, and applies the sample PgShardCluster.
#
# It exists because the pieces did not add up on their own. CI publishes
# only the PostgreSQL images, so the router, admin and operator images have
# to be built locally; make kind-up creates a cluster and loads nothing;
# and make deploy leaves the operator's --router-image and --admin-image at
# their published defaults, which a locally built router is not.
#
# Usage: hack/dev/up.sh [pg-major]
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
major="${1:-${PG_MAJOR:-18}}"
tag="${PGSHARD_DEV_TAG:-dev}"
cd "$root"

echo "==> images"
hack/dev/build-images.sh "$tag" "$major" postgres control controller

echo "==> kind cluster"
hack/kind/up.sh
hack/kind/load-images.sh \
	"pgshard-operator:$tag" "pgshard-admin:$tag" "pgshard-router:$tag" "pgshard-controller:$tag" \
	"ghcr.io/andrew01234567890/pgshard-postgres:$major"

echo "==> operator"
make install
kubectl apply -f config/manager/namespace.yaml
kubectl apply -f config/rbac
# The operator creates the router and admin workloads, so it is the operator
# that has to be told which images to use; substituting only its own would
# leave every cluster it makes pulling images that were never published.
sed -e "s|image: ghcr.io/andrew01234567890/pgshard-operator:latest|image: pgshard-operator:$tag|" \
    -e "s|^\( *\)- --metrics-bind-address=:8080|\\1- --metrics-bind-address=:8080\n\\1- --router-image=pgshard-router:$tag\n\\1- --admin-image=pgshard-admin:$tag|" \
    config/manager/manager.yaml | kubectl apply -f -
kubectl -n pgshard-system rollout status deploy/pgshard-operator --timeout=180s

echo "==> cluster"
kubectl apply -f config/samples/pgshard_v1alpha1_pgshardcluster.yaml

cat <<EOF

The operator is running and a PgShardCluster named demo is being created.

  kubectl get psc demo -w
  kubectl get pods -l pgshard.io/cluster=demo

When Ready and RouterReady are True, connect through the router with the
credential the operator generated -- see the "first login" section of
docs/guide/getting-started.md.

Tear it down with: make kind-down
EOF
