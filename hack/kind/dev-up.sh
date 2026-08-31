#!/usr/bin/env bash
# Brings up the whole development stack: a kind cluster, the images this
# repository builds, the operator running them, and a small cluster.
#
# The images are built locally and loaded into kind rather than pulled: only
# the PostgreSQL images are published, so a router or operator tag would not
# resolve, and a locally built one has to be named to the operator explicitly
# (--router-image) or it would ask for the unpublished tag instead.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cluster="${KIND_CLUSTER_NAME:-pgshard-e2e}"
major="${PG_MAJOR:-18}"

pg_img="ghcr.io/andrew01234567890/pgshard-postgres:${major}"
op_img="pgshard-operator:dev"
router_img="pgshard-router:dev"
admin_img="pgshard-admin:dev"

echo "==> kind cluster"
"$here/up.sh"

echo "==> PostgreSQL $major image"
# The PostgreSQL image is a source build and takes a long time, so an image
# already present is reused; delete it to rebuild.
if ! docker image inspect "$pg_img" >/dev/null 2>&1; then
  (cd "$root" && docker buildx bake "postgres-${major}")
fi

echo "==> control-plane images"
(cd "$root" && docker build -q -f Dockerfile.control --build-arg CMD=pgshard-operator -t "$op_img" . >/dev/null)
(cd "$root" && docker build -q -f Dockerfile.router -t "$router_img" . >/dev/null)
# The admin UI is deployed by the operator for every cluster, so its image
# has to be here too or the Deployment sits in ImagePullBackOff.
(cd "$root" && docker build -q -f Dockerfile.control --build-arg CMD=pgshard-admin -t "$admin_img" . >/dev/null)

echo "==> loading images into kind"
"$here/load-images.sh" "$pg_img" "$op_img" "$router_img" "$admin_img"

echo "==> operator"
kubectl --context "kind-$cluster" apply -f "$root/config/crd/bases"
kubectl --context "kind-$cluster" apply -f "$root/config/manager/namespace.yaml"
kubectl --context "kind-$cluster" apply -f "$root/config/rbac"
# The router image is passed as a flag because the operator creates the
# router Deployment itself; without it the pods would ask for a tag nobody
# publishes.
sed -e "s|image: ghcr.io/andrew01234567890/pgshard-operator:latest|image: $op_img|" \
    -e "s|^\( *\)- --leader-elect|\1- --leader-elect\n\1- --router-image=$router_img\n\1- --admin-image=$admin_img|" \
    "$root/config/manager/manager.yaml" | kubectl --context "kind-$cluster" apply -f -
kubectl --context "kind-$cluster" -n pgshard-system rollout status deploy/pgshard-operator --timeout=180s

echo "==> demo cluster"
sed "s|major: 18|major: $major|" "$root/config/samples/pgshard_v1alpha1_pgshardcluster.yaml" |
  kubectl --context "kind-$cluster" apply -f -

cat <<TXT

The operator is running and the demo cluster is applied. It takes a few
minutes to provision. Watch it with:

  kubectl --context kind-$cluster get pgshardcluster demo -w

and connect once it is Ready:

  kubectl --context kind-$cluster get secret demo-superuser -o jsonpath='{.data.password}' | base64 -d
  kubectl --context kind-$cluster port-forward svc/demo-router 5432:5432

Tear the whole thing down with: make kind-down
TXT
