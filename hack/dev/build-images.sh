#!/usr/bin/env bash
# Builds the pgshard images a local cluster needs, tagged <name>:<tag>.
# One definition, used by hack/e2e/run.sh and hack/dev/up.sh: a local run
# that built different images from the one CI builds would prove nothing.
#
# Usage: hack/dev/build-images.sh <tag> <pg-major> [postgres|control|controller|target-major]...
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tag="${1:?image tag (e.g. e2e or dev)}"
major="${2:?pg major (e.g. 18)}"
shift 2
cd "$root"

for what in "$@"; do
	case "$what" in
	postgres)     docker buildx bake "postgres-$major" --load ;;
	control)
		docker build -f Dockerfile.control --build-arg CMD=pgshard-operator -t "pgshard-operator:$tag" .
		docker build -f Dockerfile.control --build-arg CMD=pgshard-admin -t "pgshard-admin:$tag" .
		docker build -f Dockerfile.router -t "pgshard-router:$tag" .
		;;
	controller)   docker build -f Dockerfile.control --build-arg CMD=pgshard-controller -t "pgshard-controller:$tag" . ;;
	target-major) docker buildx bake postgres-19 --load ;;
	*) echo "build-images.sh: unknown image group $what" >&2; exit 2 ;;
	esac
done
