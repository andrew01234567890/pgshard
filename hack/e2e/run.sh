#!/usr/bin/env bash
# Runs one e2e suite the way CI runs it: the images that suite needs, a kind
# cluster, the images loaded into it, and the one go test invocation with the
# timeout and filter that suite is known to need.
#
# `go test ./test/e2e/...` is not that. It runs every package in one
# invocation under Go's default ten-minute timeout, while a single test in
# there is allowed fifty minutes to nearly two hours; packages run
# concurrently, and each one's deployOperator installs and deletes the same
# CRDs, namespace, RBAC and operator as the others. It fails for reasons
# that say nothing about the code.
#
# Usage: hack/e2e/run.sh <suite> [pg-major]
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
suite="${1:-}"
major="${2:-${PG_MAJOR:-18}}"

# suites is the whole definition of what an e2e suite is: the go test
# arguments, and which images it needs beyond the base set. CI runs the same
# names; hack/e2e/test-suites.sh checks the two have not drifted apart.
case "$suite" in
# The root package only, and unfiltered. -run Smoke over ./test/e2e/...
# reached exactly one test and silently excluded the others in that
# package -- TestForwardedAddr and the image-pull backoff tests are pure
# table tests of e2e helpers, needing no cluster and running in
# milliseconds, and they had never executed anywhere. Naming the package
# rather than filtering by test name keeps that from recurring.
smoke)         args=(./test/e2e/); needs="" ;;
operator)      args=(-timeout 70m ./test/e2e/operator/...); needs="base controller" ;;
backup)        args=(-timeout 70m ./test/e2e/backup/...); needs="base controller" ;;
reshard)       args=(-timeout 50m -skip 'TestReshardSplitUnderLoad|TestReshardMergeUnderLoad' ./test/e2e/reshard/...); needs="base controller" ;;
reshard-split) args=(-timeout 110m -run TestReshardSplitUnderLoad ./test/e2e/reshard/...); needs="base controller" ;;
reshard-merge) args=(-timeout 110m -run TestReshardMergeUnderLoad ./test/e2e/reshard/...); needs="base controller" ;;
upgrade)       args=(-timeout 110m ./test/e2e/upgrade/...); needs="base controller target-major" ;;
*)
	echo "usage: hack/e2e/run.sh <suite> [pg-major]" >&2
	echo "suites: smoke operator backup reshard reshard-split reshard-merge upgrade" >&2
	exit 2
	;;
esac

cd "$root"
if [[ "$needs" == *base* ]]; then
	echo "==> building images"
	docker buildx bake "postgres-$major" --load
	docker build -f Dockerfile.control --build-arg CMD=pgshard-operator -t pgshard-operator:e2e .
	docker build -f Dockerfile.control --build-arg CMD=pgshard-admin -t pgshard-admin:e2e .
	docker build -f Dockerfile.router -t pgshard-router:e2e .
fi
if [[ "$needs" == *controller* ]]; then
	docker build -f Dockerfile.control --build-arg CMD=pgshard-controller -t pgshard-controller:e2e .
fi
if [[ "$needs" == *target-major* ]]; then
	docker buildx bake postgres-19 --load
fi

echo "==> kind cluster"
hack/kind/up.sh
if [[ "$needs" == *base* ]]; then
	hack/kind/load-images.sh pgshard-operator:e2e pgshard-admin:e2e pgshard-router:e2e "ghcr.io/andrew01234567890/pgshard-postgres:$major"
fi
if [[ "$needs" == *controller* ]]; then
	hack/kind/load-images.sh pgshard-controller:e2e
fi
if [[ "$needs" == *target-major* ]]; then
	hack/kind/load-images.sh ghcr.io/andrew01234567890/pgshard-postgres:19
fi

echo "==> $suite"
export PG_MAJOR="$major"
export ROUTER_IMAGE=pgshard-router:e2e
export OPERATOR_IMAGE=pgshard-operator:e2e
export ADMIN_IMAGE=pgshard-admin:e2e
export CONTROLLER_IMAGE=pgshard-controller:e2e
exec go test -tags e2e -count=1 -v "${args[@]}"
