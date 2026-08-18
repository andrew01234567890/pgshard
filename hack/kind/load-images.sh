#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -eq 0 ]; then
  echo "usage: $0 IMAGE [IMAGE...]" >&2
  exit 2
fi
for img in "$@"; do
  kind load docker-image --name "${KIND_CLUSTER_NAME:-pgshard-e2e}" "$img"
done
