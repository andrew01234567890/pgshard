#!/usr/bin/env bash
# Loads registry images into the kind cluster: pulled here with retries,
# saved for the node's platform and loaded as an archive, so kubelet finds
# them present and never pulls. A registry that answers one pull with a 502
# otherwise leaves a suite waiting on a pod that never starts (PGS-842).
#
# Not `kind load docker-image`: under Docker's containerd image store it
# cannot load an image pulled from a multi-platform index ("ctr: content
# digest ... not found"), and says so only in its output.
set -euo pipefail
if [ "$#" -eq 0 ]; then
  echo "usage: $0 IMAGE [IMAGE...]" >&2
  exit 2
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
platform="linux/$(docker version --format '{{.Server.Arch}}')"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
for img in "$@"; do
  "$root/hack/ci/retry.sh" -n 5 -d 30 -- docker pull --platform "$platform" "$img"
  docker save --platform "$platform" -o "$tmp/image.tar" "$img"
  kind load image-archive --name "${KIND_CLUSTER_NAME:-pgshard-e2e}" "$tmp/image.tar"
  rm -f "$tmp/image.tar"
done
