#!/usr/bin/env bash
# Prints the buildx flags that make a build take its base images from our
# mirror instead of Docker Hub, one per line.
#
# --build-context overrides what a FROM resolves to without touching the
# Dockerfile, so the pins stay exactly where they are and a build with no
# flags -- a contributor's, or anyone without access to the mirror -- goes
# to Docker Hub as before. The override is by digest on both sides, so it
# cannot substitute different bytes: buildx would have to find that digest
# in the mirror, and the mirror workflow asserts the copy preserved it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mirror="${PGSHARD_BASE_MIRROR:-ghcr.io/andrew01234567890/pgshard-base}"

while IFS=$'\t' read -r ref _; do
  # kind pulls its node image itself rather than through buildx, so a
  # build-context override would not reach it. It is mirrored; using the
  # mirror there means changing the cluster config, which is separate work.
  case "$ref" in kindest/*) continue ;; esac
  printf -- '--build-context\n%s=docker-image://%s@%s\n' "$ref" "$mirror" "${ref#*@}"
done < <("$root/hack/ci/base-images.sh")
