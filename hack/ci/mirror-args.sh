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
#
# An override for a digest the mirror does not have is NOT a fallback: buildx
# resolves it, gets "not found", and the build fails. That is the state every
# base-image bump is in, because the mirror is filled on push to main and the
# bump is still a pull request -- so PGSHARD_MIRROR_PRESENT names the file
# listing the digests the mirror was observed to serve, and a ref outside it
# gets no override and goes to Docker Hub, which is what a fallback means.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mirror="${PGSHARD_BASE_MIRROR:-ghcr.io/andrew01234567890/pgshard-base}"
present="${PGSHARD_MIRROR_PRESENT:-}"

if [ -n "$present" ] && [ ! -f "$present" ]; then
  echo "mirror-args: PGSHARD_MIRROR_PRESENT=$present does not exist" >&2
  exit 1
fi

while IFS=$'\t' read -r ref _; do
  # kind pulls its node image itself rather than through buildx, so a
  # build-context override would not reach it. It is mirrored; using the
  # mirror there means changing the cluster config, which is separate work.
  case "$ref" in kindest/*) continue ;; esac
  digest="${ref#*@}"
  if [ -n "$present" ] && ! grep -qxF "$digest" "$present"; then
    continue
  fi
  printf -- '--build-context\n%s=docker-image://%s@%s\n' "$ref" "$mirror" "$digest"
done < <("$root/hack/ci/base-images.sh")
