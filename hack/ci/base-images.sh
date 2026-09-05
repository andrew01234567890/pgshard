#!/usr/bin/env bash
# Prints every Docker Hub image this repository pins by digest, one per line,
# as "<source ref><TAB><mirror tag>". The mirror tag names the same bytes in
# our own registry; the digest is what makes them the same bytes, so a
# consumer references <mirror repo>@<digest> and gets exactly what the
# Dockerfile asked for, from a registry that does not rate-limit us by IP.
#
# Only Docker Hub is listed. gcr.io is not the host that takes e2e cells down,
# and mirroring an image nobody is throttled on is upkeep for nothing.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The daemon decides a reference is a registry host when the first path
# segment contains a dot or a colon, or is "localhost"; anything else is
# Docker Hub, whether it is an official image ("golang:...") or a user one
# ("kindest/node:...").
is_docker_hub() {
  local first="${1%%/*}"
  [ "$first" = "$1" ] && return 0
  case "$first" in
    localhost|*.*|*:*) [ "$first" = "docker.io" ] ;;
    *) return 0 ;;
  esac
}

while read -r ref; do
  is_docker_hub "$ref" || continue
  slug="${ref%@*}"
  slug="${slug#docker.io/}"
  slug="${slug#library/}"
  slug="${slug//\//-}"
  slug="${slug//:/-}"
  # The digest is part of the tag, because the same name:tag can appear at
  # two digests at once and did: dependabot raises one pull request per
  # directory, so a golang bump lands in Dockerfile.control and
  # Dockerfile.router in one and in postgres/Dockerfile in another, and
  # between them the tree pins golang:1.27-bookworm at both. Deriving the
  # tag from name:tag alone made those two collide, the check refused
  # ("two images share a mirror tag"), and NEITHER pull request could
  # merge -- each was blocked by the state the other would resolve.
  digest="${ref#*@sha256:}"
  printf '%s\t%s\n' "$ref" "$slug-${digest:0:12}"
done < <(
  # The files are discovered, not listed. A hardcoded list is how the e2e
  # cache-warming step came to cover two of the three Dockerfiles: the third
  # was added later and nothing said so.
  cd "$root"
  {
    git ls-files -z '*Dockerfile' '*Dockerfile.*' 'hack/kind/*.yaml' \
      | xargs -0 awk '/^[[:space:]]*FROM .*@sha256:/ {print $2}
                      /^[[:space:]]*image: .*@sha256:/ {print $2}'
  } | sort -u
)
