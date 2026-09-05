#!/usr/bin/env bash
# Checks hack/ci/base-images.sh against a second, independent scan. The point
# is coverage: a base image that the mirror does not know about is pulled
# anonymously from Docker Hub at build time, which is the failure this whole
# mechanism exists to remove, and nothing else would report it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

out="$(hack/ci/base-images.sh)"
[ -n "$out" ] || { echo "base-images: emitted nothing"; exit 1; }

while IFS=$'\t' read -r ref slug; do
  case "$ref" in
    *@sha256:????????????????????????????????????????????????????????????????) ;;
    *) echo "base-images: $ref is not pinned to a full digest"; exit 1 ;;
  esac
  [ -n "$slug" ] || { echo "base-images: $ref has no mirror tag"; exit 1; }
  case "$slug" in
    *[!a-zA-Z0-9._-]*) echo "base-images: $slug is not a usable tag"; exit 1 ;;
  esac
  # The tag has to distinguish DIGESTS, not just names. The same name:tag is
  # pinned at two digests whenever a bump is split across pull requests --
  # dependabot raises one per directory -- and a tag derived from the name
  # alone made those collide, refused, and deadlocked both.
  digest="${ref#*@sha256:}"
  case "$slug" in
    *-"${digest:0:12}") ;;
    *) echo "base-images: $slug does not end in the digest of $ref, so two digests of one tag would collide"; exit 1 ;;
  esac
  case "$ref" in
    */*) case "${ref%%/*}" in
           *.*|*:*|localhost) echo "base-images: $ref is not on Docker Hub and should not be listed"; exit 1 ;;
         esac ;;
  esac
done <<< "$out"

if [ "$(printf '%s\n' "$out" | cut -f2 | sort | uniq -d)" != "" ]; then
  echo "base-images: two images share a mirror tag"; exit 1
fi

# The independent half: grep, rather than the script's own awk, for every
# digest-pinned reference in the tree, and require the Docker Hub ones to be
# listed. This is what fails when a Dockerfile is added.
missing=0
while read -r ref; do
  case "$ref" in
    */*) case "${ref%%/*}" in *.*|*:*|localhost) continue ;; esac ;;
  esac
  if ! printf '%s\n' "$out" | cut -f1 | grep -qxF "$ref"; then
    echo "base-images: $ref is pinned in the tree and not mirrored"
    missing=1
  fi
done < <(git grep -hoE '(FROM|image:) +[^ ]+@sha256:[0-9a-f]{64}' -- '*Dockerfile' '*Dockerfile.*' 'hack/kind/*.yaml' \
           | awk '{print $2}' | sort -u)
[ "$missing" = 0 ] || exit 1

echo "base-images: OK ($(printf '%s\n' "$out" | wc -l) image(s))"
