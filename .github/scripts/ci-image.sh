#!/usr/bin/env bash
# Make an image that ci-images.lock pins available to the runner's own Docker
# daemon and print the reference the job should use, or -- given
# `--assert-container` -- answer whether a container already running is running
# the image the lock pins.
#
# Retrying `docker pull` is not a defence against Docker Hub: the outages CI
# has lost jobs to lasted minutes, and no affordable number of attempts
# outlasts one. What removes the registry from the steady-state path is the
# archive under the store below, which the workflow caches under a key derived
# from the lock file. For the images that file names, a run with a warm cache
# opens no connection to a registry; the pull is the cold-start path and the
# fallback when an archive cannot be used, never the normal one.
#
# That is the whole of what this covers, and it is worth being exact about the
# rest rather than letting the workflow read as though Docker Hub were gone.
# The base images `deploy/images/*.Dockerfile` name are fetched by BuildKit
# during `make images` and are not served from here. A KIND node pulls through
# its own containerd, which an archive cannot seed for a reference named by
# repository digest. Both still reach a registry on every run.
#
# The reference printed is the lock entry without its digest, because a tag is
# the only thing `docker load` can restore: an archive carries no repository
# digest, so `docker run repo@sha256:...` would go back to the registry for an
# image already sitting in the store.
#
# A tag is also why nothing here trusts a name. The cache is unsigned input
# that anything able to write this branch's cache can replace, and an image
# left under the same tag by an earlier step is no better attested, so the
# question `is this image the one the lock names?` has to be answered against
# something the cache does not supply. That is the lock's third column, the
# image id: the digest of the image config, which `docker load` recomputes
# from the archive's own bytes. An archive holding anything else cannot pass.
#
# `--assert-container <name> <container>` answers the same question about
# something this script did not put there. Buildx boots its builder by pulling
# before it consults the local image store, and nothing available to a caller
# suppresses that pull, so what a job can rely on is not that the pull failed
# but that the container it ended up with is running the pinned image. The
# comparison is the same one: `docker container inspect` reports `.Image` as
# the image id, which is what the lock's third column carries.
set -euo pipefail

script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly script_directory
readonly lock="${script_directory}/ci-images.lock"
readonly store="${PGSHARD_CI_IMAGE_STORE:-${HOME}/.cache/pgshard-ci-images}"
readonly attempts=5

usage() {
  echo "usage: ${0##*/} <image-name>" >&2
  echo "       ${0##*/} --assert-container <image-name> <container>" >&2
  exit 2
}

case "${1-}" in
  --assert-container)
    (( $# == 3 )) || usage
    readonly mode=assert name="$2" container="$3"
    ;;
  '' | -*)
    usage
    ;;
  *)
    (( $# == 1 )) || usage
    readonly mode=serve name="$1" container=
    ;;
esac

if ! entry="$(
  awk -v name="$name" '$1 == name { print $2, $3; found = 1 } END { exit !found }' "$lock"
)"; then
  echo "${lock} pins no image named ${name}" >&2
  exit 1
fi
readonly entry

read -r reference identity <<<"$entry"
readonly reference identity
if [[ -z "$reference" || -z "$identity" ]]; then
  echo "${lock} pins ${name} without both a reference and an image id" >&2
  exit 1
fi

if [[ "$mode" == assert ]]; then
  if ! running="$(docker container inspect --format '{{.Image}}' "$container")"; then
    echo "no container named ${container} to check against ${lock}" >&2
    exit 1
  fi
  readonly running
  if [[ "$running" != "$identity" ]]; then
    echo "${container} runs ${running}, but ${lock} names ${identity} for ${name}" >&2
    exit 1
  fi
  exit 0
fi

readonly local_reference="${reference%@*}"
readonly archive="${store}/${name}.tar"

image_identifier() {
  docker image inspect --format '{{.Id}}' "$1" 2>/dev/null
}

is_the_locked_image() {
  [[ "$(image_identifier "$local_reference")" == "$identity" ]]
}

restore_from_archive() {
  [[ -s "$archive" ]] || return 1
  docker load --input "$archive" >&2 || return 1
  is_the_locked_image
}

pull_from_registry() {
  local attempt
  for (( attempt = 1; attempt <= attempts; attempt++ )); do
    if docker pull "$reference" >&2; then
      docker tag "$reference" "$local_reference" >&2
      return 0
    fi
    if (( attempt < attempts )); then
      sleep $(( attempt * 10 ))
    fi
  done
  echo "failed to pull ${reference} after ${attempts} attempts" >&2
  return 1
}

archive_for_reuse() {
  mkdir -p "$store"
  docker save --output "${archive}.partial" "$local_reference" >&2 || return 1
  mv "${archive}.partial" "$archive"
}

if ! is_the_locked_image; then
  if ! restore_from_archive; then
    pull_from_registry
    # An image that cannot be archived still runs this job and only costs the
    # next one a pull, so this is announced rather than fatal: failing here would
    # turn a full cache or an unwritable store into a red build.
    archive_for_reuse ||
      echo "::warning::could not archive ${local_reference} for reuse" >&2
  fi
fi

# Only a pull of the pinned repository digest reaches here without having
# already matched, so a mismatch now means the entry's two digests name
# different images. That is a fault in the lock rather than a flake, and it is
# fatal because every check above is only worth what the id is worth.
if ! is_the_locked_image; then
  echo "${local_reference} is $(image_identifier "$local_reference"), but ${lock} names ${identity} for ${name}" >&2
  exit 1
fi

printf '%s\n' "$local_reference"
