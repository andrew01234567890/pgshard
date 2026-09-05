#!/usr/bin/env bash
# Checks hack/ci/mirror-args.sh emits usable overrides. A malformed one is
# not an error buildx reports: --build-context whose name matches no FROM is
# accepted and ignored, so the build silently goes to Docker Hub and the
# mirror stops being load-bearing without anything saying so.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

mapfile -t out < <(hack/ci/mirror-args.sh)
[ "${#out[@]}" -gt 0 ] || { echo "mirror-args: emitted nothing"; exit 1; }
[ $(( ${#out[@]} % 2 )) -eq 0 ] || { echo "mirror-args: odd number of lines; each flag needs its value"; exit 1; }

froms=$(git grep -hoE '^[[:space:]]*FROM [^ ]+@sha256:[0-9a-f]{64}' -- '*Dockerfile' '*Dockerfile.*' | awk '{print $2}' | sort -u)

i=0
while [ "$i" -lt "${#out[@]}" ]; do
  flag="${out[$i]}"
  value="${out[$((i+1))]}"
  i=$((i+2))
  [ "$flag" = "--build-context" ] || { echo "mirror-args: expected --build-context, got $flag"; exit 1; }
  name="${value%%=*}"
  target="${value#*=}"
  # The name has to be a reference a FROM actually uses, or the override is
  # accepted and does nothing.
  printf '%s\n' "$froms" | grep -qxF "$name" || { echo "mirror-args: $name matches no FROM in any Dockerfile"; exit 1; }
  # Same digest on both sides: the override may change the host, never the
  # bytes.
  case "$target" in
    docker-image://*@"${name#*@}") ;;
    *) echo "mirror-args: $name is redirected to $target, which is not the same digest"; exit 1 ;;
  esac
done

# kind pulls its node image itself, so an override for it would be accepted
# and never used. It must not be emitted.
if printf '%s\n' "${out[@]}" | grep -q 'kindest/'; then
  echo "mirror-args: emitted an override for a kind node image, which buildx never sees"
  exit 1
fi

# An override for a digest the mirror does not have fails the build; it does
# not fall back. So the presence filter is the fallback, and it has to hold
# for the case that matters: a base-image bump, whose new digest the mirror
# has never seen because the mirror is filled on push to main.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

: > "$tmp/none"
mapfile -t filtered < <(PGSHARD_MIRROR_PRESENT="$tmp/none" hack/ci/mirror-args.sh)
[ "${#filtered[@]}" -eq 0 ] || {
  echo "mirror-args: an empty mirror still produced ${#filtered[@]} line(s); the build would resolve a digest that is not there"
  exit 1
}

# One digest present, the rest absent: exactly that one is overridden.
one="${out[1]%%=*}"
one="${one#*@}"
printf '%s\n' "$one" > "$tmp/one"
mapfile -t filtered < <(PGSHARD_MIRROR_PRESENT="$tmp/one" hack/ci/mirror-args.sh)
[ "${#filtered[@]}" -eq 2 ] || { echo "mirror-args: expected 1 override for a mirror holding 1 digest, got $(( ${#filtered[@]} / 2 ))"; exit 1; }
[ "${filtered[1]}" = "${out[1]}" ] || { echo "mirror-args: filtered to ${filtered[1]}, wanted ${out[1]}"; exit 1; }

# A file that is named but missing is a wiring mistake, and silently emitting
# every override would put the build back in the state this prevents.
if PGSHARD_MIRROR_PRESENT="$tmp/absent" hack/ci/mirror-args.sh >/dev/null 2>&1; then
  echo "mirror-args: a missing presence file was accepted"
  exit 1
fi

echo "mirror-args: OK ($(( ${#out[@]} / 2 )) override(s), presence filter honoured)"
