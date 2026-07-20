#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

audit_mode=false
if [[ "${1:-}" == "--audit" ]]; then
  audit_mode=true
  shift
fi

head_sha="${1:?usage: ci-diff-base.sh [--audit] HEAD_SHA [PUSH_BEFORE_SHA]}"
push_before_sha="${2:-}"
github_repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

git rev-parse --verify "${head_sha}^{commit}" >/dev/null
head_sha="$(git rev-parse "${head_sha}^{commit}")"

released_semver_tag_at() {
  local commit="$1"
  local release_sha
  local tag
  while IFS= read -r tag; do
    if ! canonical_semver_tag "$tag"; then
      continue
    fi
    if release_sha="$(
      gh release view "$tag" --repo "$github_repository" \
        --json targetCommitish --jq .targetCommitish 2>/dev/null
    )" && [[ "$release_sha" == "$commit" ]]; then
      return 0
    fi
  done < <(git tag --points-at "$commit")
  return 1
}

canonical_semver_tag() {
  local tag="$1"
  local component
  local max_u64="18446744073709551615"
  if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    return 1
  fi
  for component in "${BASH_REMATCH[@]:1}"; do
    if [[ ${#component} -gt ${#max_u64} ]] \
      || { [[ ${#component} -eq ${#max_u64} ]] && [[ "$component" > "$max_u64" ]]; }; then
      return 1
    fi
  done
}

first_parent_contains() {
  local candidate="$1"
  git rev-list --first-parent "$head_sha" \
    | awk -v candidate="$candidate" '$0 == candidate { found=1 } END { exit !found }'
}

# The ordinary single-commit path remains cheap when the push predecessor is
# already released. An untagged predecessor means an earlier release failed or
# has not run, so component detection must cover the complete unreleased gap.
if [[ -n "$push_before_sha" && ! "$push_before_sha" =~ ^0+$ ]] \
  && git cat-file -e "${push_before_sha}^{commit}" 2>/dev/null; then
  push_before_sha="$(git rev-parse "${push_before_sha}^{commit}")"
  if first_parent_contains "$push_before_sha" && released_semver_tag_at "$push_before_sha"; then
    printf '%s\n' "$push_before_sha"
    exit 0
  fi
fi

while IFS= read -r commit; do
  if released_semver_tag_at "$commit"; then
    printf '%s\n' "$commit"
    exit 0
  fi
done < <(git rev-list --first-parent --skip=1 "$head_sha")

# With no release tag, component detection uses an empty base to validate every
# tracked component. History auditing instead starts immediately before the
# release marker so every potentially releasable commit is scanned.
if [[ "$audit_mode" == true ]]; then
  while IFS= read -r commit; do
    if git cat-file -e "$commit:crates/pgshard-release/RELEASE_START" 2>/dev/null; then
      if git rev-parse "${commit}^" >/dev/null 2>&1; then
        git rev-parse "${commit}^"
        exit 0
      fi
      printf 'release marker commit %s has no auditable predecessor\n' "$commit" >&2
      exit 1
    fi
  done < <(git rev-list --first-parent --reverse "$head_sha")
  printf 'release marker is absent from first-parent history\n' >&2
  exit 1
fi

exit 0
