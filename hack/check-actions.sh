#!/usr/bin/env bash
set -euo pipefail

# Apply conservative textual guardrails; actionlint is the authoritative YAML check.
# Usage: hack/check-actions.sh [workflow-directory]

workflow_dir=${1:-.github/workflows}
if [[ ! -d "$workflow_dir" ]]; then
  printf 'workflow directory not found: %s\n' "$workflow_dir" >&2
  exit 1
fi

shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if ((${#workflows[@]} == 0)); then
  printf 'no workflow files found in %s\n' "$workflow_dir" >&2
  exit 1
fi

failed=0
for workflow in "${workflows[@]}"; do
  if grep -nF 'pull_request_target' "$workflow"; then
    printf 'forbidden pull_request_target event in %s\n' "$workflow" >&2
    failed=1
  fi

  if ! grep -qE '^permissions:[[:space:]]*$' "$workflow"; then
    printf 'missing top-level least-privilege permissions in %s\n' "$workflow" >&2
    failed=1
  fi

  if grep -nE '^[[:space:]]*permissions:[[:space:]]*(write|write-all|read-all)[[:space:]]*$' "$workflow"; then
    printf 'broad permissions in %s\n' "$workflow" >&2
    failed=1
  fi

  if grep -nE '^[[:space:]]*(contents|actions|pull-requests|issues|packages|id-token):[[:space:]]*write[[:space:]]*$' "$workflow"; then
    printf 'unnecessary write permission in %s\n' "$workflow" >&2
    failed=1
  fi

  if grep -nE '\$\{\{[[:space:]]*secrets' "$workflow"; then
    printf 'workflow references repository secrets; PR workflows must run without secrets: %s\n' "$workflow" >&2
    failed=1
  fi

  if grep -nE 'github\.event\.pull_request\.title' "$workflow"; then
    printf 'workflow interpolates an untrusted pull-request title: %s\n' "$workflow" >&2
    failed=1
  fi

  while IFS= read -r line; do
    line_number=${line%%:*}
    text=${line#*:}
    if [[ "$text" =~ uses:[[:space:]]*\./ ]]; then
      continue
    fi
    if [[ ! "$text" =~ uses:[[:space:]]*[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)?@[0-9a-fA-F]{40}[[:space:]]+#.+ ]]; then
      printf '%s:%s: every third-party action must use a full SHA and adjacent version comment\n' "$workflow" "$line_number" >&2
      failed=1
    fi
  done < <(grep -nE '^[[:space:]]*(-[[:space:]]*)?uses:[[:space:]]*[^#[:space:]]+' "$workflow" || true)
done

if ((failed)); then
  exit 1
fi
printf 'workflow safety checks passed (%d files)\n' "${#workflows[@]}"
