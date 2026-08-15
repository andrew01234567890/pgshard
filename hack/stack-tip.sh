#!/usr/bin/env bash
set -euo pipefail

if (($# != 2)); then
  printf 'usage: %s EXPECTED_TIP_SHA EXPECTED_BASELINE_SHA\n' "$0" >&2
  exit 2
fi

expected_tip=$1
expected_baseline=$2
if [[ ! "$expected_tip" =~ ^[0-9a-fA-F]{40}$ ]]; then
  printf 'expected tip SHA must be exactly 40 hexadecimal characters: %s\n' "$expected_tip" >&2
  exit 1
fi
if [[ ! "$expected_baseline" =~ ^[0-9a-fA-F]{40}$ ]]; then
  printf 'expected baseline SHA must be exactly 40 hexadecimal characters: %s\n' "$expected_baseline" >&2
  exit 1
fi

actual_tip=$(git rev-parse HEAD)
if [[ "$actual_tip" != "$expected_tip" ]]; then
  printf 'expected tip SHA mismatch: input=%s checked-out=%s\n' "$expected_tip" "$actual_tip" >&2
  exit 1
fi

resolved_tip=$(git rev-parse --verify "${expected_tip}^{commit}") || {
  printf 'expected tip does not resolve to an existing commit: %s\n' "$expected_tip" >&2
  exit 1
}
if [[ "$resolved_tip" != "$expected_tip" ]]; then
  printf 'expected tip does not resolve to the exact requested commit: %s\n' "$expected_tip" >&2
  exit 1
fi

base_sha=$(git rev-parse --verify "${expected_baseline}^{commit}") || {
  printf 'expected baseline does not resolve to an existing commit: %s\n' "$expected_baseline" >&2
  exit 1
}
if [[ "$base_sha" != "$expected_baseline" ]]; then
  printf 'expected baseline does not resolve to the exact requested commit: %s\n' "$expected_baseline" >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$base_sha" "$expected_tip"; then
  printf 'expected tip %s is not descended from baseline %s; refusing comparison\n' "$expected_tip" "$base_sha" >&2
  exit 1
fi

repo_dir=$(git rev-parse --show-toplevel)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
candidate_dir="$tmp_dir/candidate"
base_dir="$tmp_dir/base"
mkdir -p "$candidate_dir" "$base_dir"
git archive "$expected_tip" | tar -x -C "$candidate_dir"
git archive "$base_sha" | tar -x -C "$base_dir"

has_benchmarks() {
  "$repo_dir/hack/has-benchmarks.sh" "$1"
}
if has_benchmarks "$candidate_dir"; then
  :
else
  benchmark_status=$?
  if ((benchmark_status > 1)); then
    printf 'unable to determine whether candidate %s contains Go benchmarks\n' "$expected_tip" >&2
    exit 1
  fi
  printf 'no Go benchmarks found at candidate %s; no baseline available for comparison\n' "$expected_tip" >&2
  exit 1
fi
if has_benchmarks "$base_dir"; then
  :
else
  benchmark_status=$?
  if ((benchmark_status > 1)); then
    printf 'unable to determine whether baseline %s contains Go benchmarks\n' "$base_sha" >&2
    exit 1
  fi
  printf 'no Go benchmark baseline found at %s; refusing to claim performance\n' "$base_sha" >&2
  exit 1
fi

run_benchmarks() {
  local directory=$1
  local output=$2
  (
    cd "$directory"
    GOMAXPROCS=1 go test -run '^$' -bench '^Benchmark' -benchmem -count=5 -benchtime=1s -cpu=1 ./...
  ) >"$output"
}

base_output="$tmp_dir/base.txt"
candidate_output="$tmp_dir/candidate.txt"
printf 'running deterministic baseline benchmarks from %s\n' "$base_sha"
run_benchmarks "$base_dir" "$base_output"
printf 'running deterministic candidate benchmarks from %s\n' "$expected_tip"
run_benchmarks "$candidate_dir" "$candidate_output"
"$repo_dir/hack/compare-benchmarks.sh" "$base_output" "$candidate_output"

printf 'KIND validation and production performance gates are intentionally not part of this workflow yet.\n'
