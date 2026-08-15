#!/usr/bin/env bash
set -euo pipefail

if (($# != 2)); then
  printf 'usage: %s BASE-OUTPUT CANDIDATE-OUTPUT\n' "$0" >&2
  exit 2
fi

base_output=$1
candidate_output=$2
for output in "$base_output" "$candidate_output"; do
  [[ -s "$output" ]] || { printf 'benchmark output is empty: %s\n' "$output" >&2; exit 1; }
done

# `go test -bench` emits one row per sample. Keep the package in each key so
# identically named benchmarks from different packages cannot be conflated.
extract() {
  awk '
    $1 == "pkg:" { package = $2; next }
    $1 ~ /^Benchmark[A-Za-z0-9_]+(-[0-9]+)?$/ {
      if (package == "") {
        invalid = 1
        next
      }
      name = $1
      sub(/-[0-9]+$/, "", name)
      key = package "::" name
      for (i = 2; i <= NF; i++) {
        if ($i == "ns/op" && i > 2) {
          samples[key]++
          total[key] += $(i - 1)
          break
        }
      }
    }
    END {
      if (invalid) exit 2
      for (key in samples) {
        if (samples[key] > 0) printf "%s\t%.6f\n", key, total[key] / samples[key]
      }
    }
  ' "$1" | LC_ALL=C sort
}

base_metrics=$(mktemp)
candidate_metrics=$(mktemp)
base_keys=$(mktemp)
candidate_keys=$(mktemp)
trap 'rm -f "$base_metrics" "$candidate_metrics" "$base_keys" "$candidate_keys"' EXIT

if ! extract "$base_output" >"$base_metrics"; then
  printf 'unable to parse baseline benchmark output\n' >&2
  exit 1
fi
if ! extract "$candidate_output" >"$candidate_metrics"; then
  printf 'unable to parse candidate benchmark output\n' >&2
  exit 1
fi

if [[ ! -s "$base_metrics" || ! -s "$candidate_metrics" ]]; then
  printf 'benchmark output contained no comparable package-qualified ns/op rows; no baseline available\n' >&2
  exit 1
fi

cut -f1 "$base_metrics" >"$base_keys"
cut -f1 "$candidate_metrics" >"$candidate_keys"
if ! diff -u "$base_keys" "$candidate_keys"; then
  printf 'baseline and candidate benchmark sets differ; refusing comparison\n' >&2
  exit 1
fi

printf 'benchmark comparison (average ns/op across fixed samples; lower is better)\n'
printf '%-72s %14s %14s %10s\n' benchmark base candidate ratio
join -t $'\t' -j 1 "$base_metrics" "$candidate_metrics" |
  awk -F '\t' '{ ratio = $3 / $2; printf "%-72s %14.3f %14.3f %9.3fx\n", $1, $2, $3, ratio }'
