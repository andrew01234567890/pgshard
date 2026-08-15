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

# `go test -bench` emits one row per sample. Summarize ns/op by benchmark name so
# the candidate and baseline are compared using the same fixed runner settings.
extract() {
  awk '
    $1 ~ /^Benchmark[A-Za-z0-9_]+(-[0-9]+)?$/ {
      name = $1
      sub(/-[0-9]+$/, "", name)
      for (i = 2; i <= NF; i++) {
        if ($i == "ns/op" && i > 2) {
          samples[name]++
          total[name] += $(i - 1)
          break
        }
      }
    }
    END {
      for (name in samples) {
        if (samples[name] > 0) printf "%s\t%.6f\n", name, total[name] / samples[name]
      }
    }
  ' "$1" | sort
}

base_metrics=$(mktemp)
candidate_metrics=$(mktemp)
trap 'rm -f "$base_metrics" "$candidate_metrics"' EXIT
extract "$base_output" >"$base_metrics"
extract "$candidate_output" >"$candidate_metrics"

if [[ ! -s "$base_metrics" || ! -s "$candidate_metrics" ]]; then
  printf 'benchmark output contained no comparable ns/op rows; no baseline available\n' >&2
  exit 1
fi

printf 'benchmark comparison (average ns/op across fixed samples; lower is better)\n'
printf '%-48s %14s %14s %10s\n' benchmark base candidate ratio
join -t $'\t' -j 1 "$base_metrics" "$candidate_metrics" |
  awk -F '\t' '{ ratio = $3 / $2; printf "%-48s %14.3f %14.3f %9.3fx\n", $1, $2, $3, ratio }'

if ! join -t $'\t' -j 1 "$base_metrics" "$candidate_metrics" | grep -q .; then
  printf 'candidate and baseline have no common benchmarks; no baseline available\n' >&2
  exit 1
fi
