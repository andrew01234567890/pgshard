#!/usr/bin/env bash
# Evaluates a benchstat CSV comparison and prints REGRESSION lines for gated
# benchmarks. Exit 1 if any regression is found.
#
# A benchmark regresses only when ALL of the following hold:
#   - its name matches PERF_GATE_PATTERN (default: Gate), so only benchmarks
#     explicitly tagged as gates are enforced;
#   - benchstat reports a statistically significant sec/op change (not "~");
#   - the base is at least PERF_MIN_BASE_NS ns/op (default 100), excluding
#     trivial benchmarks whose noise dominates;
#   - the relative increase exceeds PERF_THRESHOLD_PCT (default 20);
#   - the absolute increase is at least PERF_MIN_ABS_NS ns/op (default 50).
# Usage: hack/perf/gate.sh compare.csv
set -euo pipefail
csv="$1"
pattern="${PERF_GATE_PATTERN:-Gate}"
threshold="${PERF_THRESHOLD_PCT:-20}"
min_base_ns="${PERF_MIN_BASE_NS:-100}"
min_abs_ns="${PERF_MIN_ABS_NS:-50}"

awk -F, -v pattern="$pattern" -v threshold="$threshold" -v min_base="$min_base_ns" -v min_abs="$min_abs_ns" '
  $1 == "" && $2 != "" { metric = $2; next }
  $1 == "" || $1 ~ /^(goos|goarch|pkg|cpu|geomean)/ { next }
  metric != "sec/op" { next }
  $1 !~ pattern { next }
  { matched++ }
  $6 == "~" || $6 == "" { next }
  {
    base_ns = $2 * 1e9; head_ns = $4 * 1e9
    pct = $6; gsub(/[+%]/, "", pct); pct += 0
    abs = head_ns - base_ns
    if (base_ns >= min_base && pct > threshold && abs >= min_abs) {
      printf "REGRESSION: %s %s (%s, +%.0fns/op)\n", $1, $6, $7, abs
      fail = 1
    }
  }
  END {
    if (!matched) { printf "PERF GATE: no benchmark matched pattern %s; the gate guards nothing\n", pattern; exit 1 }
    exit fail
  }
' "$csv"
