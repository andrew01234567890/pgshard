#!/usr/bin/env bash
# Compares benchmarks between a base git ref and the working tree.
# Usage: hack/perf/benchstat.sh [BASE_REF] [OUT_DIR]
set -euo pipefail
base_ref="${1:-${PERF_BASE_REF:-origin/main}}"
out="${2:-${PERF_OUT_DIR:-perf-out}}"
count="${PERF_COUNT:-10}"
threshold="${PERF_THRESHOLD_PCT:-20}"
pkgs="${PERF_PKGS:-./test/perf/...}"
BENCHSTAT_VERSION="${BENCHSTAT_VERSION:-v0.0.0-20260813145340-fd4a688df892}"

root="$(git rev-parse --show-toplevel)"
mkdir -p "$out"
out="$(cd "$out" && pwd)"

go install "golang.org/x/perf/cmd/benchstat@${BENCHSTAT_VERSION}"
benchstat="$(go env GOPATH)/bin/benchstat"

run_bench() {
  if ! (cd "$1" && go list $pkgs >/dev/null 2>&1); then
    echo "no benchmark packages at $1; treating as empty" >&2
    return 0
  fi
  (cd "$1" && go test -run '^$' -bench . -benchmem -count "$count" $pkgs)
}

base_dir="$(mktemp -d)"
trap 'git -C "$root" worktree remove --force "$base_dir" >/dev/null 2>&1 || true' EXIT
git -C "$root" worktree add --detach "$base_dir" "$base_ref" >/dev/null
run_bench "$base_dir" > "$out/base.txt"
run_bench "$root" > "$out/head.txt"

"$benchstat" -format csv "$out/base.txt" "$out/head.txt" > "$out/compare.csv"
"$benchstat" "$out/base.txt" "$out/head.txt" > "$out/compare.txt"

{
  echo "## Benchmark comparison (base: \`$base_ref\`, count=$count)"
  echo
  echo '```'
  cat "$out/compare.txt"
  echo '```'
} > "$out/summary.md"

# CSV rows: name, base-center, base-ci, head-center, head-ci, delta, p-value, ...
# A "~" delta means no statistically significant change.
fail=0
while IFS=, read -r name _ _ _ _ delta pval _; do
  [ -z "$name" ] && continue
  case "$name" in name*|goos*|goarch*|pkg*|cpu*|geomean*|"") continue ;; esac
  case "$delta" in ~|"") continue ;; esac
  d="${delta//[+%\"]/}"
  if awk -v d="$d" -v t="$threshold" 'BEGIN{exit !(d+0 > t+0)}'; then
    echo "REGRESSION: $name $delta ($pval)" >> "$out/summary.md"
    fail=1
  fi
done < "$out/compare.csv"

cat "$out/summary.md"
exit "$fail"
