#!/usr/bin/env bash
# Compares benchmarks between a base git ref and the working tree.
# Usage: hack/perf/benchstat.sh [BASE_REF] [OUT_DIR]
set -euo pipefail
base_ref="${1:-${PERF_BASE_REF:-origin/main}}"
out="${2:-${PERF_OUT_DIR:-perf-out}}"
count="${PERF_COUNT:-10}"
# The gate watched ./test/perf/... alone, which is 14 benchmarks -- while
# 12 more live beside the code they measure, in the router, the pooler,
# the scatter merge, the VStream vector and the snapshot. Those are the
# hot paths a regression actually shows up on: BenchmarkMigrating's own
# comment says it measures the check the router makes on every write, and
# a regression there was invisible to the only gate watching for one.
#
# Named explicitly rather than ./internal/... because a blanket sweep
# builds every internal package's test binary, and some need envtest or
# Docker at TestMain even with -run '^$'.
pkgs="${PERF_PKGS:-./test/perf/... ./internal/catalog/snapshot/... ./internal/pooler/... ./internal/router/...}"
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

fail=0
"$root/hack/perf/gate.sh" "$out/compare.csv" >> "$out/summary.md" || fail=1

cat "$out/summary.md"
exit "$fail"
