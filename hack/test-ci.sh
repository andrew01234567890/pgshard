#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
"$repo_dir/hack/check-actions.sh" "$repo_dir/.github/workflows"

if command -v actionlint >/dev/null 2>&1; then
  actionlint -color=false "$repo_dir/.github/workflows"/*.yml
else
  printf 'actionlint not installed; shell safety checks still ran\n'
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
cat >"$tmp_dir/bad.yml" <<'EOF'
name: bad
on: push
permissions:
  contents: read
jobs:
  bad:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
EOF
if "$repo_dir/hack/check-actions.sh" "$tmp_dir" >/dev/null 2>&1; then
  printf 'check-actions.sh accepted an unpinned action fixture\n' >&2
  exit 1
fi

tip_sha=$(git -C "$repo_dir" rev-parse HEAD)
if "$repo_dir/hack/stack-tip.sh" "$tip_sha" invalid-baseline >/dev/null 2>&1; then
  printf 'stack-tip.sh accepted a malformed baseline SHA\n' >&2
  exit 1
fi
if "$repo_dir/hack/stack-tip.sh" 0000000000000000000000000000000000000000 "$tip_sha" >/dev/null 2>&1; then
  printf 'stack-tip.sh accepted a mismatched tip SHA\n' >&2
  exit 1
fi

printf 'BenchmarkVersionText 5 100.0 ns/op 8 B/op 1 allocs/op\n' >"$tmp_dir/base-bench.txt"
printf 'BenchmarkVersionText 5 125.0 ns/op 8 B/op 1 allocs/op\n' >"$tmp_dir/candidate-bench.txt"
if ! "$repo_dir/hack/compare-benchmarks.sh" "$tmp_dir/base-bench.txt" "$tmp_dir/candidate-bench.txt" >/dev/null; then
  printf 'compare-benchmarks.sh rejected a valid benchmark fixture\n' >&2
  exit 1
fi

fixture_dir="$tmp_dir/benchmark-fixture"
mkdir -p "$fixture_dir"
printf 'package fixture\n\nfunc BenchmarkFixture(b *testing.B) {}\n' >"$fixture_dir/fixture_test.go"
mkdir -p "$tmp_dir/no-rg-bin"
printf '#!/usr/bin/env bash\nexit 99\n' >"$tmp_dir/no-rg-bin/rg"
chmod +x "$tmp_dir/no-rg-bin/rg"
if ! PATH="$tmp_dir/no-rg-bin:$PATH" "$repo_dir/hack/has-benchmarks.sh" "$fixture_dir"; then
  printf 'has-benchmarks.sh failed when an unavailable rg was shadowed\n' >&2
  exit 1
fi
printf '#!/usr/bin/env bash\nexit 99\n' >"$tmp_dir/no-rg-bin/find"
chmod +x "$tmp_dir/no-rg-bin/find"
set +e
PATH="$tmp_dir/no-rg-bin:$PATH" "$repo_dir/hack/has-benchmarks.sh" "$fixture_dir" >/dev/null 2>&1
benchmark_scan_status=$?
set -e
if [[ "$benchmark_scan_status" -ne 2 ]]; then
  printf 'has-benchmarks.sh did not distinguish find failure (status=%s)\n' "$benchmark_scan_status" >&2
  exit 1
fi

printf 'CI helper checks passed\n'
