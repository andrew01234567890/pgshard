#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
"$repo_dir/hack/check-actions.sh" "$repo_dir/.github/workflows"

if ! command -v actionlint >/dev/null 2>&1; then
  printf 'actionlint is required; install the pinned CI version before running test-ci.sh\n' >&2
  exit 1
fi
actionlint -color=false "$repo_dir/.github/workflows"/*.yml

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

assert_rejected() {
  fixture_name=$1
  fixture_body=$2
  fixture_dir="$tmp_dir/$fixture_name"
  mkdir -p "$fixture_dir"
  printf '%s\n' "$fixture_body" >"$fixture_dir/bad.yml"
  if "$repo_dir/hack/check-actions.sh" "$fixture_dir" >/dev/null 2>&1; then
    printf 'check-actions.sh accepted fixture: %s\n' "$fixture_name" >&2
    exit 1
  fi
}

assert_rejected pull-request-target $'name: bad\non: {pull_request_target: null}\npermissions:\n  contents: read\njobs:\n  bad:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true'
assert_rejected write-all $'name: bad\non: push\npermissions:\n  contents: read\njobs:\n  bad:\n    permissions: write-all\n    runs-on: ubuntu-latest\n    steps:\n      - run: true'
assert_rejected bracket-secret $'name: bad\non: pull_request\npermissions:\n  contents: read\njobs:\n  bad:\n    runs-on: ubuntu-latest\n    steps:\n      - env:\n          TOKEN: ${{ secrets[\x27NPM_TOKEN\x27] }}\n        run: true'
assert_rejected pull-request-title $'name: bad\non: pull_request\npermissions:\n  contents: read\njobs:\n  bad:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo "${{ github.event.pull_request.title }}"'

tip_sha=$(git -C "$repo_dir" rev-parse HEAD)
if "$repo_dir/hack/stack-tip.sh" "$tip_sha" invalid-baseline refs/remotes/origin/main pull-request >/dev/null 2>&1; then
  printf 'stack-tip.sh accepted a malformed baseline SHA\n' >&2
  exit 1
fi
if "$repo_dir/hack/stack-tip.sh" 0000000000000000000000000000000000000000 "$tip_sha" refs/remotes/origin/main pull-request >/dev/null 2>&1; then
  printf 'stack-tip.sh accepted a mismatched tip SHA\n' >&2
  exit 1
fi
dispatch_repo="$tmp_dir/dispatch-repo"
git init --quiet "$dispatch_repo"
git -C "$dispatch_repo" config user.name fixture
git -C "$dispatch_repo" config user.email fixture@example.invalid
printf 'default main\n' >"$dispatch_repo/state.txt"
git -C "$dispatch_repo" add state.txt
git -C "$dispatch_repo" commit --quiet -m default-main
dispatch_default_sha=$(git -C "$dispatch_repo" rev-parse HEAD)
printf 'candidate\n' >"$dispatch_repo/state.txt"
git -C "$dispatch_repo" commit --quiet -am candidate
dispatch_tip_sha=$(git -C "$dispatch_repo" rev-parse HEAD)
if (cd "$dispatch_repo" && "$repo_dir/hack/stack-tip.sh" "$dispatch_tip_sha" "$dispatch_tip_sha" "$dispatch_default_sha" dispatch) >/dev/null 2>&1; then
  printf 'stack-tip.sh accepted a dispatch baseline absent from default head\n' >&2
  exit 1
fi

printf 'pkg: example/one\nBenchmarkShared 5 100.0 ns/op 8 B/op 1 allocs/op\npkg: example/two\nBenchmarkShared 5 200.0 ns/op 8 B/op 1 allocs/op\n' >"$tmp_dir/base-bench.txt"
printf 'pkg: example/one\nBenchmarkShared 5 125.0 ns/op 8 B/op 1 allocs/op\npkg: example/two\nBenchmarkShared 5 225.0 ns/op 8 B/op 1 allocs/op\n' >"$tmp_dir/candidate-bench.txt"
if ! "$repo_dir/hack/compare-benchmarks.sh" "$tmp_dir/base-bench.txt" "$tmp_dir/candidate-bench.txt" >/dev/null; then
  printf 'compare-benchmarks.sh rejected a valid benchmark fixture\n' >&2
  exit 1
fi
if [[ "$("$repo_dir/hack/compare-benchmarks.sh" "$tmp_dir/base-bench.txt" "$tmp_dir/candidate-bench.txt")" != *'example/one::BenchmarkShared'* || "$("$repo_dir/hack/compare-benchmarks.sh" "$tmp_dir/base-bench.txt" "$tmp_dir/candidate-bench.txt")" != *'example/two::BenchmarkShared'* ]]; then
  printf 'compare-benchmarks.sh did not preserve package-qualified benchmark keys\n' >&2
  exit 1
fi
printf 'pkg: example/one\nBenchmarkShared 5 100.0 ns/op 8 B/op 1 allocs/op\n' >"$tmp_dir/unmatched-candidate.txt"
if "$repo_dir/hack/compare-benchmarks.sh" "$tmp_dir/base-bench.txt" "$tmp_dir/unmatched-candidate.txt" >/dev/null 2>&1; then
  printf 'compare-benchmarks.sh accepted unmatched benchmark sets\n' >&2
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
