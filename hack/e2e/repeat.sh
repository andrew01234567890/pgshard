#!/usr/bin/env bash
# Runs one e2e cell N times on a ref and reports the pass rate.
#
# A test is not flaky until a rate says so. repeat.yml, which exists for
# exactly that, never stands up kind and none of its targets reaches
# test/e2e, so before this the only way to re-run an e2e cell was to push
# and let pull_request fire the whole matrix once (PGS-952).
#
#   hack/e2e/repeat.sh <suite> <pg> <runs> [ref]
#   hack/e2e/repeat.sh operator 18 50 main
set -euo pipefail

suite="${1:?usage: repeat.sh <suite> <pg> <runs> [ref]}"
pg="${2:?usage: repeat.sh <suite> <pg> <runs> [ref]}"
runs="${3:?usage: repeat.sh <suite> <pg> <runs> [ref]}"
ref="${4:-main}"
wf="e2e-kind.yml"

echo "dispatching $runs run(s) of e2e (pg$pg, $suite) on $ref"
ids=()
for i in $(seq 1 "$runs"); do
	before="$(gh run list --workflow "$wf" --branch "$ref" --event workflow_dispatch --limit 1 --json databaseId --jq '.[0].databaseId // 0')"
	gh workflow run "$wf" --ref "$ref" -f "suite=$suite" -f "pg=$pg" >/dev/null
	# gh does not report the run it just started, so wait for a new id.
	id="$before"
	for _ in $(seq 1 60); do
		sleep 2
		id="$(gh run list --workflow "$wf" --branch "$ref" --event workflow_dispatch --limit 1 --json databaseId --jq '.[0].databaseId // 0')"
		[ "$id" != "$before" ] && break
	done
	if [ "$id" = "$before" ]; then
		echo "run $i: the dispatch never appeared; giving up" >&2
		exit 1
	fi
	ids+=("$id")
	echo "  run $i/$runs -> $id"
done

pass=0
fail=0
for id in "${ids[@]}"; do
	gh run watch "$id" --exit-status >/dev/null 2>&1 && ok=1 || ok=0
	# The cell's own conclusion, not the run's: every other cell of the
	# matrix is skipped on a dispatch and a skipped job is not a failure.
	concl="$(gh run view "$id" --json jobs --jq \
		'[.jobs[] | select(.name | test("^e2e repeat \\(")) | .conclusion] | join(",")')"
	case "$concl" in
	success) pass=$((pass + 1)) ;;
	"") echo "run $id: no e2e repeat job; the dispatch matched no cell" >&2; fail=$((fail + 1)) ;;
	*) fail=$((fail + 1)) ;;
	esac
	echo "  $id: $concl (run exit ok=$ok)"
done

total=$((pass + fail))
echo
echo "e2e (pg$pg, $suite) on $ref: $pass/$total passed"
[ "$fail" = 0 ]
