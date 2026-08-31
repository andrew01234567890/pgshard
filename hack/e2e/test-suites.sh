#!/usr/bin/env bash
# Checks that hack/e2e/run.sh and .github/workflows/e2e-kind.yml agree about
# what an e2e suite is. Two copies of the same list is how the local entry
# point came to be something CI would never run; this fails the build when
# they drift rather than waiting for someone to notice.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run="$root/hack/e2e/run.sh"
wf="$root/.github/workflows/e2e-kind.yml"
status=0

# The names each side knows.
script_suites="$(sed -n 's/^\([a-z-]*\)) *args=(.*/\1/p' "$run" | LC_ALL=C sort)"
wf_suites="$(sed -n 's/^ *\([a-z-]*\)) go test .*/\1/p' "$wf" | LC_ALL=C sort)"
if [ "$script_suites" != "$wf_suites" ]; then
	echo "e2e suites differ between hack/e2e/run.sh and the workflow:" >&2
	diff <(echo "$script_suites") <(echo "$wf_suites") >&2 || true
	status=1
fi

# The go test arguments each side runs for a suite, normalised to the parts
# that decide what runs and for how long.
args_of() { # args_of <text>
	tr -d "'" <<<"$1" | tr ' ' '\n' | grep -v '^$' | LC_ALL=C sort | tr '\n' ' '
}
while IFS= read -r name; do
	[ -n "$name" ] || continue
	s="$(sed -n "s/^$name) *args=(\(.*\)); needs=.*/\1/p" "$run")"
	w="$(sed -n "s|^ *$name) go test -tags e2e -count=1 -v \(.*\) 2>&1 .*|\1|p" "$wf")"
	if [ "$(args_of "$s")" != "$(args_of "$w")" ]; then
		echo "e2e suite $name runs different tests locally and in CI:" >&2
		echo "  run.sh:   $(args_of "$s")" >&2
		echo "  workflow: $(args_of "$w")" >&2
		status=1
	fi
done <<<"$script_suites"

[ "$status" = 0 ] && echo "e2e-suites: OK"
exit "$status"
