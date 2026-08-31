#!/usr/bin/env bash
# A dropped module proxy must not fail a gate on the first attempt, and a
# tool that does not exist must still fail on it. Both are checked against a
# stub `go` rather than the network.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp" 2>/dev/null || true' EXIT

cat > "$tmp/go" <<'STUB'
#!/usr/bin/env bash
if [ "$1" = "env" ]; then echo "$TMPDIR_GOPATH"; exit 0; fi
n=$(cat "$TMPDIR_GOPATH/attempts" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$TMPDIR_GOPATH/attempts"
if [ "$n" -lt "$FAIL_UNTIL" ]; then
	echo "go: reading module: connection reset by peer" >&2
	exit 1
fi
exit 0
STUB
chmod +x "$tmp/go"
export TMPDIR_GOPATH="$tmp"
export PATH="$tmp:$PATH"

fail=0

FAIL_UNTIL=3 "$root/hack/tools/install.sh" actionlint >/dev/null 2>&1 || fail=1
if [ "$fail" -ne 0 ]; then
	echo "FAIL: two dropped attempts were not retried"
else
	echo "ok: retried past two dropped attempts (attempts=$(cat "$tmp/attempts"))"
fi

rm -f "$tmp/attempts"
if FAIL_UNTIL=99 "$root/hack/tools/install.sh" actionlint >/dev/null 2>&1; then
	echo "FAIL: a tool that never installs reported success"
	fail=1
else
	echo "ok: gave up after $(cat "$tmp/attempts") attempts when it never succeeded"
fi

rm -f "$tmp/attempts"
if FAIL_UNTIL=1 "$root/hack/tools/install.sh" nosuchtool >/dev/null 2>&1; then
	echo "FAIL: an unknown tool reported success"
	fail=1
elif [ -f "$tmp/attempts" ]; then
	echo "FAIL: an unknown tool reached go install"
	fail=1
else
	echo "ok: an unknown tool failed without retrying"
fi

if [ "$fail" -ne 0 ]; then exit 1; fi
echo "test-install-retry: OK"
