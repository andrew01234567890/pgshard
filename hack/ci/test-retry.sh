#!/usr/bin/env bash
# Tests hack/ci/retry.sh. A retry helper that silently gave up, or retried a
# command that could never succeed, would be worse than none.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
retry="$here/retry.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fails=0

check() {
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1: got $2, want $3" >&2
    fails=$((fails + 1))
  fi
}

# A command that succeeds runs once.
"$retry" -n 3 -d 0 -- sh -c "echo run >> $tmp/a" >/dev/null 2>&1
check "successful command runs once" "$(wc -l < "$tmp/a")" "1"

# One that fails twice and then succeeds is retried until it does.
cat > "$tmp/flaky" <<SH
#!/bin/sh
echo run >> "$tmp/b"
[ "\$(wc -l < "$tmp/b")" -ge 3 ]
SH
chmod +x "$tmp/flaky"
"$retry" -n 5 -d 0 -- "$tmp/flaky" >/dev/null 2>&1
check "flaky command is retried" "$?" "0"
check "flaky command ran three times" "$(wc -l < "$tmp/b")" "3"

# One that always fails is attempted exactly -n times and fails with its own
# exit status, so a real failure still fails the job.
cat > "$tmp/broken" <<SH
#!/bin/sh
echo run >> "$tmp/c"
exit 7
SH
chmod +x "$tmp/broken"
"$retry" -n 3 -d 0 -- "$tmp/broken" >/dev/null 2>&1
check "a command that cannot succeed keeps its exit status" "$?" "7"
check "a command that cannot succeed is attempted -n times" "$(wc -l < "$tmp/c")" "3"

# The cleanup runs between attempts, not after the last one.
cat > "$tmp/broken2" <<SH
#!/bin/sh
exit 1
SH
chmod +x "$tmp/broken2"
"$retry" -n 3 -d 0 -c "echo clean >> $tmp/d" -- "$tmp/broken2" >/dev/null 2>&1
check "cleanup runs between attempts" "$(wc -l < "$tmp/d")" "2"

# Nothing to run is a usage error, not a silent success.
"$retry" -n 2 -d 0 -- >/dev/null 2>&1
check "no command is a usage error" "$?" "2"

if [ "$fails" -ne 0 ]; then
  echo "test-retry: $fails failure(s)" >&2
  exit 1
fi
echo "test-retry: OK"
