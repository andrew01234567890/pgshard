#!/usr/bin/env bash
# Runs hack/check-actions.sh against fixtures: good must pass, every bad must fail.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
checker="$root/hack/check-actions.sh"
data="$root/hack/testdata"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

if ! "$checker" "$data/good" >/dev/null; then
  echo "FAIL: good fixtures rejected"
  "$checker" "$data/good" || true
  fail=1
fi

for f in "$data"/bad/*.yml; do
  rm -rf "$tmp/one"; mkdir -p "$tmp/one"; cp "$f" "$tmp/one/"
  if "$checker" "$tmp/one" >/dev/null 2>&1; then
    echo "FAIL: bad fixture accepted: $(basename "$f")"
    fail=1
  else
    echo "ok: rejected $(basename "$f")"
  fi
done

if [ "$fail" -ne 0 ]; then exit 1; fi
echo "test-check-actions: OK"
