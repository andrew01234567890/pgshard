#!/usr/bin/env bash
# Builds the collision the checker exists for -- two branches numbering a
# migration against the same base -- and asserts it is refused, and that a
# branch numbering above the base is not.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
checker="$root/hack/check-migration-numbers.sh"
tmp="$(mktemp -d)"
# The cleanup must not decide the exit status. git can leave a background
# maintenance run writing into .git, rm then reports "Directory not empty",
# and under set -e a failing trap failed the whole job after every check had
# already passed -- which is how this broke main rather than a branch.
trap 'rm -rf "$tmp" 2>/dev/null || true' EXIT

cd "$tmp"
git init -q -b main .
git config user.email t@example.invalid
git config user.name test
# No background repacking in a repository that exists for four commits and
# is deleted a second later.
git config gc.auto 0
git config maintenance.auto false
mkdir -p internal/catalog/schema
echo "SELECT 1;" > internal/catalog/schema/0001_base.sql
git add -A && git commit -qm base

git checkout -q -b first
echo "SELECT 1;" > internal/catalog/schema/0002_first.sql
git add -A && git commit -qm first
git checkout -q main
git merge -q --no-ff first -m "merge first"

git checkout -q -b second main~1
echo "SELECT 1;" > internal/catalog/schema/0002_second.sql
git add -A && git commit -qm second

fail=0
if "$checker" main >/dev/null 2>&1; then
	echo "FAIL: a migration colliding with one already on main was accepted"
	fail=1
else
	echo "ok: refused 0002_second.sql against 0002_first.sql"
fi

git checkout -q -b third main
echo "SELECT 1;" > internal/catalog/schema/0003_third.sql
git add -A && git commit -qm third
if ! "$checker" main >/dev/null 2>&1; then
	echo "FAIL: a migration numbered above main was refused"
	"$checker" main || true
	fail=1
else
	echo "ok: accepted 0003_third.sql"
fi

# The same file present on both sides is the ordinary case of a branch that
# carries a migration already merged; it is not a collision.
git checkout -q main
if ! "$checker" main >/dev/null 2>&1; then
	echo "FAIL: a branch identical to main was refused"
	fail=1
else
	echo "ok: accepted a branch identical to main"
fi

if [ "$fail" -ne 0 ]; then exit 1; fi
echo "test-check-migration-numbers: OK"
