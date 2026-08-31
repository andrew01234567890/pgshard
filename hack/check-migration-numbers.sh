#!/usr/bin/env bash
# Fails when this branch adds a catalog migration whose number is already
# taken on the base branch. Two branches numbered against the same main both
# pick the next free number, and the loader then refuses the whole ledger --
# on main, after both have merged.
set -euo pipefail

base="${1:-origin/main}"
dir=internal/catalog/schema

if ! git rev-parse --verify --quiet "$base" >/dev/null; then
	echo "check-migration-numbers: $base is not fetched; skipping" >&2
	exit 0
fi

versions() {
	git ls-tree --name-only "$1" "$dir/" | sed -n 's|.*/\([0-9]\{4\}\)_\(.*\)\.sql$|\1 \2|p'
}

status=0
while read -r version name; do
	[ -n "$version" ] || continue
	theirs=$(versions "$base" | awk -v v="$version" '$1 == v {print $2}')
	if [ -n "$theirs" ] && [ "$theirs" != "$name" ]; then
		echo "migration ${version}_${name}.sql collides with ${version}_${theirs}.sql on ${base}" >&2
		echo "  renumber it above every migration on ${base}: the loader refuses a ledger with two files at one version" >&2
		status=1
	fi
done < <(versions HEAD)
exit $status
