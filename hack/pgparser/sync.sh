#!/usr/bin/env bash
# Vendors libpg_query at a pinned tag and commit into
# third_party/libpg_query/<major>.
#
# Usage: hack/pgparser/sync.sh <pg-major> <libpg_query-tag> [expected-commit]
#
# A tag is a movable name. Roughly 14 MiB of C goes straight into the SQL
# parser of the router, so the commit that tag resolves to is checked
# against the one we expect before a single byte is copied. Re-syncing a
# tag already vendored takes the expectation from the VERSION file that
# sync wrote; a new tag has to be given its commit on the command line,
# because nothing else knows what it should be.
set -euo pipefail

major="${1:?pg major (e.g. 18)}"
tag="${2:?libpg_query tag (e.g. 18.0.0)}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
dest="$root/third_party/libpg_query/$major"
expect="${3:-}"
if [ -z "$expect" ] && [ -f "$dest/VERSION" ] && [ "$(head -1 "$dest/VERSION")" = "$tag" ]; then
	expect="$(sed -n 2p "$dest/VERSION")"
fi
if [ -z "$expect" ]; then
	echo "sync.sh: no expected commit for $tag: pass it as the third argument" >&2
	echo "  git ls-remote https://github.com/pganalyze/libpg_query refs/tags/$tag" >&2
	exit 2
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp" 2>/dev/null || true' EXIT

git -c advice.detachedHead=false clone -q --depth 1 -b "$tag" https://github.com/pganalyze/libpg_query "$tmp/src"
commit="$(git -C "$tmp/src" rev-parse HEAD)"
if [ "$commit" != "$expect" ]; then
	echo "sync.sh: tag $tag is at $commit, expected $expect" >&2
	echo "  the tag moved, or the source is not the one this repository pinned; nothing was copied" >&2
	exit 1
fi

mkdir -p "$dest"
find "$dest" -mindepth 1 -maxdepth 1 ! -name '*.go' -exec rm -rf {} +
mkdir -p "$dest/include/postgres" "$dest/include/protobuf" "$dest/protobuf"

lib="$tmp/src"
cp "$lib"/src/*.c "$lib"/src/*.h "$dest/"
cp "$lib"/src/postgres/*.c "$dest/"
cp -a "$lib"/src/include/. "$dest/include/"
cp -a "$lib"/src/postgres/include/. "$dest/include/postgres/"
cp "$lib"/pg_query.h "$lib"/postgres_deparse.h "$dest/include/"
cp "$lib"/protobuf/pg_query.pb-c.c "$dest/"
cp "$lib"/protobuf/pg_query.pb-c.h "$dest/include/protobuf/"
cp "$lib"/protobuf/pg_query.proto "$dest/protobuf/"
cp "$lib"/vendor/protobuf-c/protobuf-c.c "$lib"/vendor/xxhash/xxhash.c "$dest/"
mkdir -p "$dest/include/protobuf-c" "$dest/include/xxhash"
cp "$lib"/vendor/protobuf-c/protobuf-c.h "$dest/include/protobuf-c/"
cp "$lib"/vendor/xxhash/xxhash.h "$dest/include/xxhash/"
cp "$lib"/LICENSE "$dest/LICENSE"
cp "$lib"/src/postgres/COPYRIGHT "$dest/COPYRIGHT.postgresql"

printf '%s\n%s\n' "$tag" "$commit" > "$dest/VERSION"
cat > "$dest/NOTICE" <<NOTICE
This directory vendors libpg_query (https://github.com/pganalyze/libpg_query)
at tag $tag (commit $commit), which is licensed under the BSD 3-Clause
License (see LICENSE). It bundles portions of PostgreSQL $major (see
COPYRIGHT.postgresql, PostgreSQL License), protobuf-c (BSD 2-Clause) and
xxHash (BSD 2-Clause). Individual files carry further licences, all
permissive; sbom.spdx.json lists every one found in the vendored bytes and
is what hack/pgparser/verify.sh checks. Regenerate with:
hack/pgparser/sync.sh $major $tag
NOTICE
# The manifest is what makes a later hand-edit of the vendored C visible.
# It covers upstream's files only: the .go files in this directory are
# pgshard's binding and change on their own schedule.
( cd "$dest" && find . -type f ! -name '*.go' ! -name SHA256SUMS ! -name sbom.spdx.json -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > SHA256SUMS )

# Written from the manifest, so it records the bytes just checksummed.
python3 "$(dirname "$0")/sbom.py" generate "$dest"

echo "synced libpg_query $tag ($commit) into $dest"
