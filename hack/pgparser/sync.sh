#!/usr/bin/env bash
# Vendors libpg_query at a pinned tag into third_party/libpg_query/<major>.
# Usage: hack/pgparser/sync.sh <pg-major> <libpg_query-tag>
set -euo pipefail

major="${1:?pg major (e.g. 18)}"
tag="${2:?libpg_query tag (e.g. 18.0.0)}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
dest="$root/third_party/libpg_query/$major"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

git -c advice.detachedHead=false clone -q --depth 1 -b "$tag" https://github.com/pganalyze/libpg_query "$tmp/src"
commit="$(git -C "$tmp/src" rev-parse HEAD)"

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
xxHash (BSD 2-Clause). Regenerate with: hack/pgparser/sync.sh $major $tag
NOTICE
echo "synced libpg_query $tag ($commit) into $dest"
