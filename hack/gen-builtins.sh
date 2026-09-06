#!/usr/bin/env bash
# Regenerates internal/router/plan/builtins.go from PostgreSQL's pg_proc.dat.
#
# The planner has to tell an aggregate from a scalar function by name alone:
# it parses with libpg_query and never asks a shard what a function is. The
# names therefore come from the servers we target, not from a list kept by
# hand -- one that missed json_object_agg_unique and every other aggregate
# added since it was written.
#
# Reads the majors' catalogs from a local checkout when PG_SRC_18/PG_SRC_19
# point at one, else fetches them.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/internal/router/plan/builtins.go"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

for major in 18 19; do
    var="PG_SRC_$major"
    src="${!var:-}"
    if [ -n "$src" ]; then
        cp "$src/src/include/catalog/pg_proc.dat" "$work/$major.dat"
    else
        curl -fsSL "https://raw.githubusercontent.com/postgres/postgres/REL_${major}_STABLE/src/include/catalog/pg_proc.dat" -o "$work/$major.dat"
    fi
done

python3 "$root/hack/gen-builtins.py" "$work/18.dat" "$work/19.dat" > "$out"
gofmt -w "$out"
echo "wrote $out"
