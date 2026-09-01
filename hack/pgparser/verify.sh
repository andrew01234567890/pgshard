#!/usr/bin/env bash
# Checks that the vendored libpg_query trees are the bytes sync.sh recorded.
#
# The vendored C is compiled into the router and parses every statement a
# client sends. Nothing else in the repository would notice a change to it:
# the secret scan looks for credentials, govulncheck reads Go, and a diff
# of 14 MiB of generated parser is not something a reviewer reads. A
# manifest written at sync time and checked here is what makes an edit
# after the fact visible.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
status=0
found=0
for dir in "$root"/third_party/libpg_query/*/; do
	[ -d "$dir" ] || continue
	found=1
	name="$(basename "$dir")"
	if [ ! -f "$dir/SHA256SUMS" ]; then
		echo "verify: third_party/libpg_query/$name has no SHA256SUMS; re-run hack/pgparser/sync.sh" >&2
		status=1
		continue
	fi
	# A file added since the sync is as much a change as an edited one, and
	# sha256sum -c alone would not see it. The SBOM is written from the
	# manifest and so cannot appear in it; sbom.py check is what guards
	# that file, by reading the vendored bytes rather than trusting it.
	listed="$(mktemp)"; present="$(mktemp)"
	awk '{ sub(/^\*/, "", $2); print $2 }' "$dir/SHA256SUMS" | LC_ALL=C sort > "$listed"
	( cd "$dir" && find . -type f ! -name '*.go' ! -name SHA256SUMS ! -name sbom.spdx.json ) | LC_ALL=C sort > "$present"
	if ! diff -u "$listed" "$present" > /dev/null; then
		echo "verify: third_party/libpg_query/$name has files the manifest does not list, or is missing files it does:" >&2
		diff -u "$listed" "$present" >&2 || true
		status=1
	fi
	if ! ( cd "$dir" && sha256sum --quiet -c SHA256SUMS ); then
		echo "verify: third_party/libpg_query/$name does not match the checksums sync.sh recorded" >&2
		status=1
	fi
	# The checksums see an edit; this sees a change in kind. An upstream
	# release that brings in a file under a licence the component never
	# carried before changes what pgshard may ship, and the NOTICE beside
	# it is written by hand and would go on saying otherwise.
	if ! python3 "$root/hack/pgparser/sbom.py" check "$dir"; then
		status=1
	fi
	rm -f "$listed" "$present"
done
if [ "$found" = 0 ]; then
	echo "verify: no vendored libpg_query trees found" >&2
	exit 1
fi
[ "$status" = 0 ] && echo "vendored-parser: OK"
exit "$status"
