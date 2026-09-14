#!/usr/bin/env bash
# Prints every image the object-store manifests run, one per line. CI pulls
# these itself, with retries, before anything needs them: a registry that
# answers one pull with a 502 otherwise fails a required job on luck
# (PGS-842), and kubelet inside kind gives up on a pull far sooner.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sed -n 's/^[[:space:]]*\(-[[:space:]]*\)\{0,1\}image:[[:space:]]*\([^[:space:]#]*\).*/\2/p' "$root"/hack/objectstores/k8s/*.yaml | LC_ALL=C sort -u
