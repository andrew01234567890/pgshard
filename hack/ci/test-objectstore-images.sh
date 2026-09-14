#!/usr/bin/env bash
# Checks hack/ci/objectstore-images.sh against what uses the images. A
# manifest image it misses is pulled by kubelet with no retry, and a MinIO
# the integration suite runs that is not the listed one is pulled by docker
# run with none either.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

out="$(hack/ci/objectstore-images.sh)"
[ -n "$out" ] || { echo "objectstore-images: emitted nothing"; exit 1; }

for f in hack/objectstores/k8s/*.yaml; do
  while read -r ref; do
    grep -qxF "$ref" <<<"$out" || { echo "objectstore-images: $f runs $ref, which is not listed"; exit 1; }
  done < <(awk '{ for (i = 1; i < NF; i++) if ($i == "image:") print $(i + 1) }' "$f")
done

while read -r ref; do
  case "$ref" in
    *@sha256:* | *:?*) ;;
    *) echo "objectstore-images: $ref has no tag or digest, so what CI pulls is not what kubelet runs"; exit 1 ;;
  esac
  [ "${ref##*:}" != latest ] || { echo "objectstore-images: $ref is :latest, which kubelet always pulls again"; exit 1; }
done <<<"$out"

minio="$(sed -n 's/^const minioImage = "\(.*\)"$/\1/p' internal/agent/backup_integration_test.go)"
[ -n "$minio" ] || { echo "objectstore-images: no minioImage constant in internal/agent/backup_integration_test.go"; exit 1; }
grep -qxF "$minio" <<<"$out" || { echo "objectstore-images: the integration suite runs $minio, which the manifests do not"; exit 1; }
echo "objectstore-images: OK"
