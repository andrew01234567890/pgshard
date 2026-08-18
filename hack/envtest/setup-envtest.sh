#!/usr/bin/env bash
# Installs the envtest control-plane binaries (kube-apiserver, etcd) and prints
# the directory containing them on stdout. Usage: setup-envtest.sh <assets-dir>
set -euo pipefail

assets_dir="${1:?usage: setup-envtest.sh <assets-dir>}"
version="${SETUP_ENVTEST_VERSION:-v0.24.1}"
k8s="${ENVTEST_K8S_VERSION:-1.34}"

go run "sigs.k8s.io/controller-runtime/tools/setup-envtest@${version}" \
	use "${k8s}" --bin-dir "${assets_dir}" -p path
