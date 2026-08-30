#!/usr/bin/env bash
# Installs the pinned tools the gates need into $(go env GOPATH)/bin.
# With no arguments it installs all of them; otherwise the named ones.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$here/versions.env"

install_one() {
  case "$1" in
    golangci-lint) go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}" ;;
    buf)           go install "github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}" ;;
    protoc-gen-go) go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}" ;;
    protoc-gen-go-grpc) go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}" ;;
    govulncheck)   go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" ;;
    actionlint)    go install "github.com/rhysd/actionlint/cmd/actionlint@${ACTIONLINT_VERSION}" ;;
    *) echo "install.sh: unknown tool $1" >&2; return 1 ;;
  esac
  echo "installed $1"
}

tools=("$@")
if [ "${#tools[@]}" -eq 0 ]; then
  tools=(golangci-lint buf protoc-gen-go protoc-gen-go-grpc govulncheck actionlint)
fi
for t in "${tools[@]}"; do install_one "$t"; done

echo "tools are in $(go env GOPATH)/bin; put it on PATH"
