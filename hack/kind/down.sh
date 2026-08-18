#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name "${KIND_CLUSTER_NAME:-pgshard-e2e}"
