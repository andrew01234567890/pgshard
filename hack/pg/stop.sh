#!/usr/bin/env bash
# Usage: hack/pg/stop.sh [name|major]
set -euo pipefail
target=${1:-}
case "$target" in
  "") docker ps -aq --filter name='^pgshard-pg' | xargs -r docker rm -f >/dev/null ;;
  [0-9]*) docker rm -f "pgshard-pg${target}" >/dev/null ;;
  *) docker rm -f "$target" >/dev/null ;;
esac
