#!/usr/bin/env bash
# Runs a command, retrying it while it fails. For steps whose failures are
# usually somebody else's network: a registry answering 403, a module proxy
# dropping a stream, a cluster that did not come up. A step that fails for
# its own reasons fails every attempt and still fails the job, only later.
#
# Usage: retry.sh [-n attempts] [-d seconds] [-c cleanup-command] -- cmd...
set -uo pipefail

attempts=3
delay=10
cleanup=""
while [ $# -gt 0 ]; do
  case "$1" in
    -n) attempts="$2"; shift 2 ;;
    -d) delay="$2"; shift 2 ;;
    -c) cleanup="$2"; shift 2 ;;
    --) shift; break ;;
    *) echo "retry.sh: unknown option $1" >&2; exit 2 ;;
  esac
done
if [ $# -eq 0 ]; then
  echo "retry.sh: nothing to run" >&2
  exit 2
fi

n=1
while true; do
  "$@"
  status=$?
  if [ "$status" -eq 0 ]; then
    if [ "$n" -gt 1 ]; then
      echo "retry.sh: succeeded on attempt $n/$attempts"
    fi
    exit 0
  fi
  if [ "$n" -ge "$attempts" ]; then
    echo "retry.sh: giving up after $n attempt(s): exit $status" >&2
    exit "$status"
  fi
  echo "retry.sh: attempt $n/$attempts failed with exit $status; retrying in ${delay}s" >&2
  if [ -n "$cleanup" ]; then
    # Best effort: a cleanup that fails must not mask the retry.
    sh -c "$cleanup" || true
  fi
  sleep "$delay"
  n=$((n + 1))
  delay=$((delay * 2))
done
