#!/usr/bin/env bash
# Refuses a Dockerfile instruction that downloads Go modules without retrying.
# The module proxy answers builds with a stream error often enough that one
# unretried download fails a whole suite on luck (PGS-540), and the PostgreSQL
# image was left single-shot after the other two were fixed (PGS-841).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

mapfile -t files < <(git ls-files '*Dockerfile*')
[ "${#files[@]}" -gt 0 ] || { echo "module-downloads: found no Dockerfiles"; exit 1; }

# One logical instruction per record: continuation lines are joined first, so
# a download inside a retry loop is judged together with the loop around it.
bad="$(awk '
  function judge() {
    gsub(/[ \t]+/, " ", text)
    if (text ~ /go mod download/ && text !~ /for attempt in/) print file ":" start
    text = ""
  }
  FNR == 1 { if (text != "") judge(); file = FILENAME }
  # BuildKit skips comment and blank lines inside a continued instruction.
  text != "" && /^[ \t]*(#.*)?$/ { next }
  /^[ \t]*#/ { next }
  {
    if (text == "") start = FNR
    line = $0
    cont = sub(/\\[ \t]*$/, "", line)
    text = text " " line
    if (!cont) judge()
  }
  END { if (text != "") judge() }
' "${files[@]}")"

if [ -n "$bad" ]; then
  echo "module-downloads: go mod download without a retry loop at:"
  echo "$bad"
  exit 1
fi
echo "module-downloads: OK"
