#!/usr/bin/env bash
set -u

if (($# != 1)); then
  printf 'usage: %s SOURCE-TREE\n' "$0" >&2
  exit 2
fi

source_tree=$1
if [[ ! -d "$source_tree" ]]; then
  printf 'benchmark source tree does not exist: %s\n' "$source_tree" >&2
  exit 2
fi

file_list=$(mktemp) || {
  printf 'unable to create a temporary benchmark file list\n' >&2
  exit 2
}
trap 'rm -f "$file_list"' EXIT

# Use only tools present on the hosted runner. NUL-delimited paths keep spaces,
# tabs, and newlines in filenames from changing the set of scanned files.
if ! find "$source_tree" -type f -name '*_test.go' -print0 >"$file_list"; then
  printf 'unable to enumerate Go test files under: %s\n' "$source_tree" >&2
  exit 2
fi

while IFS= read -r -d '' test_file; do
  if grep -Eq '^func Benchmark[A-Za-z0-9_]+\(' "$test_file"; then
    exit 0
  fi
  grep_status=$?
  if ((grep_status > 1)); then
    printf 'unable to inspect benchmark file: %s\n' "$test_file" >&2
    exit 2
  fi
done <"$file_list"

# Exit 1 means the scan completed successfully and found no benchmark.
exit 1
