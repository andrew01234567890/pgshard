#!/usr/bin/env bash
# Policy checks for GitHub Actions workflows. Requires python3 with PyYAML.
set -euo pipefail

dir="${1:-.github/workflows}"
shopt -s nullglob
files=("$dir"/*.yml "$dir"/*.yaml)
if [ "${#files[@]}" -eq 0 ]; then
  echo "check-actions: no workflow files under $dir" >&2
  exit 1
fi

python3 - "${files[@]}" <<'PY'
import re
import sys

try:
    import yaml
except ImportError:
    sys.exit("check-actions: python3 yaml module is required")

SHA_RE = re.compile(r"^[^@]+@[0-9a-f]{40}$")
INTERP_RE = re.compile(
    r"\$\{\{\s*github\.event\.(pull_request\.(title|body|head\.ref)|issue\.(title|body)|comment\.body)\s*\}\}"
)

def norm_perms(p):
    return {k.lower(): (v.lower() if isinstance(v, str) else v) for k, v in p.items()} if isinstance(p, dict) else p

def check_permissions(perms, where, errs, top):
    if perms is None:
        return
    if isinstance(perms, str) and perms.lower() == "write-all":
        errs.append(f"{where}: permissions: write-all is forbidden")
    if top and isinstance(perms, dict) and norm_perms(perms).get("contents") == "write":
        errs.append(f"{where}: top-level contents: write is forbidden")

errs = []
for path in sys.argv[1:]:
    with open(path) as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        errs.append(f"{path}: not a workflow mapping")
        continue
    on = doc.get("on", doc.get(True))
    triggers = []
    if isinstance(on, str):
        triggers = [on]
    elif isinstance(on, list):
        triggers = on
    elif isinstance(on, dict):
        triggers = list(on.keys())
    if "pull_request_target" in triggers:
        errs.append(f"{path}: pull_request_target trigger is forbidden")
    check_permissions(doc.get("permissions"), path, errs, top=True)
    for jname, job in (doc.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        where = f"{path}: job {jname}"
        check_permissions(job.get("permissions"), where, errs, top=False)
        uses = job.get("uses")
        if isinstance(uses, str) and not uses.startswith("./") and not SHA_RE.match(uses):
            errs.append(f"{where}: reusable workflow not pinned to a 40-hex SHA: {uses}")
        for i, step in enumerate(job.get("steps") or []):
            if not isinstance(step, dict):
                continue
            swhere = f"{where} step {i + 1}"
            uses = step.get("uses")
            if isinstance(uses, str) and not uses.startswith("./") and not uses.startswith("docker://"):
                if not SHA_RE.match(uses):
                    errs.append(f"{swhere}: action not pinned to a 40-hex SHA: {uses}")
            run = step.get("run")
            if isinstance(run, str) and INTERP_RE.search(run):
                errs.append(f"{swhere}: untrusted event field interpolated into run block")

if errs:
    print("check-actions: FAIL")
    for e in errs:
        print("  " + e)
    sys.exit(1)
print(f"check-actions: OK ({len(sys.argv) - 1} workflow(s))")
PY
