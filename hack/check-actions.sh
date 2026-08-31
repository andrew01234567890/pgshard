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
    r"\$\{\{\s*github\.(event\.(pull_request\.(title|body|head\.ref|head\.repo\.full_name)|issue\.(title|body)|comment\.body)|head_ref)\s*\}\}"
)
DOCKER_DIGEST_RE = re.compile(r"^docker://[^@]+@sha256:[0-9a-f]{64}$")

def walk_strings(v):
    if isinstance(v, str):
        yield v
    elif isinstance(v, dict):
        for x in v.values():
            yield from walk_strings(x)
    elif isinstance(v, list):
        for x in v:
            yield from walk_strings(x)

def norm_perms(p):
    return {k.lower(): (v.lower() if isinstance(v, str) else v) for k, v in p.items()} if isinstance(p, dict) else p

# A pull request's own code runs in these workflows, including a change to
# the workflow file itself, so a token that can publish or sign must not be
# handed to the whole file: it is available before review. Job-level is
# fine, because a job can be gated on main.
FORBIDDEN_AT_TOP = ("contents", "packages", "id-token", "deployments", "attestations")

def check_permissions(perms, where, errs, top):
    if perms is None:
        return
    if isinstance(perms, str) and perms.lower() == "write-all":
        errs.append(f"{where}: permissions: write-all is forbidden")
    if top and isinstance(perms, dict):
        got = norm_perms(perms)
        for name in FORBIDDEN_AT_TOP:
            if got.get(name) == "write":
                errs.append(f"{where}: top-level {name}: write is forbidden; grant it on the job that needs it, gated on main")

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
            if isinstance(uses, str) and uses.startswith("docker://"):
                if not DOCKER_DIGEST_RE.match(uses):
                    errs.append(f"{swhere}: docker:// action not pinned by sha256 digest: {uses}")
            elif isinstance(uses, str) and not uses.startswith("./"):
                if not SHA_RE.match(uses):
                    errs.append(f"{swhere}: action not pinned to a 40-hex SHA: {uses}")
            run = step.get("run")
            if isinstance(run, str) and INTERP_RE.search(run):
                errs.append(f"{swhere}: untrusted event field interpolated into run block")
            if any(INTERP_RE.search(v) for v in walk_strings(step.get("with"))):
                errs.append(f"{swhere}: untrusted event field interpolated into with: input")

if errs:
    print("check-actions: FAIL")
    for e in errs:
        print("  " + e)
    sys.exit(1)
print(f"check-actions: OK ({len(sys.argv) - 1} workflow(s))")
PY
