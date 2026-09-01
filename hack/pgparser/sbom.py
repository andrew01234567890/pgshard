#!/usr/bin/env python3
"""Writes and checks the SBOM of a vendored libpg_query tree.

`generate` records what the vendored bytes say about themselves. `check`
scans them again and fails when they no longer say the same thing, which
is the case worth catching: an upstream release that brings in a file
under a licence the component never carried before changes what pgshard
may ship, and nothing else in the repository would notice. The checksum
manifest sees an edit; only this sees a change in kind.
"""

import datetime
import hashlib
import json
import pathlib
import sys

import licences

TOOL = "Tool: hack/pgparser/sbom.py"
SBOM_NAME = "sbom.spdx.json"


def component(root):
    version = (root / "VERSION").read_text().splitlines()
    if len(version) < 2:
        sys.exit("sbom: %s/VERSION must hold a tag and a commit" % root)
    return version[0].strip(), version[1].strip()


def manifest_digest(root):
    sums = root / "SHA256SUMS"
    if not sums.is_file():
        sys.exit("sbom: %s has no SHA256SUMS; run hack/pgparser/sync.sh first" % root)
    return hashlib.sha256(sums.read_bytes()).hexdigest()


def document(root):
    tag, commit = component(root)
    found = sorted(licences.scan(root))
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "libpg_query-%s" % tag,
        "documentNamespace": "https://github.com/pganalyze/libpg_query@%s" % commit,
        "creationInfo": {
            "created": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "creators": [TOOL],
        },
        "documentDescribes": ["SPDXRef-Package-libpg-query"],
        "packages": [{
            "SPDXID": "SPDXRef-Package-libpg-query",
            "name": "libpg_query",
            "versionInfo": tag,
            "downloadLocation": "git+https://github.com/pganalyze/libpg_query.git@%s" % commit,
            "sourceInfo": "vendored into %s by hack/pgparser/sync.sh" % root,
            "filesAnalyzed": False,
            # The licences the vendored sources themselves carry. Recorded
            # as concluded rather than declared: it is what reading the
            # bytes established, not what an upstream metadata file said.
            "licenseConcluded": " AND ".join("(%s)" % lic for lic in found) if found else "NOASSERTION",
            "licenseDeclared": "NOASSERTION",
            "copyrightText": "NOASSERTION",
            "checksums": [{"algorithm": "SHA256", "checksumValue": manifest_digest(root)}],
        }],
    }


def recorded(root):
    path = root / SBOM_NAME
    if not path.is_file():
        sys.exit("sbom: %s has no %s; run hack/pgparser/sbom.py generate %s" % (root, SBOM_NAME, root))
    doc = json.loads(path.read_text())
    pkg = doc["packages"][0]
    concluded = pkg["licenseConcluded"]
    if concluded == "NOASSERTION":
        return set(), pkg
    return {part.strip("()") for part in concluded.split(" AND ")}, pkg


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("generate", "check"):
        sys.exit("usage: sbom.py generate|check <vendored directory>")
    action, root = sys.argv[1], pathlib.Path(sys.argv[2])
    if not root.is_dir():
        sys.exit("sbom: %s is not a directory" % root)

    if action == "generate":
        (root / SBOM_NAME).write_text(json.dumps(document(root), indent=2) + "\n")
        print("sbom: wrote %s/%s" % (root, SBOM_NAME))
        return

    was, pkg = recorded(root)
    now = set(licences.scan(root))
    tag, commit = component(root)
    status = 0
    if "UNKNOWN" in now:
        print("sbom: %s carries files with a licence this check cannot read:" % root, file=sys.stderr)
        for path in sorted(licences.scan(root)["UNKNOWN"]):
            print("  %s" % path, file=sys.stderr)
        print("  Identify them, then add them to hack/pgparser/licences.py.", file=sys.stderr)
        status = 1
    for lic in sorted(now - was):
        print("sbom: %s now carries %s, which its SBOM does not record" % (root, lic), file=sys.stderr)
        status = 1
    for lic in sorted(was - now):
        print("sbom: %s no longer carries %s, which its SBOM still records" % (root, lic), file=sys.stderr)
        status = 1
    if pkg.get("versionInfo") != tag:
        print("sbom: %s vendors %s but its SBOM records %s" % (root, tag, pkg.get("versionInfo")), file=sys.stderr)
        status = 1
    if pkg.get("checksums", [{}])[0].get("checksumValue") != manifest_digest(root):
        print("sbom: %s has a checksum manifest the SBOM was not written from" % root, file=sys.stderr)
        status = 1
    if status:
        print("sbom: re-run hack/pgparser/sbom.py generate %s and review what changed" % root, file=sys.stderr)
    sys.exit(status)


if __name__ == "__main__":
    main()
