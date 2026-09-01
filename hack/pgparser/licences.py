#!/usr/bin/env python3
"""Reads the licences the vendored parser's own files claim.

The NOTICE beside the vendored tree is written by hand at sync time. It
says BSD 3-Clause, the PostgreSQL Licence and two BSD 2-Clause components,
and nothing checked that the bytes still agreed after an upgrade -- an
upstream release that pulled in a file under a copyleft licence would
change what pgshard may ship and leave the NOTICE saying otherwise.

Identification is by the licence's own operative wording, not by a name a
file might merely mention. Unrecognised text in a file that clearly
carries a licence is reported as unknown rather than guessed at: an
unknown licence is exactly the case worth a human's attention.
"""

import json
import pathlib
import re
import sys

# Ordered: the first whose markers all appear wins, so BSD-3-Clause is
# tested before BSD-2-Clause, which is its own text minus one clause.
BISON_EXCEPTION = "special exception, you may create a larger work"

LICENCES = [
    ("PostgreSQL", [
        "Permission to use, copy, modify, and distribute this software and its",
        "documentation for any purpose, without fee, and without a written agreement",
    ]),
    ("ISC", [
        "Permission to use, copy, modify, and distribute this software for any",
        "purpose with or without fee is hereby granted",
    ]),
    # Henry Spencer's regex terms, which PostgreSQL carries verbatim.
    ("Spencer-99", [
        "Redistribution and use in source and binary forms -- with or without",
        "indicate the origin and nature of any modifications",
    ]),
    ("BSD-3-Clause", [
        "Redistribution and use in source and binary forms",
        "Neither the name of",
    ]),
    ("BSD-2-Clause", [
        "Redistribution and use in source and binary forms",
        "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS",
    ]),
    ("Apache-2.0", ["Licensed under the Apache License"]),
    ("MIT", ["Permission is hereby granted, free of charge"]),
    # Nothing here may ship under a copyleft licence, so these are named in
    # order to be reported, not in order to be allowed.
    ("GPL-2.0-or-later", ["GNU General Public License"]),
    ("LGPL", ["GNU Lesser General Public License"]),
    ("MPL-2.0", ["Mozilla Public License"]),
]

# A file makes a licence statement when it grants something. A copyright
# line alone does not: nearly every PostgreSQL header carries one and
# points at the COPYRIGHT file for the terms, and treating those as
# unreadable licences would bury the handful that say something of their
# own under five hundred that do not.
CLAIMS = re.compile(
    r"(permission is hereby|permission to use|redistribution and use"
    r"|licensed under the|gnu (general|lesser) public license"
    r"|mozilla public license|this file is part of)",
    re.I,
)

# Only the head of a file: a licence lives in its header, and matching the
# whole of a 14 MiB generated parser would find the word in a string table.
HEAD_BYTES = 8192


def licences_in(text):
    found = set()
    for name, markers in LICENCES:
        if all(m in text for m in markers):
            found.add(name)
    return found


def scan(root):
    seen = {}
    for path in sorted(root.rglob("*")):
        # NOTICE and the standalone licence texts are the component's own
        # statement about itself; scanning them would report what they are
        # rather than what the sources say, which is the question here.
        if not path.is_file() or path.name in ("SHA256SUMS", "sbom.spdx.json", "NOTICE"):
            continue
        try:
            head = path.read_text(encoding="utf-8", errors="replace")[:HEAD_BYTES]
        except OSError as err:
            sys.exit("licences: %s: %s" % (path, err))
        found = licences_in(head)
        # Bison stamps its output with the GPL and then grants an exception
        # covering exactly this use. The distinction is the whole question
        # for a generated parser, so it is carried in the identifier
        # instead of being flattened to "GPL".
        if "GPL-2.0-or-later" in found and BISON_EXCEPTION in head:
            found.discard("GPL-2.0-or-later")
            found.add("GPL-2.0-or-later WITH Bison-exception-2.2")
        if not found and CLAIMS.search(head):
            # A file that plainly makes a licence statement we cannot read.
            found = {"UNKNOWN"}
        for name in found:
            seen.setdefault(name, []).append(str(path.relative_to(root)))
    return seen


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: licences.py <vendored directory>")
    root = pathlib.Path(sys.argv[1])
    if not root.is_dir():
        sys.exit("licences: %s is not a directory" % root)
    seen = scan(root)
    print(json.dumps({name: sorted(paths) for name, paths in sorted(seen.items())}, indent=2))


if __name__ == "__main__":
    main()
