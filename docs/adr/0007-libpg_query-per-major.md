# 7. Parsing with libpg_query, pinned per PostgreSQL major

Status: accepted

## Context

The router routes from the parse tree: which tables a statement touches,
whether it writes, where its shard key is bound, whether it is something
pgshard refuses. A wrong parse is a wrong route, and a wrong route is data
in the wrong shard.

`pg_query_go` is the obvious dependency, and it tracks PostgreSQL 17. We
target 18 and 19.

## Decision

Vendor libpg_query at a tag pinned **per PostgreSQL major**
(`third_party/libpg_query/<major>`) and generate our own cgo binding, one
engine per major, selected by the server version in force.

Two things follow from "per major" rather than "latest".

A parser that is a *different* version from the server is not a smaller
problem than no parser. It is a parser that accepts syntax the server will
reject and, worse, one that can attach a different meaning to syntax both
accept. Pinning to the major means the grammar the router reasons about is
the grammar the shard will execute.

Syntax the pinned parser does not know is refused with `0A000` and a
message that names the reason. It is never guessed at, never passed through
unparsed. While no libpg_query release exists for PostgreSQL 19, 19-only
syntax is refused for exactly this reason, and the refusal is the honest
answer rather than a silent single-shard fallback.

The alternative considered was a pure-Go grammar. It removes cgo, and it
adds a second implementation of PostgreSQL's grammar to keep correct
against two majors. Multigres maintains one under the PostgreSQL licence
and it remains the fallback if cgo becomes a blocker in a target
environment; it is not the default because the parse must match the server
and the server's own parser is the only thing that does by construction.

## Consequences

- The router build needs cgo and a C toolchain, and the vendored sources
  are checked in so a build does not reach the network. `make vendor-check`
  re-runs the sync script and fails if the tree differs from the pinned
  tag.
- Adding a PostgreSQL major means vendoring a libpg_query tag for it and
  building another engine, not flipping a flag.
- Parsing is on the hot path, so results are cached by fingerprint with an
  admission cap; the cache is a performance decision, the pinning is a
  correctness one.
