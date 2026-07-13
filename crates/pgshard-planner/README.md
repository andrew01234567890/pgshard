# pgshard-planner

This internal crate is the fail-closed boundary between untrusted PostgreSQL
query text and future routing analysis. It currently provides byte-, token-,
AST- and stack-bounded candidate parsing configured with a PostgreSQL dialect,
plus an opaque statement wrapper whose debug output cannot expose SQL. A
privacy-safe lexical nesting guard runs before the upstream parser, including
for dialect-specific angle-bracket data types that bypass its recursion limit.

Parsing is not PostgreSQL semantic validation and a syntactic statement kind is
not a routing or read-only decision. Future route analysis must explicitly
prove every supported AST shape and reject everything else. A PostgreSQL 18
smoke corpus records positive DML examples and known syntax the candidate parser
accepts but PostgreSQL rejects; it is not a differential compatibility claim.

The upstream parser contains payload-bearing debug calls. pgshard therefore
compiles the `log` facade with its static maximum level set to `Off` and verifies
that behavior with an installed-logger regression in optimized builds. Product
telemetry uses explicitly sanitized `tracing` fields instead.

The API is synchronous. The future pooler session layer must execute it on a
bounded CPU worker pool with a concurrency limit; it must not let large parses
block asynchronous socket workers.

`cargo bench -p pgshard-planner --bench parse_statement` measures this parsing
boundary in isolation. It is informational and is not a pooler-throughput claim.
