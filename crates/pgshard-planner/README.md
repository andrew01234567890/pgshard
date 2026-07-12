# pgshard-planner

This internal crate is the fail-closed boundary between untrusted PostgreSQL
query text and future routing analysis. It currently provides bounded,
stack-protected parsing for a conservative PostgreSQL-dialect subset and an
opaque statement wrapper whose debug output cannot expose SQL.

Parsing is not PostgreSQL semantic validation and a syntactic statement kind is
not a routing or read-only decision. Future route analysis must explicitly
prove every supported AST shape and reject everything else. PostgreSQL 18
compatibility is maintained with a PostgreSQL 18 live-server differential test
for every admitted top-level DML kind.

The API is synchronous. The future pooler session layer must execute it on a
bounded CPU worker pool with a concurrency limit; it must not let large parses
block asynchronous socket workers.

`cargo bench -p pgshard-planner --bench parse_statement` measures this parsing
boundary in isolation. It is informational and is not a pooler-throughput claim.
