# pgshard-planner

This internal crate is the fail-closed boundary between untrusted PostgreSQL
query text and future routing analysis. It currently provides byte-, token-,
AST- and stack-bounded candidate parsing configured with a PostgreSQL dialect,
plus an opaque statement wrapper whose debug output cannot expose SQL. A
privacy-safe lexical nesting guard runs before the upstream parser, including
for candidate-only angle-bracket `ARRAY` data types that bypass its recursion
limit. Unrelated PostgreSQL comparison operators do not consume that budget.
Parsing, AST validation, and destruction reserve stack in proportion to the
already-bounded structural-token count; whitespace, comments, and empty
statement semicolons cannot inflate it. This also covers flat recursive trees,
such as long binary expressions, which do not consume delimiter or
parser-recursion depth. The reserve normally stays on the caller's stack; a
larger stack segment is allocated only when the caller has insufficient space.

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

The first catalog-bound route template accepts only an explicitly
schema-qualified `SELECT *` from one registered table whose entire predicate is
direct equality between its unqualified shard-key column and one canonical
`$1` through `$65535` placeholder. It rejects aliases, joins, CTEs, subqueries,
additional predicates, casts, modifiers, ordering, limits, locks, and the
noncanonical `==` operator. The template is not executable until parameter
types, operator resolution, and the corresponding Bind value are validated.

`cargo bench -p pgshard-planner --bench parse_statement` measures this parsing
boundary in isolation. `cargo bench -p pgshard-planner --bench
analyze_parameter_route` measures parsing plus the catalog-bound template. Both
are informational and are not pooler-throughput claims.
