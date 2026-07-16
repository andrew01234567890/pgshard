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
The next proof stage consumes PostgreSQL's authoritative parameter description,
requires the selected parameter to have the exact built-in shard-key type OID,
and binds the result to the exact cluster and complete catalog-snapshot
checksum. Every active shard must expose the named column on the named logical
database and schema-qualified permanent ordinary table with the exact built-in
type and storage semantics; inheritance is rejected. The backend session must
remain pinned to an empty `search_path` from Parse through execution. A
parameter OID alone is unsafe: PostgreSQL can coerce an explicitly typed
`bigint` parameter to a `double precision` column, making distinct large
integers compare equal after rounding even though they hash differently.
PostgreSQL 18 integration coverage locks down that regression; rejects catalog
rows from another table, views, unlogged tables, inherited tables, and
partitioned tables; and demonstrates that an attacker-schema `=` overload
changes results under an unsafe path, including when PostgreSQL re-analyzes a
statement originally prepared under an empty path. The tokens record validated
caller observations; the future session and schema runtimes must obtain them
authoritatively in fenced reads, fence the complete snapshot and schema epochs,
and enforce the invariants continuously. The router crate now composes this
proof with a decoded zero-copy Bind parameter collection, rechecks the exact
snapshot, requires the caller's current empty-path token, checks the parameter
count and selected format/NULL/value, and produces the canonical shard route.
Prepared-statement/portal generation,
backend identity, authoritative session enforcement, and execution remain
absent.

`cargo bench -p pgshard-planner --bench parse_statement` measures this parsing
boundary in isolation. `cargo bench -p pgshard-planner --bench
analyze_parameter_route` measures parsing, the catalog-bound template, and exact
parameter-type resolution. Both are informational and are not
pooler-throughput claims.

Alongside the deterministic malformed-input corpus, randomized property tests
exercise bounded arbitrary Unicode SQL and sample the supported route template
across the full legal parameter-number range, both operand orientations, and
redundant-parenthesis shapes. They also sample leading-zero and out-of-range
placeholders and verify that those forms fail closed. Router properties vary
the selected Bind position, including one-parameter messages, and cover both a
single shared format code and per-parameter format codes. PostgreSQL 18 remains
the authority for SQL semantics.
