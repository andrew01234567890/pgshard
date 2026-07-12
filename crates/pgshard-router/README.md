# pgshard router core

This non-publishable Rust crate implements the small, fail-closed routing core
used after a caller has resolved a registered table and its shard-key bind
parameter. It attaches the exact catalog epoch, canonical version-one hash, and
logical shard to the resulting plan.

This is not a pooler. It does not parse SQL, speak the PostgreSQL wire protocol,
manage backend connections, identify a statement's shard-key expression, or
execute a request. Until those layers exist, no client can use this crate as a
database endpoint.

`bigint`, UUID, and `bytea` values require PostgreSQL binary bind format. The
crate intentionally does not approximate PostgreSQL's text input grammars.
Every route requires proof that the frontend session is pinned to canonical
`client_encoding=UTF8`. PostgreSQL converts both text and binary `text` binds
from the session encoding before storage, so hashing raw bytes from any other
encoding would be incorrect. With the UTF-8 session proof, `text` accepts either
bind format after strict UTF-8 validation because the catalog contract also
requires UTF-8 and the built-in `C` collation. Unknown
databases and tables, NULL keys, malformed lengths, ambiguous formats, and
invalid text all fail closed.

`cargo bench -p pgshard-router --bench route_bound_parameter` measures the
complete immutable-snapshot lookup, decode, hash, and range-routing core. It is
a microbenchmark, not the planned end-to-end comparison against PgBouncer.
