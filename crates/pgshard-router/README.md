# pgshard router core

This non-publishable Rust crate currently retains a small, fail-closed routing
core as test-only scaffolding for the future connection-owning router. It
attaches the exact catalog epoch, canonical version-one hash, and logical shard
to the resulting test plan, but exports no production API or dependency surface.

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
databases and tables, NULL keys, malformed lengths, ambiguous formats, invalid
text, and text containing PostgreSQL's forbidden zero byte all fail closed.

The private resolved-Bind composition path accepts the zero-copy parameter collection
only after the caller supplies the exact Parse-time route proof, the retained
catalog snapshot with the same cluster, epoch, and complete checksum, and an
empty-`search_path` token derived from a fresh authoritative backend
observation. It
requires the Bind count to match PostgreSQL's `ParameterDescription` exactly
and selects the proven parameter before applying the same routing core. It does
not consume statement or portal names. The future session layer must map those
names to the exact prepared generation, pin the same backend, retain the
snapshot/schema fences, and revalidate them through Execute; this core result is
not execution permission.

There is deliberately no public routing entry point or resolved-Bind benchmark
yet. The current physical-observation helpers validate supplied values but do
not prove that they came from PostgreSQL. The composition remains private test
scaffolding. Exposing it and benchmarking the hot path must wait for a
connection-owning physical-catalog reader that issues an opaque capability
bound to the observed backend generations. The planned end-to-end comparison
against PgBouncer remains separate.
