---
title: SQL compatibility
description: Supported and rejected PostgreSQL behavior in Milestone 1.
---

# SQL compatibility

:::warning Planned compatibility, not current support
No pooler endpoint or semantic statement planner exists yet. The source has a
byte/token/AST/stack-bounded permissive candidate parser configured with a
PostgreSQL dialect, a fail-closed core that routes an already-resolved, non-NULL
shard-key bind parameter against one immutable catalog snapshot, and a bounded
zero-copy decoder for PostgreSQL 18 frontend frames and selected simple/extended
query message bodies. The first catalog-bound template accepts only an explicitly
schema-qualified `SELECT * FROM schema.table WHERE shard_key = $n` shape (or
reversed equality), with no other clause or expression. It rejects `==` before
AST proof because PostgreSQL resolves that spelling as a distinct, potentially
custom operator while the candidate parser collapses it to the same AST node as
`=`. It is not executable until Parse parameter types/operator resolution and
the Bind value are checked. The implemented parameter-resolution stage requires
PostgreSQL's authoritative description to report the exact built-in shard-key
type OID. It also requires a proof that the physical shard-key column belongs to
the exact database, schema-qualified permanent ordinary table, and column on
every active shard, does not participate in inheritance or partitioning, and
has that exact built-in type, with built-in `C` collation and UTF8 encoding for
text. Template and proof carry the cluster identity, managed-schema epoch, and
checksum of the complete retained catalog snapshot, so a proof from another
cluster or pre/post-reshard topology cannot be reused. The
physical proof is mandatory because a ParameterDescription reports only the
parameter type: PostgreSQL can accept an explicitly typed `bigint` parameter
against a `double precision` column, round distinct large integers to the same
float, and still report parameter OID 20. The stage also requires the backend to
report an empty `search_path` immediately before Parse. With an empty path,
PostgreSQL implicitly searches `pg_catalog` for operators; an attacker-schema
`=` overload cannot shadow built-in equality.
This observation is not durable by itself: PostgreSQL can re-analyze a cached
statement under a later path and select a newly visible operator. The pooler
session runtime that keeps the path empty through Parse, Describe, Bind, and
Execute and the schema runtime that gathers and fences physical observations are
not yet implemented.
A successful syntax parse or template extraction alone is not PostgreSQL
semantic validation or permission to route. The source does not yet
authenticate or execute clients. The
table below is the Milestone 1 acceptance contract; see
[implementation status](../project/status.md).
:::

The planned pooler will speak PostgreSQL simple and extended query protocols and
support parameter-aware routing. Compatibility depends on whether a statement
can be proven to target one shard.

| Behavior | Milestone 1 target |
|---|---|
| Single-shard PostgreSQL statement | Supported, subject to managed DDL/session limits |
| Multi-row insert across shards | Supported through 2PC |
| Multi-shard update/delete with routable predicates | Supported through 2PC |
| Scatter `SELECT` whose results can be concatenated exactly | Supported |
| Cross-shard join or aggregate | Rejected |
| Global `ORDER BY`, `LIMIT`, `DISTINCT`, window, set operation | Rejected |
| Cross-shard foreign key | Rejected |
| Unique constraint without shard key | Rejected |
| Shard-key update | Rejected |
| `COPY` on a sharded table | Rejected |
| Managed online DDL | Supported subset |
| Distributed `READ COMMITTED` | Supported |
| Distributed `REPEATABLE READ` or `SERIALIZABLE` | Rejected |

## Transaction pooling limits

Safe session settings are replayed when a transaction receives a backend. Temporary objects, `LISTEN`, session advisory locks, holdable cursors, and backend-bound state are rejected because they cannot move safely between pooled connections or enter PostgreSQL prepared transactions.

Backend connections used for routed statements pin `search_path` to the empty
string. Every referenced application table must therefore be explicitly
schema-qualified, while PostgreSQL resolves unqualified operators only from its
implicitly searched `pg_catalog`. Client attempts to change `search_path` are
rejected, and the setting is read back before Parse and again before
Bind/Execute. The later check is mandatory because PostgreSQL's plan cache can
re-analyze a prepared statement when the path changes. This prevents
user-defined `=` overloads from changing a predicate that the pooler admitted as
built-in shard-key equality.

Parameter OIDs and catalog registration are not substitutes for the physical
schema proof. Before a route proof is cached, the schema manager reads the
shard-key column's exact database/schema/table/column identity,
`pg_class.relkind` and `relpersistence`, `pg_inherits` membership,
`pg_attribute.atttypid` and `attcollation`, database encoding, and shard-local
managed-schema epoch from every active shard. Only permanent ordinary tables
outside inheritance are admitted. The proof fails closed on missing, duplicate,
stale, unexpected, misidentified, view-like, foreign, unlogged, temporary,
inherited, or partitioned observations. DDL activation and Bind/Execute must
fence the exact retained snapshot checksum and schema epoch so that this proof
cannot survive a topology or physical-schema change.

The pooler pins `client_encoding` to canonical `UTF8` and rejects attempts to
change it. PostgreSQL converts both text-format and binary `text` binds from the
session encoding before storage; routing raw bytes from any other encoding can
disagree with the stored value and is therefore not allowed. Both formats also
reject the zero byte exactly as PostgreSQL does.

The decoder caps one frontend frame at 64 MiB. Startup, authentication, and
control-message families retain PostgreSQL 18's smaller family-specific limits.
It reports oversized frames before their bodies are buffered; the future
session layer must then close the client connection as a protocol violation.
The transport layer, which is not implemented yet, must handle PostgreSQL 18
direct TLS and ALPN before startup framing. It must also preserve a pipelined
TLS ClientHello after an SSL request for an accepted handshake, while rejecting
buffered bytes if encryption is refused.
An explicit replication-streaming phase admits only the CopyData, CopyDone, and
Terminate frontend frames accepted by PostgreSQL 18's WAL sender. It does not
yet decode `pgoutput` payloads or implement a change-stream session.
The syntax planner applies separate limits of 16 KiB of SQL text, 4,096 lexer
tokens, 2,048 counted AST nodes, 50 lexically nested delimiters, and 50
parser-recursion levels. The lexical guard includes candidate-only
angle-bracket `ARRAY` data types because that upstream parser path does not
consume its recursion budget; unrelated PostgreSQL comparison operators do not
consume the array-type budget. Flat binary expressions and set-operation trees
also bypass delimiter depth, so parsing, AST validation, rejection, and
destruction use a stack reserve scaled from structural tokens. Whitespace,
comments, and empty statement semicolons remain subject to the total token cap
but cannot inflate that reserve. An accepted opaque tree keeps the same reserve
for safe destruction by a caller on a smaller stack; the implementation
allocates a larger stack segment only when the current stack does not have the
required space. Larger inputs are rejected even though they
fit inside a valid frontend frame. Tokenization is byte-bounded first; only one
statement is parsed, and remaining input causes immediate multiple-statement
rejection. Parsing is synchronous; the future pooler must isolate it on a
bounded CPU worker pool instead of blocking socket-processing tasks.

Named prepared statements are virtualized at the pooler. Their routing plan is invalidated by relevant schema or routing epoch changes.

## Keys and constraints

A registered sharded table has one immutable shard-key column. Supported key
types are integer, UUID, text, and bytea. Text keys require database encoding
`UTF8` and the built-in deterministic, byte-distinguishing `C` collation on the
key column. Registration rejects ICU, nondeterministic, case-insensitive and
other collations, and a later collation change is managed DDL that fails before
activation. This ensures values equal under PostgreSQL equality cannot hash to
different shards. Primary and unique constraints include the shard key so
PostgreSQL can enforce them locally. Sequences are shard-local and do not promise
a gapless or globally ordered value stream.

## Roles and grants

Application users, role membership, grants, and default privileges are managed declaratively across all shards. Direct role/grant SQL through the application endpoint is rejected to prevent drift. Milestone 1 application roles cannot be superusers or receive replication, `BYPASSRLS`, `CREATEDB`, or `CREATEROLE` capabilities.
