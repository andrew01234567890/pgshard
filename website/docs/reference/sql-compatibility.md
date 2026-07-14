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
zero-copy decoder for PostgreSQL 18 frontend/backend frames, selected
simple/extended frontend bodies including `Describe`/`Close`, exact backend
`ParameterDescription` metadata, empty completions, and `ReadyForQuery`
transaction status. The first catalog-bound template accepts only an explicitly
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
statement under a later path and select a newly visible operator. The implemented
resolved-Bind core rechecks the exact retained snapshot, accepts a validated
empty-path token that the caller must rebuild from the current backend, and
checks PostgreSQL's authoritative parameter count plus the selected
format/NULL/value bytes without copying before producing a canonical shard
route. It intentionally does not accept or trust statement and portal names.
The backend codec supplies framing, borrowed type OIDs, completion-body
validation, transaction-status bytes, and bounded startup-control encoders
only; it does not prove that a description belongs to the relevant Parse,
backend, or session, or track a query cycle.
The pooler session runtime that maps those names to an exact prepared
generation, pins the same backend, keeps the path empty through Parse, Describe,
Bind, and Execute, and retains the snapshot/schema fences is not yet
implemented. Neither is the schema runtime that gathers and fences physical
observations.
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
SCRAM responses use a dedicated authentication phase that applies PostgreSQL
18's 1,024-byte limit from the length word before buffering.
It reports oversized frames before their bodies are buffered; the future
session layer must then close the client connection as a protocol violation.
Backend framing likewise applies exact fixed-message and
`ParameterDescription` maxima, PostgreSQL 18's four-to-256-byte cancellation-key
bound to `BackendKeyData`, libpq's 2,000-byte ceiling to startup authentication
and protocol-negotiation responses, its 30,000-byte ceiling to the remaining
tags it does not classify as long, and the configured ceiling only to long
row/COPY/error/notice families. These checks happen from the backend header
before an upstream body is buffered.
PostgreSQL 18 separately accepts one-to-256-byte keys in incoming
`CancelRequest` packets. The future session layer must match the complete
opaque key to the selected backend connection and enforce the effective
protocol version. A completed PostgreSQL 18 startup proof now requires the
server's exact four-byte backend key before protocol 3.2 or its exact 32-byte
key at protocol 3.2. The cancellation encoder requires that proof and decoded
`BackendKeyData` before producing a request, but the proof and key are not yet
bound to a live pooled socket or exposed through cancellation routing.
Typed zero-copy decoders expose the process identifier and opaque key without
rendering the key in debug output, and validate `ParameterStatus` as exactly
two terminated UTF-8 strings. A reported `client_encoding` is authoritative
session state: the pooler must reject a value other than canonical `UTF8`
before decoding more query-protocol bodies.
Typed startup-control decoders also validate PostgreSQL 18 authentication
request layouts, counted and terminated SASL mechanism lists, opaque exchange
payloads, and `NegotiateProtocolVersion` responses. Authentication salts,
mechanism names, and exchange data are omitted from debug output. Negotiation
decoding preserves the complete major/minor protocol code and requires every
reported option name to use the reserved `_pq_.` prefix. A separate linear
PostgreSQL 18 validator borrows the exact outbound startup parameters and
requires a negotiation response exactly when the requested minor version is
newer than 3.2 or a reserved option is present. It accepts only the server's
exact selected version and the complete unsupported-option name sequence in
request order, including duplicates. Missing and duplicate responses fail, and
an invalid response consumes the validator so it cannot be retried into an
authenticated proof. The future socket session must still enforce transport
and authentication order, authentication policy, channel binding, server
identity, rejection of reserved protocol 3.1 as client policy, and the
configured minimum protocol version.
Fixed frontend encoders cover SSL and GSS negotiation requests. A
caller-buffered regular-startup encoder accepts protocol major three, preserves
ordered and duplicate byte-string parameters including empty values, and
enforces PostgreSQL 18's 10,004-byte total bound before changing output. The
live PostgreSQL 18 fixture uses it for protocol 3.0, 3.2, and negotiated 3.99
connections. Transport policy and direct-TLS ALPN remain outside this encoder.
Exact control encoders cover `AuthenticationOk`, minimal `ErrorResponse`,
`ParameterStatus`, `BackendKeyData`, `NegotiateProtocolVersion`, and every
`ReadyForQuery` transaction state. They also cover PostgreSQL 18's ordered
SCRAM-SHA-256 advertisement and opaque SASL continue/final frames. Frontend
typed decoders borrow the selected SASL mechanism, preserve absent versus empty
initial data, and borrow follow-up response bytes without rendering them.
Minimal errors contain canonical localized and nonlocalized severity, SQLSTATE,
and primary-message fields; optional diagnostic fields are not encoded yet.
The caller supplies the phase-appropriate error limit: 30,000 bytes before
authentication or a bounded authenticated-session policy no greater than the
64 MiB pooler ceiling. Caller-buffered encoders validate the complete bounded
frame before changing output and redact payloads from errors. The PostgreSQL 18
fixture requires non-SASL startup-control output other than `ErrorResponse` to
equal live server bytes; the SCRAM and error primitives currently have
source-aligned unit coverage only. No ordered session writer, SCRAM
cryptographic state machine, or client-facing startup exchange exists yet.
The transport layer, which is not implemented yet, must handle PostgreSQL 18
direct TLS and ALPN before startup framing. It must also preserve a pipelined
TLS ClientHello after an SSL request for an accepted handshake, while rejecting
buffered bytes if encryption is refused.
An explicit replication-streaming phase admits only the CopyData, CopyDone, and
Terminate frontend frames accepted by PostgreSQL 18's WAL sender. Server
CopyData bodies decode as exact borrowed XLogData or keepalive envelopes. A
fixed-size frontend encoder emits exact Standby Status Update CopyData frames
after validating within-sample `flush <= write` and `apply <= write` ordering.
Apply may exceed flush as PostgreSQL's logical worker permits for locally
written, unflushed commits. A live PostgreSQL 18 COPY-BOTH fixture proves the
server accepts the production frame, exposes three distinct positions, and
sends a requested keepalive after the initial catch-up keepalive is drained.
A state machine scoped to one COPY-BOTH session rejects cross-sample write,
flush, and apply regression all-or-nothing. On reconnect it starts all three
positions from the last durable checkpoint instead of restoring volatile write
or apply progress. Feedback scheduling and durable-checkpoint binding remain
the future replication session's responsibility.
An authoritative client/server UTF-8 proof and validated `pgoutput` v1-v4
configuration gate streamed and two-phase controls, and the control decoder
covers Begin, Commit, Origin, Stream Start/Stop/Commit/
Abort, Begin Prepare, Prepare, Commit Prepared, Rollback Prepared, and Stream
Prepare. A stateful decoder derives the optional XID layout from Stream
Start/Stop and decodes borrowed Relation and Type names, replica identity, and
prevalidated Relation columns plus Insert, Update, Delete, and Truncate bodies.
Borrowed tuple columns distinguish null, unchanged-toast, UTF-8 text, and
opaque binary values. Custom logical Message records expose a validated UTF-8
prefix and borrowed opaque contents without rendering either in debug output,
but only when the exact accepted command enabled `messages`. Stream Start
carries the top-level transaction XID while schema and row records may carry a
subtransaction XID. A custom Message inside the segment must be transactional
and repeat the active top-level XID. In the live PostgreSQL 18 fixture, a
Message emitted inside a savepoint retains that top-level XID while the
Relation record carries the savepoint XID. Decoder state proves segment layout,
not complete transaction order: there is no relation cache, feedback runtime or
scheduler, durable checkpoint, slot lifecycle, snapshot, cross-shard merge, or
change-stream service yet.

The future replication session must construct and bind that configuration only
after the exact `START_REPLICATION` command enters COPY-BOTH mode and it has
authoritatively read the selected replication slot's persistent `two_phase`
state. PostgreSQL's effective gate is the logical OR of that slot state and the
new request: a later `two_phase=false` request does not disable an already
enabled slot. The decoder does not prove server acceptance or slot state from
caller-selected values by itself.
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
