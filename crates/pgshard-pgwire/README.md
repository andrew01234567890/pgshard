# pgshard PostgreSQL wire framing

This non-publishable crate implements bounded, zero-copy decoding for
PostgreSQL 18 frontend and backend frames, frontend SASL responses, and selected
query-protocol bodies, plus bounded frontend startup-request, backend-control,
minimal-error, and fixed-size replication-feedback encoding. Its constants,
tags, and layouts follow the `REL_18_STABLE`
PostgreSQL server source.

The decoder recognizes startup protocol versions, SSL and GSS negotiation,
one-to-256-byte PostgreSQL 18 `CancelRequest` keys, and every frontend message
tag accepted by PostgreSQL 18. Startup packets retain PostgreSQL's 10,004-byte
total limit; fixed negotiation and cancellation-key bounds are enforced from
their eight-byte header before buffering the rest. Ordinary messages use the
stricter of PostgreSQL's small-message limit, general authentication limit, or
1,024-byte SCRAM limit and a caller-supplied large-message limit, with a hard
64 MiB pooler ceiling. Entering the SCRAM phase requires the proof returned by
a successful bounded SCRAM-advertisement encode; callers cannot construct that
phase directly. Bytes
already present after an SSL request remain unconsumed so an accepted TLS
handshake can consume an immediately pipelined ClientHello, as PostgreSQL 18
does. The eventual transport must reject those bytes if it refuses TLS and,
when it accepts TLS, feed them through the TLS stack and reject any raw bytes
left unconsumed after the handshake. Buffered data after a GSS request is
rejected because PostgreSQL 18 does not pass its receive buffer into the GSS
handshake.
PostgreSQL 18 direct TLS begins with a TLS record rather than a PostgreSQL
startup frame; the future transport must detect it before calling this decoder
and require the `postgresql` ALPN protocol during the handshake.

Frontend startup encoders emit exact fixed SSL and GSS negotiation requests,
preserve ordered and duplicate byte-string parameters in caller-buffered
protocol-three startup packets, and validate every variable packet before
touching output. PostgreSQL 18 cancellation encoding accepts only a completed
startup-protocol proof and decoded `BackendKeyData`, enforcing the server's
exact four-byte key before protocol 3.2 or 32-byte key at protocol 3.2. The
connection owner must still bind that proof and key to the exact upstream
socket; the encoders do not implement transport or cancellation routing.

This crate does not open a socket, terminate TLS, authenticate users, parse SQL,
pool connections, or proxy a message. A caller must supply the current protocol
phase; the decoder rejects illegal known tags from their first byte, before
trusting or buffering the body length. Body-specific legality still belongs to
the eventual session state machine. In the regular phase, COPY data, done, and
failure messages are admitted because PostgreSQL accepts and ignores them to
resynchronize after COPY has failed.

The backend decoder recognizes every PostgreSQL 18 server-to-client tag and
applies the stricter of a caller-selected ceiling and the tag's protocol
family bound before buffering its body. Fixed empty responses and
`ReadyForQuery` use their exact lengths, `BackendKeyData` includes a
four-to-256-byte PostgreSQL 18 cancellation key, `ParameterDescription`
includes at most 65,535 OIDs, startup authentication and protocol-negotiation
messages use libpq's 2,000-byte ceiling, other tags not classified as long
retain libpq's 30,000-byte defensive ceiling, and long row/COPY/error/notice
families remain subject to the caller ceiling no larger than 64 MiB. Unknown
tags are rejected before their length is trusted. Authentication, query-cycle,
COPY, and replication phase legality remains the future session state machine's
responsibility. The typed backend body decoders validate
`ParameterDescription` exactly, expose its type OIDs through a borrowed
fallible iterator, validate `ParameterStatus` as exactly two UTF-8 strings, borrow the
process identifier and secret key from `BackendKeyData`, validate the exact
empty-response family, decode `ReadyForQuery` as idle, in-transaction, or
failed-transaction state, and decode startup authentication and protocol
negotiation controls. Authentication decoding covers the PostgreSQL 18 request
codes and exact fixed payloads, borrows SASL mechanism lists and opaque exchange
bytes, and redacts salts, mechanism names, and exchange data from debug output.
Frontend SCRAM framing uses an encoder-issued phase proof so PostgreSQL 18's
1,024-byte limit is enforced from the length word only after this process has
produced the corresponding bounded advertisement. Typed zero-copy decoders
preserve the selected mechanism and distinguish an absent initial response
from a present empty response, then borrow subsequent opaque response bytes;
frames buffered under the broader generic authentication limit are rejected by
these typed SCRAM decoders. Their debug output reports lengths only. The future authentication state
machine must match the selected mechanism to the exact advertisement and
enforce exchange ordering.
Protocol negotiation preserves the complete backend-selected version and exact
reserved `_pq_.` option list without allocation. A linear PostgreSQL 18 startup
validator borrows the exact outbound parameters, requires negotiation precisely
when the server does, checks the exact selected version and ordered unsupported
option sequence including duplicates, and cannot be reused after a rejected
response. Finishing it before authentication yields a protocol proof that
requires PostgreSQL 18's exact four-byte cancellation key before protocol 3.2
or exact 32-byte key at protocol 3.2. `ParameterStatus` is decoded before it
establishes the authoritative encoding token so that a session can reject
anything other than canonical `UTF8`. The effective session must still bind
the protocol proof and backend key data to one exact upstream socket and
enforce authentication ordering and policy, channel binding, server identity,
and configured client-protocol policy. The frontend body decoders include
`Describe` and `Close` statement/portal targets. They do not associate a
description with a Parse generation, virtualize a name, track a query cycle,
identify a backend, or establish a catalog fence.

Backend control encoders emit exact `AuthenticationOk` and `ReadyForQuery`
arrays plus caller-buffered minimal `ErrorResponse`, SCRAM
`AuthenticationSASL`, `AuthenticationSASLContinue`,
`AuthenticationSASLFinal`, `BackendKeyData`, `ParameterStatus`, and
`NegotiateProtocolVersion` frames. SCRAM advertisements are closed to
PostgreSQL 18's SHA-256 mechanisms and put the channel-binding variant first.
The advertisement encoder returns both its initialized frame length and the
otherwise-unconstructible proof required to frame subsequent SCRAM responses.
`ErrorResponse` encoding emits canonical `S`, `V`, `C`, and `M` fields,
validates the five-byte SQLSTATE and nonempty UTF-8 message, and deliberately
omits optional diagnostics. Its explicit caller limit uses libpq's 30,000-byte
pre-authentication ceiling during startup while allowing bounded authenticated
session policy up to the pooler's 64 MiB hard limit. Variable frames are
completely sized and validated before the output is touched. They retain
PostgreSQL 18's family bounds and never include values or cancellation keys in
errors. Unsupported
protocol options are supplied as borrowed byte slices so validation and writing
need no internal allocation or replayable iterator contract. A decoded
protocol-three request can derive PostgreSQL 18's selected version directly,
including the 3.2 response to a future minor version. The live
PostgreSQL 18 trust-authentication fixture creates its protocol 3.0, 3.2, and
negotiated 3.99 outbound packets through the production startup encoder, then
decodes and re-encodes the non-SASL backend startup frames other than
`ErrorResponse` with byte-for-byte equality. The SSL, GSS, cancellation, SCRAM,
and `ErrorResponse` primitives have source-aligned unit coverage but no live
exchange yet. These primitives do not yet provide a client listener,
authentication policy, or ordered session writer.

The replication-streaming phase follows PostgreSQL 18's WAL sender COPY-BOTH
loop and accepts only frontend CopyData, CopyDone, and Terminate messages. It is
separate from COPY IN because CopyFail, Flush, and Sync are not valid standby
messages. Backend replication `CopyData` now decodes exact zero-copy XLogData
and primary-keepalive envelopes. A fixed 39-byte frontend encoder emits
PostgreSQL 18 Standby Status Updates without allocation after validating
`flush_lsn <= write_lsn` and `apply_lsn <= write_lsn`. Apply may be ahead of
flush, matching PostgreSQL's logical worker while local commits are written but
not yet durable. A per-COPY-BOTH-session progress state machine rejects
cross-sample write, flush, or apply regression all-or-nothing. It cannot be
cloned or reset. The feedback owner must advance flush only for a durable
checkpoint, discard volatile write and apply progress after disconnect, and
start a new tracker from that durable checkpoint.
The live PostgreSQL 18 fixture sends this production frame in COPY-BOTH mode,
drains the initial catch-up keepalive, observes three distinct positions through
`pg_stat_replication`, receives a subsequent requested keepalive, and completes
the replication exchange cleanly.
A configuration token validates `pgoutput` protocol versions one through four
plus streaming, custom-message, and two-phase feature gates;
the control decoder covers buffered Begin/Commit/Origin, streamed transaction
start/stop/commit/abort, and every two-phase control including Stream Prepare.
It requires authoritative `server_encoding=UTF8` and `client_encoding=UTF8`
proofs from the same connection, bounds prepared-transaction identifiers, and
redacts origins, GIDs, names, and logical payloads from debug output. A
stateful wrapper derives whether the otherwise-ambiguous XID prefix is present
from validated Stream Start/Stop controls and decodes Relation and Type schema
messages, Insert/Update/Delete/Truncate row changes, and custom logical Message
records only when the exact accepted command enabled `messages`. A Stream Start
names the top-level transaction, while schema and row prefixes may name a
different nonzero subtransaction. A custom Message inside a stream must be
transactional and repeat the active top-level XID; PostgreSQL 18 attributes the
tested Message emitted inside a savepoint to that top-level transaction rather
than the savepoint Relation XID. Relation columns and logical tuples are
prevalidated once and exposed through borrowed fallible iterators. Tuple
values distinguish null, unchanged-toast, UTF-8 text, and opaque binary without
copying or rendering values in debug output. Custom Message prefixes require
the connection UTF-8 proof; their binary contents remain borrowed and are
represented only by length in debug output.
Every Begin and streamed transaction identifier must be nonzero. Validated
startup, query, backend, relation, tuple, and truncate iterators use checked
slicing and return `ValidatedIteratorError` if their private construction
invariants are ever violated, rather than silently shortening a message or
retaining a latent panic path.
Complete transaction ordering, relation cache semantics, feedback scheduling
and persistence, durable checkpoints, cross-shard merge, and the VStream-like
service remain later work.

The future replication session must bind that token to the exact accepted
`START_REPLICATION` command and the selected slot's authoritative persistent
`two_phase` state rather than trusting caller-selected options. PostgreSQL keeps
two-phase decoding enabled after a later `two_phase=false` start unless the
slot itself is explicitly disabled, so the effective decoder gate is the
logical OR of the request and slot states.

Debug output reports only frame metadata and lengths. It never renders startup
values, cancellation authentication keys, SQL, authentication data, error
fields, rows, or other frontend/backend bodies.

Query-protocol C-strings require the validated UTF-8 session proof, are checked
as UTF-8, and are exposed as `&str`. Parameter value bytes remain opaque until
their declared PostgreSQL types and text/binary formats are resolved.

Four `cargo-fuzz` targets exercise startup framing, every frontend phase and
typed frontend body, backend framing and typed backend bodies, plus stateful
`pgoutput` sequences across a branch-covering matrix containing all twelve
streaming/two-phase/message behaviors plus every protocol version and both
two-phase provenance paths. Every fallible borrowed iterator is traversed. A
minimized valid corpus for each target is committed so a clean checkout
immediately exercises deep typed messages and iterators. GitHub Actions runs the
four targets as parallel jobs for 10,000 inputs on pgwire-affecting pull
requests and main-branch pushes, and 100,000 inputs on the scheduled workflow,
using pinned `cargo-fuzz` and nightly toolchains.
Run one target locally from this crate with, for example:

```console
cargo +nightly-2026-06-24 fuzz run decode_frontend -- -runs=10000 -max_len=65536 -timeout=10
```

`cargo bench -p pgshard-pgwire --bench decode_frontend` measures framing alone.
`cargo bench -p pgshard-pgwire --bench decode_bind` measures framing plus a
four-parameter extended-query bind.

`cargo bench -p pgshard-pgwire --bench decode_pgoutput_control` measures a
borrowed transaction Begin control.
`cargo bench -p pgshard-pgwire --bench decode_pgoutput_relation` measures
prevalidation and iteration of a borrowed two-column Relation message.
`cargo bench -p pgshard-pgwire --bench decode_pgoutput_insert` measures a
borrowed two-column Insert and tuple iteration.
`cargo bench -p pgshard-pgwire --bench decode_pgoutput_message` measures a
borrowed custom logical Message with an opaque binary payload. None is a
substitute for the planned end-to-end pooler/PgBouncer comparison.
