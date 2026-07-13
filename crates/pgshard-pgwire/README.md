# pgshard PostgreSQL wire framing

This non-publishable crate implements bounded, zero-copy decoding for
PostgreSQL 18 frontend and backend frames and selected query-protocol bodies.
Its constants, tags, and layouts follow the `REL_18_STABLE` PostgreSQL server
source.

The decoder recognizes startup protocol versions, SSL and GSS negotiation,
one-to-256-byte PostgreSQL 18 `CancelRequest` keys, and every frontend message
tag accepted by PostgreSQL 18. Startup packets retain PostgreSQL's 10,004-byte
total limit; fixed negotiation and cancellation-key bounds are enforced from
their eight-byte header before buffering the rest. Ordinary messages use the
stricter of PostgreSQL's small-message limit or authentication limit and a
caller-supplied large-message limit, with a hard 64 MiB pooler ceiling. Bytes
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
iterator, validate `ParameterStatus` as exactly two UTF-8 strings, borrow the
process identifier and secret key from `BackendKeyData`, validate the exact
empty-response family, decode `ReadyForQuery` as idle, in-transaction, or
failed-transaction state, and decode startup authentication and protocol
negotiation controls. Authentication decoding covers the PostgreSQL 18 request
codes and exact fixed payloads, borrows SASL mechanism lists and opaque exchange
bytes, and redacts salts, mechanism names, and exchange data from debug output.
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

The replication-streaming phase follows PostgreSQL 18's WAL sender COPY-BOTH
loop and accepts only frontend CopyData, CopyDone, and Terminate messages. It is
separate from COPY IN because CopyFail, Flush, and Sync are not valid standby
messages. Backend replication `CopyData` now decodes exact zero-copy XLogData
and primary-keepalive envelopes. A configuration token validates `pgoutput`
protocol versions one through four plus streaming and two-phase feature gates;
the control decoder covers buffered Begin/Commit/Origin, streamed transaction
start/stop/commit/abort, and every two-phase control including Stream Prepare.
It requires the authoritative UTF-8 session proof, bounds prepared-transaction
identifiers, and redacts origins, GIDs, and logical payloads from debug output.
Row, relation, type, truncate, and logical-message bodies have distinct tags but
are deliberately rejected until their dedicated decoders exist. Transaction
ordering, relation state, WAL feedback, durable checkpoints, cross-shard merge,
and the VStream-like service remain later work.

The future replication session must bind that token to the exact accepted
`START_REPLICATION` command rather than trusting caller-selected options.

Debug output reports only frame metadata and lengths. It never renders startup
values, cancellation authentication keys, SQL, authentication data, error
fields, rows, or other frontend/backend bodies.

Query-protocol C-strings require the validated UTF-8 session proof, are checked
as UTF-8, and are exposed as `&str`. Parameter value bytes remain opaque until
their declared PostgreSQL types and text/binary formats are resolved.

`cargo bench -p pgshard-pgwire --bench decode_frontend` measures framing alone.
`cargo bench -p pgshard-pgwire --bench decode_bind` measures framing plus a
four-parameter extended-query bind.

`cargo bench -p pgshard-pgwire --bench decode_pgoutput_control` measures a
borrowed transaction Begin control. None is a substitute for the planned
end-to-end pooler/PgBouncer comparison.
