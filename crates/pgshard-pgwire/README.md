# pgshard PostgreSQL wire framing

This non-publishable crate implements bounded, zero-copy decoding for
PostgreSQL 18 frontend frames. Its constants and length rules follow the
`REL_18_STABLE` PostgreSQL server source.

The decoder recognizes startup protocol versions, SSL and GSS negotiation,
variable-length PostgreSQL 18 cancellation keys, and every frontend message tag
accepted by PostgreSQL 18. Startup packets retain PostgreSQL's 10,004-byte total
limit. Ordinary messages use the stricter of PostgreSQL's small-message limit
or authentication limit and a caller-supplied large-message limit, with a hard
64 MiB pooler ceiling. Bytes already present after an SSL request remain
unconsumed so an accepted TLS handshake can consume an immediately pipelined
ClientHello, as PostgreSQL 18 does. The eventual transport must reject those
bytes if it refuses TLS and, when it accepts TLS, feed them through the TLS
stack and reject any raw bytes left unconsumed after the handshake. Buffered
data after a GSS request is rejected because PostgreSQL 18 does not pass its
receive buffer into the GSS handshake.
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

The replication-streaming phase follows PostgreSQL 18's WAL sender COPY-BOTH
loop and accepts only frontend CopyData, CopyDone, and Terminate messages. It is
separate from COPY IN because CopyFail, Flush, and Sync are not valid standby
messages. This is only a framing contract; logical-replication message decoding
and durable stream state remain later work.

Debug output reports only frame metadata and lengths. It never renders startup
values, cancellation authentication keys, SQL, authentication data, or other
frontend bodies.

`cargo bench -p pgshard-pgwire --bench decode_frontend` measures framing alone.
It is not a substitute for the planned end-to-end pooler/PgBouncer comparison.
