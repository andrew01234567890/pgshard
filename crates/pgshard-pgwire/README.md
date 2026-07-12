# pgshard PostgreSQL wire framing

This non-publishable crate implements bounded, zero-copy decoding for
PostgreSQL 18 frontend frames. Its constants and length rules follow the
`REL_18_STABLE` PostgreSQL server source.

The decoder recognizes startup protocol versions, SSL and GSS negotiation,
variable-length PostgreSQL 18 cancellation keys, and every frontend message tag
accepted by PostgreSQL 18. Startup packets retain PostgreSQL's 10,004-byte total
limit. Ordinary messages use the stricter of PostgreSQL's small-message limit
or authentication limit and a caller-supplied large-message limit, with a hard
64 MiB pooler ceiling. Plaintext already present in the supplied buffer after
an SSL or GSS negotiation request is rejected. The eventual transport layer
must also reject plaintext that arrives before it begins the accepted handshake
to preserve PostgreSQL 18's complete protocol-confusion defense.

This crate does not open a socket, terminate TLS, authenticate users, parse SQL,
pool connections, or proxy a message. A caller must supply the current protocol
phase; the decoder rejects illegal known tags from their first byte, before
trusting or buffering the body length. Body-specific legality still belongs to
the eventual session state machine.

Debug output reports only frame metadata and lengths. It never renders startup
values, cancellation authentication keys, SQL, authentication data, or other
frontend bodies.

`cargo bench -p pgshard-pgwire --bench decode_frontend` measures framing alone.
It is not a substitute for the planned end-to-end pooler/PgBouncer comparison.
