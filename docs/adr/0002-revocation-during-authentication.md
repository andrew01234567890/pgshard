# 2. What a client learns when a revocation lands mid-authentication

Status: accepted

## Context

`TerminateWhere` ends every session whose role has been withdrawn, and a
session that is still authenticating is included: the claimed role is
published before the exchange begins, and a latch on the session records
the revocation so the session cannot go on to serve on a verifier the
cluster has since withdrawn.

The latch was consulted after `Authenticate` returned. Two differences
follow from that, and they are not the same kind of difference.

**The transcript is not byte-identical to an unknown role.** SCRAM flushes
`AuthenticationSASLFinal` -- the server signature -- as soon as the client
proof verifies, which happens inside `Authenticate`. A client whose proof
is correct therefore sees `SASLFinal` and then the generic FATAL, while an
unknown role, driven through the mock exchange, never sees that frame.

**A failing exchange reported its own reason.** The error branch ran before
the latch was consulted, so a revocation landing on a session that was
about to be told "role is not permitted to log in" or "password expired"
reported that instead.

## Decision

**The `SASLFinal` difference is accepted and left alone.** Reaching that
frame requires producing a correct client proof, which is a demonstration
that the client already holds valid credentials for the role. The
distinction it reveals -- that the role existed and the password was right
-- is a thing that client had already proved to itself before the frame
arrived. Closing it means holding or discarding a protocol frame from
inside the SCRAM state machine on the strength of session state the
authenticator has no business reading, and that coupling costs more than
the difference is worth.

**The error branch is normalised.** Once the latch is set the session ends
as a revoked session, whatever the exchange happened to be failing on.
This is not a disclosure fix: `NOLOGIN` and expiry are relayed to clients
by design elsewhere. It is a consistency one -- a session the cluster has
just withdrawn must not be handed a description of the role it no longer
has, and a revoked session should end the same way whichever branch it was
in when the revocation arrived.

## Consequences

A client that authenticates successfully into a revocation still sees a
successful SASL exchange followed by `FATAL: password authentication
failed`. Every other revoked session, whatever it was doing, sees the same
message and the same code. The authenticator stays unaware of session
lifecycle state.
