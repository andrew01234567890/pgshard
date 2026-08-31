# 4. Connecting to shards as the client's own role

Status: accepted

## Context

The router terminates client connections and opens its own to the shards
through the pooler. Something has to decide what PostgreSQL role those
backend connections use.

The straightforward answer is a single service role with `SET ROLE` per
session. It pools perfectly: one role, one pool, any session on any backend.

## Decision

Terminate SCRAM-SHA-256 at the router against verifiers mirrored in the
catalog, recover the client key from the client's own proof, and have the
pooler authenticate to PostgreSQL **as the real user**. Pools are per user.

The reason is what `SET ROLE` leaves behind. A service role must hold the
union of every user's privileges, so the backend connection is always more
privileged than the client that is using it, and the only thing standing
between them is that the router remembered to `SET ROLE` and that nothing
in the session ever resets it. `RESET ROLE`, a `SECURITY DEFINER` function,
an error that unwinds session state — each is a path from a session that
should have had one user's rights to one that has all of them.

Roles created through pgshard apply the same pre-hashed verifier on every
group, so a role authenticates identically wherever it lands, and the
catalog is the record of what that verifier is.

The router-to-pooler hop carries the recovered client key, which is
material an attacker could replay against a shard. That link is therefore a
security boundary in its own right: mTLS, keys zeroised after use, never
logged. `SCRAM-SHA-256-PLUS`, GSS and ident are refused explicitly rather
than silently downgraded — channel binding cannot mean anything when the
channel the client bound to ends at the router.

## Consequences

- Pools are per user, so a cluster with many distinct roles holds more
  backends than one with a service role would. `work_mem` is sized from
  pool sizes rather than `max_connections` for this reason.
- Roles, and their verifiers, must be materialized on every group. That is
  what makes role DDL a cluster-wide workflow with drift repair rather than
  a statement forwarded to one server.
- A user whose password changes outside pgshard, directly on a shard, is
  now inconsistent with the catalog. The role materializer detects and
  repairs that drift; it does not prevent it.
