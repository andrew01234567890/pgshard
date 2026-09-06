# 15. One identity per component, and no shared root credential

Status: accepted

## Context

pgshard authenticates in three unrelated ways, and until now they shared one
secret at the bottom.

A single superuser password, in one Secret, reaches everything: member-to-member
replication conninfo, the operator's probes, the pooler's backends, the
controller's shard and subscription DSNs, and catalog bootstrap. The agent's
gRPC server — which serves `Promote`, `Demote`, `SetWriteFence`, `Reclone` and
`DropSlot` — authenticated with a bearer token and no transport security, so the
token travelled in clear on every call. And a client's SCRAM keys are forwarded
to every shard a session touches, with one verifier installed on every group.

Eight tickets describe this from eight angles (PGS-417, 418, 421, 422, 428, 235,
236, 494). Each was individually blocked on the same unmade decision, and five
partial fixes would have left the system harder to reason about than it was.

There is already a precedent in the tree, and it is the right one. The router
does **not** use the superuser to reach the catalog: it logs in as
`pgshard_router` with only the privileges it needs, because the router
terminates untrusted client connections and should hold a credential that is
only what it needs.

## Decision

**Extend that precedent to every component, and give each an issued identity.**

1. **A scoped login role per component.** Agent, pooler, controller and operator
   each get their own PostgreSQL role with only their own privileges, replacing
   superuser use wherever a narrower role suffices. No new infrastructure; it is
   what `pgshard_router` already does, component by component.

2. **An operator-issued certificate per component, and mTLS on every internal
   listener.** The cluster CA issues one certificate per role; each listener
   names the roles that may call it, and a certificate that is not in that
   listener's list is refused. This replaces the shared bearer token as the
   thing that authorises agent control.

**A staged rollout is required: a cluster must stay reachable while half its
members speak TLS and half do not.** A restart of the whole fleet is not an
acceptable way to turn security on, because the fleet restarts one member at a
time by design and the roll can be hours. Each member records what it *started*
with, the operator publishes that per shard, and every caller dials each member
by its own row rather than by what the spec asks for. Mounting the material and
requiring it stay separate acts, in that order.

**Client SCRAM key forwarding is out of scope and stays as it is** (PGS-418).
Ending it would mean giving the pooler a service identity with role assumption,
or issuing per-shard short-lived credentials — and it reverses a charter
decision made so that each shard's own privilege checks run against the real
user, which `SET ROLE` gives up. That trade deserves its own ADR and its own
argument. It is not something to smuggle in under a security heading.

## Consequences

Turning a requirement on is a rolling restart, and on upgrade it changes an
existing cluster. That is accepted: the roll is zero-downtime by construction,
and the alternative is a cluster carrying a full internal PKI that still serves
its lifecycle RPCs behind a token in clear.

A credential that leaks is now bounded by what its component may do. A change
stream consumer's certificate opens the change stream and nothing else; the
router's opens poolers and the controller but not the agents; the operator's
opens agents. Before this, each of them opened everything the superuser could.

The per-listener caller lists are a table in the code rather than configuration,
because who calls what is a property of the system: a cluster where a pooler is
called by something other than a router is a cluster with a bug, not one with a
different policy.

`internalTLS.secretRef` clusters — where certificates are supplied rather than
issued — still have to ask for the agent requirement explicitly. Supplied
certificates carry no pgshard identity to authorise, and the operator cannot
know they are present on every member.
