# Threat model

This is the public threat model for the planned system and repository. It
identifies assets and required controls; it is not evidence that a control has
already been deployed.

## Assets

- PostgreSQL user data, WAL, backups, and recovery metadata;
- transaction prepare and commit decisions;
- control-group membership, topology, health, and fencing state;
- client credentials, TLS keys, service identities, and operator credentials;
- source code, CI credentials, release artifacts, and public test fixtures.

## Actors and trust boundaries

- An unauthenticated or compromised client can send malformed SQL, replay
  requests, or attempt to reach a non-serving member.
- A compromised application or operator credential can abuse the permissions
  granted to its service identity.
- A failed, partitioned, stale, or compromised member can advertise an old
  primary role or return incomplete data.
- A malicious contributor or compromised CI dependency can attempt to publish
  secrets or alter release artifacts.
- Backup and archive storage is a separate trust boundary; possession of an
  archive credential must not grant control-plane or write authority.

## Required controls

### Client and service access

Authenticate clients and services, use encrypted transport where supported,
and give each identity the minimum database and control-plane permissions it
needs. Keep data access, topology mutation, fencing, backup, and release
credentials separate. Do not log passwords, tokens, private keys, or raw
customer data.

### Writer safety and transactions

Write routing must consult live health and fencing state, reject stale
leadership, and fence an old writer before publishing a new one. Two-phase
recovery must follow a durable coordinator decision; an unknown result is not
an automatic rollback. These controls protect against split-brain writes and
inconsistent transaction outcomes.

### Replicas and recovery

VStream-style reads and bulk copy must be authorized only against eligible
replicas. Certified-point PITR must verify the required group boundaries and
durable records before exposing a recovery point. A backup credential must not
be able to alter topology or transaction decisions.

### Public repository and CI

Use public or synthetic data only. Never commit secrets, customer information,
private endpoints, or production logs. Scope CI tokens narrowly, mask them in
logs, pin or review third-party actions, and treat generated artifacts as
untrusted until verified. See [CONTRIBUTING.md](../CONTRIBUTING.md) and
[SECURITY.md](../SECURITY.md).

## Out of scope

This document does not certify a cloud provider, Kubernetes distribution,
network fabric, PostgreSQL extension, backup vendor, or operator deployment.
Those integrations need their own threat review and release evidence. It also
does not claim protection from a fully trusted administrator intentionally
misusing already-authorized access; least privilege and auditability reduce
that risk but do not eliminate it.
