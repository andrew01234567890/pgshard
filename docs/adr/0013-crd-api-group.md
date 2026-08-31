# 13. `pgshard.io` as the CRD API group

Status: accepted

## Context

Custom resources need an API group. It appears in every manifest a user
writes, in every RBAC rule, and in the storage of every cluster that
installs pgshard, and changing it later means a migration for everyone.

## Decision

`pgshard.io`, with `api/v1alpha1` as the first version.

A group is a DNS name, and it should be one whose ownership is not
ambiguous. `pgshard.github.io` reads as a page rather than a project and
ties the identity to a hosting account. `pgshard.dev` was the alternative;
`.io` is what the surrounding ecosystem uses and is the less surprising of
the two in a manifest next to `cert-manager.io` and `postgresql.cnpg.io`.

`v1alpha1` is honest about the stability of the API today, and it is the
version that lets fields change while the shape is still being learned.

## Consequences

- The group is effectively permanent. A change would require conversion
  webhooks and a migration for installed clusters.
- Promoting to `v1beta1` and `v1` is a deliberate step with conversion,
  not a rename.
