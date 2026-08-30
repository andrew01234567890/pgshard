# 11. Declaring the shard layout as SQL in the `pgshard` database

Status: accepted

## Context

The shard layout — which tables are sharded, by which key, and which shard
owns which range — has to be declared somewhere. Vitess uses a VSchema
document applied with a CLI. A Kubernetes-native project would reach for
more CRDs.

## Decision

The layout lives in tables in the `pgshard` database, edited with ordinary
SQL through the router, split the way Kubernetes splits objects:
user-editable desired state (`pgshard.databases`, `pgshard.tables`,
`pgshard.shard_ranges`) and system-owned status (`pgshard.table_status`,
`pgshard.shard_status`, `pgshard.workflows`).

The audience decides this. The person who declares that `orders` is sharded
by `tenant_id` is the person who wrote `CREATE TABLE orders`, and they
already have a SQL connection, a transaction, and a way to review a change.
Asking them to learn a document format and a CLI to say one more thing
about a table they just created adds a tool without adding an ability.

Being tables rather than a document also gives the validation somewhere to
live: constraint triggers check that the key column exists, that ranges are
contiguous and non-overlapping, that the hash version is known. A bad
declaration is refused by the statement that made it, in the transaction
that made it, with a SQLSTATE — not accepted and reconciled into an error
condition later.

Desired and status are separate because routing must never follow an
intention. A committed change bumps `desired_generation` and notifies; the
controller reconciles and starts a workflow; **routing follows only the
effective map in the status tables**. A pending declaration changes nothing
about where a row goes.

`PgShardCluster.spec.shards`, when set, is materialized by the operator
into `pgshard.shard_ranges` as N equal ranges. The CRD stays authoritative
for hardware; the tables stay authoritative for layout. An operator who
wants uneven ranges omits `spec.shards` and edits the table.

## Consequences

- The router must serve the `pgshard` database itself, including DDL
  against the catalog and the constraint triggers' errors, as ordinary SQL.
- The desired-state tables are an API with compatibility obligations, and
  their schema migrations are versioned like any other catalog change.
- Two sources of truth exist for shard count when `spec.shards` is set. The
  operator materializes one into the other in a single direction, and a
  disagreement is resolved by the CRD, which is why editing ranges by hand
  requires unsetting it.
