# 9. Rewriting tables in place rather than swapping them

Status: accepted

## Context

Some DDL rewrites the table: a type change that is not binary coercible, a
column with a volatile default, `SET LOGGED`, a primary key change. Under
plain PostgreSQL each takes `ACCESS EXCLUSIVE` for the length of the
rewrite, which is downtime.

The established answer is the shadow-table swap: build a copy, keep it
current with triggers, `RENAME` the two, drop the old one. `pt-online-schema-change`
and `gh-ost` work this way, and it is what most people mean by online DDL.

## Decision

Do rewrites **in place, preserving the table's OID**, pgroll-style: add a
working column, dual-write it with a trigger, backfill in batches, then
rename and drop within a short lock. The router hides working columns from
`SELECT *` and from `INSERT` without a column list, so clients see the
table they declared throughout.

A `RENAME` swap changes which OID the table's name resolves to, and in this
system that OID is load-bearing:

- Publications name relations by OID. A swapped table silently leaves the
  publication, so a VStream consumer stops receiving it and a reshard in
  flight stops copying it — with no error anywhere, because dropping out of
  a publication is not an error.
- A subscription's `pg_subscription_rel` state is per relation OID. The
  target of a reshard would keep reporting the old relation as ready.
- Replica identity, row filters and column lists are all attached to the
  relation, not the name.

The failure mode is the problem: a swap does not break, it goes quiet. On a
system whose resharding and change-stream guarantees both rest on
publications, "quiet" is the worst available behaviour.

Where PostgreSQL 19's `REPACK (CONCURRENTLY)` covers a rebuild, use it: it
is the same principle done by the server. A shadow-table swap remains for
what cannot be done in place at all, and there the publication refresh is
explicit rather than assumed.

## Consequences

- A rewrite needs room for the working column and its backfill, and the
  backfill competes with the workload; it is throttled and resumable rather
  than fast.
- The router must know a table is mid-rewrite to hide the working columns,
  so a rewrite is visible in the routing snapshot, not just in the
  migration table.
- `docs/online-ddl.md` documents which statements take which strategy. The
  classifier picks per statement; the operator does not choose.
