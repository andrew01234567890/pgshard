# 8. Moving data with native logical replication

Status: accepted

## Context

Resharding copies part of a table from the shards that own a range today to
the shards that will own it. Vitess does this with its own applier: read the
source's change stream, transform, write to the target. pgshard has a
pgoutput decoder and an applier already, for table re-keying and online DDL
rewrites, so reusing it for resharding would be the smaller diff.

## Decision

Move reshard data with PostgreSQL's own logical replication: a publication
on each source restricted by a hash row filter, a subscription on each
target with `copy_data`, `streaming = parallel`, and no `two_phase`.

The row filter is what makes this work, and it is only available because
placement uses PostgreSQL's own hash (ADR 5): the server itself selects the
rows in the moving range, so a target subscribes to exactly its future
contents.

What we get by not writing the mover: the initial copy and the streaming
catch-up are one mechanism with one consistency point, `pg_subscription_rel`
reports per-table progress, apply lag is a server statistic rather than
something we estimate, and the copy survives a restart of anything we run.
An applier of ours would have to reimplement all of that, and be as correct
as the server's, for every reshard, forever.

`two_phase` is deliberately off on these subscriptions. In-doubt prepared
transactions are drained before cutover instead; a two-phase subscription
would make the target's apply worker a participant in the source's
unresolved transactions during exactly the window the cutover needs to be
able to reason about.

The Go applier stays for the cases native replication cannot express: a
table re-key writes rows to a *different* shard than the filter would send
them to, and an online rewrite writes into shadow columns. Those are
transformations, not moves.

## Consequences

- `TRUNCATE` on a table under reshard is refused with a retry hint. The
  publication carries `insert, update, delete` only, and TRUNCATE ignores
  row filters in any case, so a truncate on a source would leave the target
  holding rows the source no longer has.
- Slot creation waits for in-doubt two-phase transactions, because an
  in-doubt prepared transaction blocks logical slot creation and pins WAL.
  Resolver latency is therefore on the critical path of starting a reshard,
  not just hygiene.
- Every shard key must be a type the row filter can hash, which is the same
  constraint the router already applies for routing.
