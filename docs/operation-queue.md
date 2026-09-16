# The operation queue

Four kinds of work change the shape of a cluster: DDL and DCL migrations,
reshards, major upgrades, and table placement moves. They cannot all run at
once — logical replication carries no DDL, a placement's swap must not race
a statement against its table, and two reshards would fight over the same
groups — so one of them has to wait for another.

Each of them used to decide that on its own, by counting what was running.
A reshard started its copy when no migration was running, so it overtook one
that was queued; the router refused DDL outright while a reshard was in
flight; a migration started again the moment a reshard switched, and broke
its reverse replication; nothing kept DDL off a table a placement was
moving. The order the work arrived in was not recorded anywhere, so none of
it could wait its turn.

Now every operation takes a place in one queue.

## The rule

Every migration, workflow and shard set draws an `arrival` from
`pgshard.operation_arrival` when it is created. One rule decides whether an
operation may start:

> Nothing that conflicts with it has started and is unfinished, and nothing
> that conflicts with it arrived earlier, is unfinished, and holds its
> place.

Two properties follow, and both matter:

- **A started operation never waits.** It only ever appears as something
  others wait for. So the waits point from not-yet-started operations to
  started or earlier ones, and can never form a cycle.
- **Not everything unfinished holds the queue.** A paused operation does
  not, nor a reshard row nothing drives (an in-place `pgshard.shard_ranges`
  edit, which has no source set), nor one whose own pre-start step keeps
  failing. Each of those would otherwise hold every later operation
  indefinitely. When such an operation goes on, it waits for whatever
  started meanwhile — an order inversion, and a safe one.

`pgshard.operations` is the view of unfinished operations with what the rule
reads; `pgshard.operations_conflict` decides whether two of them may
overlap; `pgshard.operation_blockers(kind, id)` answers, for one operation,
what it is waiting for, oldest first.

## What conflicts with what

| | DDL | reshard / upgrade | table placement |
|---|---|---|---|
| **DDL** | same database, or either is about roles or databases | always | the placement's database, until it has swapped |
| **reshard / upgrade** | always | always | always |
| **table placement** | the placement's database, until it has swapped | always | never |

Role and database statements (`CREATE ROLE`, `GRANT`, `CREATE DATABASE`, …)
are cluster-scoped: a `GRANT` in any database can name the role a `CREATE
ROLE` makes, so they keep their order against DDL in every database. Two
placements never conflict, because the per-table workflow lock already keeps
two off one table.

DDL on a **local** database or in a local schema never becomes a migration —
it runs on its home shard directly — so it cannot wait its turn. It is
refused while a reshard or upgrade has work left, or while a placement is
moving a table of that database (`pgshard.home_ddl_blockers`).

## What a client sees

A statement that has to wait says so and keeps waiting:

```
=> CREATE INDEX CONCURRENTLY orders_note_idx ON orders (note);
NOTICE:  migration 9a1d4b22-… waits for reshard 7c0f1a90-… (in progress)
```

It answers with the command tag when the migration completes, exactly as a
statement that never waited does. Three things can end the wait early:

- **`statement_timeout`.** The session gets `57014`, with a DETAIL saying
  the migration continues in the background and that running the same
  statement again waits for it. The migration keeps its place in the queue.
- **Cancelling the statement.** The same, with "canceling statement due to
  user request".
- **No controller.** If no applier has beaten `pgshard.controller_heartbeat`
  recently, the session gets `55000` rather than waiting on work nothing
  will pick up.

`SET pgshard.ddl_async = on` returns as soon as the statement is enqueued,
with a NOTICE naming the migration id.

## The same statement sent twice

A migration carries a `dedup_key`: a hash over the database, the statement
in its deparsed form, the kind, the strategy, the scope, and the canonical
metadata. Whitespace, case and comments do not change it; a password or a
SCRAM verifier suppresses it entirely, because a hash of a secret is one a
reader could test guesses against.

Enqueueing takes `pg_advisory_xact_lock` and looks for an identical
migration that is queued, running, or completed within the retry window
(10 minutes by default). If it finds one, the session attaches to it and
waits instead of enqueueing a second:

```
NOTICE:  an identical migration 9a1d4b22-… is already queued; waiting for it
         instead of queueing the statement again
```

So the ordinary retry loop — a five-minute client timeout, a retry, another
timeout — builds the index once, on the slot the first attempt took, rather
than once per attempt.

Attaching is refused when a **different** migration arrived after the
candidate in the same scope: otherwise a `CREATE`, `DROP`, `CREATE` sequence
would collapse its last step into its first. A failed migration never stands
in for a new one, so a statement run again after a failure runs again.
`SET pgshard.ddl_dedup = off` turns attaching off for a session.

A statement whose **second run is the work again** never attaches to a
completed migration, however recent: `REINDEX`, `VACUUM` and `ALTER
SEQUENCE`. Running one of those is asking for its effect now, against the
state now — an `ALTER SEQUENCE … RESTART` sent again after the application
has consumed a few thousand values is not the earlier one repeated.
Attaching to a migration that is still **queued or running** is always
right, for any kind: the work has not happened yet.

The promise is only made where the catalog can keep it. A catalog without
the operation queue has no `dedup_key` column, so the enqueue drops the key
and the timeout's DETAIL says only that the migration continues — telling a
client its retry will wait, where nothing can attach, is how the retry runs
the statement twice.

## Reading the queue

`pgshard.operation_queue` is the queue in arrival order, in one row per
operation, with the command in readable form, what it waits for, and how far
along it is:

```sql
SELECT position, kind, command, state, waiting_for, progress_bar, detail
  FROM pgshard.operation_queue;
```

```
 position |  kind   |           command            |  state  |       waiting_for        |        progress_bar
----------+---------+------------------------------+---------+--------------------------+-----------------------------
        1 | reshard | reshard 2 to 4 shards        | running |                          | [#####---------------]  28%
        2 | ddl     | CREATE INDEX orders_note_idx | waiting | reshard 7c0f1a90 (in …)  | [--------------------]   0%
```

The view never shows a statement that sets a password or a verifier; those
rows carry the command only. The admin UI serves the same rows at `/queue`
and `/api/v1/queue` ([admin.md](admin.md), [guide/admin-ui.md](guide/admin-ui.md)).

Every entry carries what it waits for, and a queue `N` deep behind one
reshard holds `O(N²)` of those between them, so a reader takes the head of
the queue rather than all of it: the admin reads the first 200 entries, with
at most 20 blockers each, and `pgshard.operation_queue_depth()` for the
total, so a long queue is reported as long rather than served in full to a
page nobody can read. Reading the view directly is unbounded, which is fine
for `psql` and is not what a page refreshing every two seconds should do.

## Where the gates are

Every operation asks `pgshard.operation_blockers` in the statement or
transaction that starts it, under the move gate lock, so two controllers
cannot both find the way clear:

| Operation | Gate |
|---|---|
| DDL migration | `catalog.SaveQueuedMigrationProgress`, which also checks the serving set and the home shard, and stamps `started_at` |
| Reshard / upgrade copy | `controller.Copier.readyToStart` before `startCopy` |
| Table placement | `controller.Placer.prepareAndStart`, rechecked at `start` against what the plan described |
| Local-database DDL | `plan.HomeDDL` in the router, against `pgshard.home_ddl_blockers` |

A component older than its catalog keeps the gates it had before:
`catalog.QueueSchema` reports whether the queue exists, and every gate falls
back when it does not.

## See also

- [ddl.md](ddl.md) — how a migration is applied once it is its turn
- [resharding.md](resharding.md) — what a reshard does while it holds the queue
- [upgrade.md](upgrade.md) — the same for a major upgrade
- [placement.md](placement.md) — table placement moves
