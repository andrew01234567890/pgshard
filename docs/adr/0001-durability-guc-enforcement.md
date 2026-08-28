# 1. Enforcing the durability floor against code the router cannot read

Status: accepted

## Context

A pgshard cluster promises that an acknowledged commit survives failover.
That promise rests on `synchronous_commit = on`, which `pgtune` fixes in
`postgresql.conf` and refuses to let an operator lower, and on the router
refusing every statement that would change it for a session.

The router's refusals are made from the parse tree. `synchronous_commit`
is a `USERSET` GUC, so no server-side `GRANT`/`REVOKE` restricts it, and a
parse tree does not include the body of a stored function: a trigger that
calls `set_config('synchronous_commit', 'off', true)` weakens the
transaction that fires it, and no AST check can see that.

## What the router already refuses

Reading the planner rather than assuming, every route to a protected
setting that goes *through* pgshard is already closed:

- `SET`/`SET LOCAL` of a protected setting — `VariableSetStmt`.
- `set_config(...)` in any expression, at any depth, including a
  non-constant setting name, which fails closed.
- `UPDATE pg_settings`, which the rule system rewrites into `set_config`.
- `ALTER ROLE ... SET`, `ALTER ROLE ... IN DATABASE ... SET`, and
  `ALTER ROLE ALL SET`.
- `ALTER DATABASE ... SET` as a whole.
- `CREATE FUNCTION`, `CREATE PROCEDURE` and `DO` — refused outright, so a
  body the router cannot parse cannot be installed through the router at
  all.

The last one is what changes the shape of this problem. The finding
assumed a function body could be installed and then fired; it cannot,
through pgshard. The residual is a connection made **directly to a shard**,
which is a separate finding with its own fix, and which will remain
possible for anyone holding the superuser password even after it lands.

## Decision

1. Do **not** patch PostgreSQL to make `synchronous_commit` `SUSET`. It is
   the strongest option and remains available, but the project patches core
   only where core cannot do the job, and here the gap is not core's: it is
   that something can bypass the router entirely. A patch would also not
   help against a superuser, who can change a `SUSET` GUC.

2. Do **not** screen function and trigger bodies at DDL time. There is no
   DDL time to screen: the statements that would carry a body are refused.
   Adding a textual scan would be code that never runs, and would read as a
   defence that is not there.

3. **Audit and report.** `controller.DurabilityCheck` asks every shard of
   the serving set, on a ticker, what the protected settings read as and
   whether any `pg_db_role_setting` entry will impose a different value on
   a future session. Drift is logged at error level naming the shard and
   the setting. This catches exactly the case that is left — a change made
   outside pgshard — and catches it whether it arrived through a role
   default, a database default, or an edited configuration file.

## Consequences

- Drift is found by a pass on a timer, not prevented at the moment it
  happens. Between passes a shard can acknowledge commits that a failover
  would lose. The audit interval is therefore a durability parameter, not
  a housekeeping one.
- The audit cannot see a `SET LOCAL` inside a transaction on another
  session. Nothing outside the server can. Closing that needs the core
  patch in (1), and the case requires code installed out of band by someone
  who already holds credentials to the shard.
- The check compares against a fixed table of expected values rather than
  against `pgtune`'s derivation, so a future setting whose safe value
  depends on the profile needs adding in both places.
