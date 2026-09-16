# Running pgroll against pgshard

[pgroll](https://github.com/xataio/pgroll) is the expand/contract migration
tool pgshard's own online-DDL design borrows from. It works against a
pgshard cluster: `pgroll init`, `start`, `complete` and `rollback` run their
normal workflow, and the schema change reaches every shard.

This page is what an operator has to do differently, and why. Everything
here is exercised by `TestPgrollAgainstAShardedDatabase`, against a table
sharded over two shards alongside a reference table.

## Before the first migration

Three things are not obvious, and each fails with an error that does not
name pgroll.

### 1. Pin pgroll's schema to the home shard

pgroll keeps its state in a schema of its own — a migrations table,
functions, and event triggers — and creates it all in one transaction. None
of that is a question about distribution, so the schema is declared local
and runs on the database's home shard, on the client's own connection:

```sql
UPDATE pgshard.databases
   SET local_schemas = '{pgroll}', ddl_transactions = 'sequential'
 WHERE name = 'app';
```

`ddl_transactions = 'sequential'` is the other half: pgroll sends DDL inside
transactions that hold nothing else — its version views go as one
`BEGIN; DROP VIEW …; CREATE VIEW …; COMMIT` — and a `sequential` database
runs each such statement as if it had been sent on its own, with a NOTICE
saying so. See [`sharding.md`](sharding.md) for both columns.

### 2. Superuser for `init`, a plain role for everything after

PostgreSQL allows event triggers only to a superuser. That is PostgreSQL's
rule, not pgshard's, and pgroll installs event triggers.

pgshard refuses a **superuser's** fanned-out DDL:

```
ERROR:  role "app" is a superuser: DDL through the router runs as a plain role only
```

That refusal is deliberate. The controller applies a client's statement by
logging in as `pgshard_ddl` and `SET ROLE`-ing into the client's role, so a
statement's privileges are the client's; a superuser there would be an
escalation with nothing above it to stop.

So the two are compatible only **in sequence**. Grant the role superuser
while pgroll installs its state, and take it away before any migration:

```sql
-- on every shard's primary, as the superuser
ALTER ROLE app SUPERUSER;
```
```console
$ pgroll init --postgres-url "$PGSHARD_URL"
```
```sql
-- on every shard's primary, before the first pgroll start
ALTER ROLE app NOSUPERUSER;
```

Miss it one way and `init` fails on the event trigger. Miss it the other and
every migration is refused with a message about superusers that never
mentions pgroll.

### 3. pgroll must own the tables it migrates

`ALTER TABLE` asks for ownership, not privileges, so a table created by the
superuser and granted to an application role is not one pgroll can migrate
as that role. The error is `must be owner of table orders`, which does not
say that. Hand ownership over on every shard:

```sql
ALTER TABLE orders OWNER TO app;
```

## Starting from a schema that already has tables

pgroll refuses to start a migration over a schema with no history of its
own, and asks for a baseline:

```console
$ pgroll baseline 00_existing ./migrations --yes --postgres-url "$PGSHARD_URL"
```

**pgroll prints that refusal and exits 0.** Anything wrapping pgroll has to
read its output, not its exit code.

## What is supported

`add_column` (without `up`/`default`) and `create_index` are **proved**
against a sharded table by the acceptance test: the column and the index
reach every shard, and reads through the version schema answer for all of
them.

`drop_index`, `rename_column`, `rename_constraint`, `create_constraint` and
`drop_column`/`drop_constraint` are the rest of what the design expects to
work — they are metadata changes of the same shape — but they are not yet
exercised end to end, and PGS-929 tracks that. Treat them as untested rather
than as guaranteed.

Anything that needs pgroll's **dual-write triggers and batched backfill** —
an `alter_column` with `up` and `down` — is not supported yet on a
non-local database: pgroll's backfill is a single scalar cursor, which
cannot be split across shards. A **local** database (`local_only = true`)
has one shard and runs all of it, including the backfill; that is what
[`TestPgrollAgainstALocalDatabase`](../../test/e2e/router/pgroll_test.go)
covers.

Two migrations are refused at `start` rather than at `complete`, on purpose:

- a pgroll migration of a **sharded table's shard key** — completing it
  drops and renames the shard key, so change it with a re-key instead
  (`UPDATE pgshard.tables`);
- a pgroll migration of a **reference table** — its dual-write triggers
  would write each shard's copy separately.

The refusal is at `start` because `pgroll rollback` can undo a start, and
nothing can undo a half-completed rename.

## When a start is refused

A refused start leaves pgroll holding a migration for the schema, so the
next one is refused with `a migration for schema "public" is already in
progress` until the failed one is rolled back:

```console
$ pgroll rollback --postgres-url "$PGSHARD_URL"
```

That is pgroll's own protocol and it works through pgshard; the refusal does
not wedge the tool.

## Known limits

- Introspection reads one shard, so pgroll sees that shard's view of the
  schema (PGS-590).
- A table placement move, and pgshard's own type-change rewrite, are refused
  while pgroll version views exist over the table.
- Change-stream consumers see pgroll's `_pgroll_*` columns while a migration
  is between `start` and `complete`.
