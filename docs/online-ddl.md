# Online rewrite-class DDL

`ALTER COLUMN … TYPE` and `ADD COLUMN … DEFAULT <volatile expression>`
would normally rewrite the whole table under an `ACCESS EXCLUSIVE` lock.
pgshard runs them online instead, per shard, as an **OID-preserving
in-place column duplication** (the pgroll model): the table's
`pg_class.oid` never changes, so logical replication slots, publications
and the VStream keep working, and clients keep reading and writing the
table throughout.

## Mechanism

The router classifies the statement as strategy `rewrite` and records a
`meta.rewrite` plan (`internal/catalog/migrations.go: RewriteChange`); the
controller's applier (`internal/controller/rewrite.go`) drives it
**phase-major across shards** — no shard cuts over before every shard has
finished its backfill:

1. **Publish the column list.** The applier reads the table's visible
   columns from a shard and stores them in the migration's meta. A trigger
   on `pgshard.migrations` notifies every router, which reloads its
   snapshot and starts hiding the migration's working column *before it
   exists*; the applier waits a settle interval (default 2s) for the
   reload.
2. **Add the working column** `_pgshard_<col>_<mig8>` of the new type —
   nullable, no default, so the `ADD COLUMN` is metadata-only and never
   rewrites.
3. **Install the dual-write trigger** `_pgshard_rw_<mig8>` (`BEFORE INSERT
   OR UPDATE`; `BEFORE INSERT` only for the add form). The type form
   assigns `NEW.<working> := (SELECT <using expr> FROM (SELECT (NEW).*) AS
   pgshard_row)`, so the `USING` expression sees the row's columns by
   name; the add form assigns the volatile `DEFAULT`.
4. **Backfill in batches.** With a single-column primary key, batches of
   `batch_size` (default 1000) rows selected by the remaining-rows
   predicate (`working IS DISTINCT FROM (<using>)`, or `working IS NULL`
   for the add form) are updated and committed one at a time; a composite
   or missing primary key falls back to one full-table `UPDATE`
   (documented trade-off). Rows written after the trigger installed are
   already in sync.
5. **Cut over**, per shard, in one transaction under the applier's
   `lock_timeout` retry loop: drop the trigger, drop the old column,
   rename the working column to the original name, re-install the old
   column default (cast to the new type) and, when the old column was
   `NOT NULL`, add a `NOT NULL … NOT VALID` constraint. The add form only
   renames and installs the volatile default as the column default.
6. **Validate** the `NOT NULL` constraint outside the exclusive lock.

The migration row's `per_shard[*].step` records the phase (0 add, 1
trigger, 2 backfill, 3 cutover, 4 validate); every phase is idempotent, so
a crashed applier resumes where it stopped (a cutover whose working column
is already gone is recognised as done).

## Router column hiding

While the migration is queued or running, `internal/router/plan/hide.go`
keeps the table looking unchanged:

* `SELECT *` (and `alias.*`) over the table — and `RETURNING *` on
  `INSERT`/`UPDATE`/`DELETE` — is expanded to the recorded visible
  columns, so the working column never reaches a client. A star that
  would also span other tables is refused (`0A000`): list the columns.
  Whole-row references (`to_jsonb(t)`, `t::text`, `count(t.*)`) cannot be
  expanded in place and are refused (`0A000`) while the rewrite runs.
  Until the column list is published (a brief window) `SELECT *` is
  refused with `55000` and a retry hint.
* An `INSERT` without a column list gets the first *N* visible columns as
  its list (*N* = the VALUES row width); `INSERT … SELECT` without a list
  is refused. (Sharded tables already require an explicit column list.)
* Any statement naming a `_pgshard_…` column is refused with `42703`.
* At cutover the applier bumps the migration row; the notify trigger makes
  routers reload and stop hiding.

Reads see the **old** column name and type until the cutover commits on
their shard; writes reach both columns through the trigger.

## PostgreSQL 19: REPACK (CONCURRENTLY)

A full-table rebuild that is genuinely a repack — `VACUUM (FULL)` of a
sharded or reference table — is classified as strategy `repack`. On a
shard running PostgreSQL 19+ the applier runs `REPACK (CONCURRENTLY)
<table>` (online, brief locks at the start and end); on 18 it falls back
to the client's `VACUUM FULL`, which locks the table (the statement still
fans out shard by shard). Note REPACK changes the relfilenode but keeps
the `pg_class.oid`.

## Failure, revert and GC

* A hard failure in phases 0–2 on any shard **reverts every shard**: drop
  trigger, drop function, drop working column. The old column was never
  touched, so the table is exactly as before.
* A failure during cutover leaves the migration `failed` with the shards
  on each side named in the error (schema DEGRADED until the statement is
  re-run or resolved by hand); already-cut-over shards are not reverted.
* A **sweeper** (`Applier.SweepRewriteArtifacts`) runs whenever the
  migration queue is empty and drops any `_pgshard_…` trigger, function or
  column left on any shard by a crashed process.

## Limitations

* One rewrite action per `ALTER TABLE`; combined statements are refused.
* `ALTER COLUMN … TYPE … COLLATE` and `… CASCADE` are refused.
* `ADD COLUMN` with a volatile default cannot carry other constraints in
  the same action.
* The shard key column can never be retyped (rekey workflow instead).
* Indexes, constraints (other than `NOT NULL` and the column default) and
  identity/generated properties referencing the old column are dropped
  with it at cutover — add them again afterwards; a future iteration will
  re-create them on the working column before cutover.
* A shadow-table swap (new table + copy + swap) is **not** implemented:
  everything in scope here is done in place precisely so `pg_class.oid`
  is preserved. If a swap path ever lands, publications must be refreshed
  explicitly (`FOR ALL TABLES` survives, but the relid change makes the
  VStream re-send its `Relation` message).
