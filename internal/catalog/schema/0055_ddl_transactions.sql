-- How DDL inside a client transaction runs in a database that fans it out.
--
-- A fanned-out statement is applied by the controller on every shard, each
-- in that shard's own transaction, so it cannot be rolled back with the
-- client's transaction; inside BEGIN/COMMIT, or inside the implicit
-- transaction of a multi-statement query, it is refused ('atomic').
--
-- Migration tools send DDL exactly that way even when nothing else is in the
-- transaction: pgroll recreates a version view as one query ("BEGIN; DROP
-- VIEW ...; CREATE VIEW ...; COMMIT") and installs its trigger function and
-- trigger in one transaction. 'sequential' lets a transaction that holds
-- nothing but DDL run it statement by statement, each applied and awaited as
-- if the client had sent it on its own. A transaction that also runs a
-- statement on a shard is still refused, either way round.
--
-- It is a declaration, like local_only, because it changes what COMMIT and
-- ROLLBACK promise: a ROLLBACK after a sequential DDL statement does not
-- undo it.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.databases
    ADD COLUMN ddl_transactions text NOT NULL DEFAULT 'atomic'
        CONSTRAINT databases_ddl_transactions_check CHECK (ddl_transactions IN ('atomic', 'sequential'));

COMMENT ON COLUMN pgshard.databases.ddl_transactions IS
    'atomic: DDL inside a transaction is refused. sequential: a transaction of DDL alone runs it statement by statement, not atomically.';

RESET ROLE;
