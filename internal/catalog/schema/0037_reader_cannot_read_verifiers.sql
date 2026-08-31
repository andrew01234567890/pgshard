-- pgshard.roles.verifier is deliberately withheld from pgshard_reader: the
-- column grant on that table names its columns one by one and leaves the
-- verifier out. pgshard.migrations then hands it back. CREATE ROLE and ALTER
-- ROLE are rewritten into SCRAM before they are recorded, so the verifier is
-- in the statement text and in meta, and migration rows are kept.
--
-- The admin UI redacts a verifier before showing it, but that protects the
-- page, not the table: a monitoring login inheriting pgshard_reader queries
-- the catalog directly, and can harvest every current and historical
-- verifier for offline guessing.
--
-- So the reader loses the base table and gets a view without the two fields
-- that carry one. It is not a redaction of the statement text -- a regular
-- expression deciding who can read a password verifier is a poor bargain --
-- and everything monitoring actually reads a migration for is still there.
REVOKE SELECT ON pgshard.migrations FROM pgshard_reader;

CREATE VIEW pgshard.migrations_public AS
SELECT id, database, kind, strategy, scope, home_shard, state,
       per_shard, error, created_at, finished_at
FROM pgshard.migrations;

COMMENT ON VIEW pgshard.migrations_public IS
    'pgshard.migrations without statement or meta, which carry SCRAM verifiers for role DDL.';

GRANT SELECT ON pgshard.migrations_public TO pgshard_reader, pgshard_admin;
