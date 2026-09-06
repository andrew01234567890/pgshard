-- The term of the current controller leader.
--
-- Leadership is a pg_try_advisory_lock held on one connection, and the flag
-- the workers read is only updated when that connection's next I/O fails.
-- A controller that lost the lock part-way through an applier pass therefore
-- kept applying a migration shard by shard while the new leader drove the
-- same row from the start: both wrote the same per-shard progress, and the
-- second attempt at an object that already existed failed the migration with
-- the object half created.
--
-- Every leader bumps this on acquiring the lock -- which it can only do once
-- the previous holder's connection is gone -- and stamps its writes with the
-- term it took. A write from any other term is refused, so a pass whose
-- leadership ended stops at its next write rather than at its next tick,
-- and a restarted controller takes a higher term and carries on at once.

SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.leader_term (
    id      boolean     PRIMARY KEY DEFAULT true CHECK (id),
    term    bigint      NOT NULL DEFAULT 0,
    took_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO pgshard.leader_term (id) VALUES (true);

GRANT SELECT ON pgshard.leader_term TO pgshard_admin, pgshard_reader;

RESET ROLE;
