-- Coordinator liveness for in-flight two-phase commits: the router
-- heartbeats its preparing decision rows so the resolver only aborts
-- transactions whose coordinator is provably gone, never one that is
-- merely slow between PREPARE and the commit decision.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.xact_decisions
    ADD COLUMN heartbeat_at timestamptz NOT NULL DEFAULT now();

RESET ROLE;
