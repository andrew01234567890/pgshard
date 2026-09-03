-- Which members require mutual TLS on their agent gRPC listener, so a caller
-- that cannot see pods can still dial them correctly.
--
-- The operator learns this from the pod annotation and dials each member
-- accordingly, because a rollout makes the fleet mixed: members restart one
-- at a time, so an agent that has restarted requires TLS while its neighbour
-- still serves plaintext. The controller dials agents too -- schema
-- materialization during a reshard or an upgrade -- and reads the catalog
-- rather than the Kubernetes API, so it has no way to tell them apart.
--
-- Published per shard rather than per member because that is what the
-- controller dials: it reaches the target's PRIMARY, named by
-- primary_endpoint on this same row. Reading the mode from that row is the
-- same read, so the endpoint and the way to reach it cannot disagree.
SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.shard_status
    ADD COLUMN agent_mtls boolean NOT NULL DEFAULT false;

RESET ROLE;
