-- The seed of the salt a router shows for a role that does not exist.
--
-- A SCRAM exchange for an unknown role is run against a throwaway verifier so
-- that a client cannot tell "no such role" from "wrong password". That only
-- works while the salt it shows is stable: a real role's salt comes from its
-- catalog verifier, identical on every router and across restarts, so a mock
-- salt that changes between two probes answers the question the exchange
-- exists to hide. The routers are several replicas behind one Service, and
-- each one drew its own seed at startup.
--
-- PostgreSQL solves this with mock_authentication_nonce, written once by
-- initdb and read by every backend. This is the same value for a pgshard
-- cluster: written once here, read by every router.
--
-- gen_random_uuid() is v4 from the server's strong random source, so two of
-- them hashed together seed this without pgcrypto.
SET LOCAL ROLE pgshard_system;

CREATE TABLE pgshard.auth_nonce (
    only_row boolean PRIMARY KEY DEFAULT true CHECK (only_row),
    nonce    bytea NOT NULL CHECK (octet_length(nonce) = 32)
);

COMMENT ON TABLE pgshard.auth_nonce IS
    'Cluster-wide seed of the mock SCRAM salt shown for roles that do not exist.';

INSERT INTO pgshard.auth_nonce (nonce)
VALUES (sha256(convert_to(gen_random_uuid()::text || gen_random_uuid()::text, 'UTF8')));

-- Only the component that runs the exchange. Knowing the seed lets anyone
-- compute the mock salt for a name and compare it with what the router
-- shows, which is the enumeration this table exists to prevent.
GRANT SELECT ON pgshard.auth_nonce TO pgshard_router;

RESET ROLE;
