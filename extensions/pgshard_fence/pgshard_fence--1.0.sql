\echo Use "CREATE EXTENSION pgshard_fence WITH SCHEMA pg_catalog" to load this file. \quit

CREATE FUNCTION pg_catalog.pgshard_fence_install(
    identity bytea,
    deadline_boottime_ns bytea,
    OUT installed_identity bytea,
    OUT installed_deadline_boottime_ns bytea
)
RETURNS record
AS 'MODULE_PATHNAME', 'pgshard_fence_install'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION pg_catalog.pgshard_fence_install(bytea, bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.pgshard_fence_install(bytea, bytea) TO postgres;
