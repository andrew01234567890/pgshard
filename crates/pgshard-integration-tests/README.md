# Integration test root

This package is the CI entry point for tests that cross a pgshard component
boundary and require a real external service. It does not provide mock or
placeholder suites.

The currently executable suite is `postgres18-wire`. It connects to a real
PostgreSQL 18 server and runs the existing raw-protocol and logical-replication
contract test from `pgshard-pgwire`. The caller must provide
`PGSHARD_PGWIRE_TEST_ADDRESS`; the runner fails instead of skipping when the
fixture is unavailable.

Catalog migration tests continue to run in the dedicated
`Catalog / PostgreSQL 18` job. Cross-shard transactions and failover integration
suites are not implemented and are deliberately not registered here.

```console
PGSHARD_PGWIRE_TEST_ADDRESS=127.0.0.1:5432 \
  cargo run --locked -p pgshard-integration-tests -- --suite postgres18-wire
```
