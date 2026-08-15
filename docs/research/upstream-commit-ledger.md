# Upstream research commit ledger

The following commits are the exact upstream snapshots used for the reviewed
design research. They are research pins, not runtime dependencies or claims of
code compatibility. Upstream projects remain independently licensed. In
particular, StackGres was inspected for research only; its AGPL-licensed code
was not copied.

| Upstream | Commit | Research role |
| --- | --- | --- |
| [PostgreSQL](https://github.com/postgres/postgres/commit/247ef8481d89) | `247ef8481d89` | PostgreSQL 18 database, SQL, WAL, and transaction behavior |
| [Vitess](https://github.com/vitessio/vitess/commit/7563910899de) | `7563910899de` | Sharding, routing, topology, and distributed-transaction reference |
| [Multigres](https://github.com/multigres/multigres/commit/abb8d4a8bb71) | `abb8d4a8bb71` | PostgreSQL-wire sharding, control-plane, and recovery reference |
| [multigres-operator](https://github.com/multigres/multigres-operator/commit/b42b82f836eb) | `b42b82f836eb` | Kubernetes reconciliation and PostgreSQL member lifecycle reference |
| [CloudNativePG (CNPG)](https://github.com/cloudnative-pg/cloudnative-pg/commit/011ba50586cf) | `011ba50586cf` | PostgreSQL operator and backup/recovery reference |
| [StackGres](https://github.com/StackGres/stackgres/commit/a776ea257a5f) | `a776ea257a5f` | Research only; AGPL-licensed; no code copying |
| [Crunchy PGO](https://github.com/CrunchyData/postgres-operator/commit/df7bc68eddb3) | `df7bc68eddb3` | PostgreSQL operator and lifecycle reference |
| [Patroni](https://github.com/patroni/patroni/commit/da835c6d71d5) | `da835c6d71d5` | HA, leader election, and fencing reference |
| [Percona](https://github.com/percona/percona-postgresql-operator/commit/eb2586e97b18) | `eb2586e97b18` | PostgreSQL operator and HA operations reference |
| [Zalando Postgres Operator](https://github.com/zalando/postgres-operator/commit/6fbf962b1c42) | `6fbf962b1c42` | PostgreSQL operator and lifecycle reference |

The ledger deliberately records the short commit identifiers supplied for this
research set. Any future source comparison must record its own commit rather
than silently treating a moving upstream branch as equivalent.
