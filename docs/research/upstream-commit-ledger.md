# Upstream research commit ledger

The following commits are the exact upstream snapshots used for the reviewed
design research. Every commit value is the full 40-character object ID, and
each link points at that exact commit in the primary repository. These are
research pins, not runtime dependencies or claims of code compatibility.
Upstream projects remain independently licensed. In particular, StackGres was
inspected for research only; its AGPL-licensed code was not copied.

| Upstream | Commit | Research role |
| --- | --- | --- |
| [PostgreSQL](https://github.com/postgres/postgres/commit/247ef8481d89c04554702a0d18ec5455fb19f321) | `247ef8481d89c04554702a0d18ec5455fb19f321` | PostgreSQL 18 database, SQL, WAL, and transaction behavior |
| [Vitess](https://github.com/vitessio/vitess/commit/7563910899de3952b74ecc2bbdc93838690d437c) | `7563910899de3952b74ecc2bbdc93838690d437c` | Sharding, routing, topology, and distributed-transaction reference |
| [Multigres](https://github.com/multigres/multigres/commit/abb8d4a8bb71bce1eb264afb57b2883ea8e92508) | `abb8d4a8bb71bce1eb264afb57b2883ea8e92508` | PostgreSQL-wire sharding, control-plane, and recovery reference |
| [multigres-operator](https://github.com/multigres/multigres-operator/commit/b42b82f836ebbd82c2b02b94413ffd12ebdd7943) | `b42b82f836ebbd82c2b02b94413ffd12ebdd7943` | Kubernetes reconciliation and PostgreSQL member lifecycle reference |
| [CloudNativePG (CNPG)](https://github.com/cloudnative-pg/cloudnative-pg/commit/011ba50586cf3e264dc75ef9d0d5814a195159e9) | `011ba50586cf3e264dc75ef9d0d5814a195159e9` | PostgreSQL operator and backup/recovery reference |
| [StackGres](https://github.com/ongres/stackgres/commit/a776ea257a5f9d6f7223eb7ab554f3af80d6d07d) | `a776ea257a5f9d6f7223eb7ab554f3af80d6d07d` | Research only; AGPL-licensed; no code copying |
| [Crunchy PGO](https://github.com/CrunchyData/postgres-operator/commit/df7bc68eddb3be6f4fc01ac1341f2cce55848136) | `df7bc68eddb3be6f4fc01ac1341f2cce55848136` | PostgreSQL operator and lifecycle reference |
| [Patroni](https://github.com/patroni/patroni/commit/da835c6d71d570f87a6821b4d797e91721d41a03) | `da835c6d71d570f87a6821b4d797e91721d41a03` | HA, leader election, and fencing reference |
| [Percona](https://github.com/percona/percona-postgresql-operator/commit/eb2586e97b1869c8a25500939373027a12b7d8d5) | `eb2586e97b1869c8a25500939373027a12b7d8d5` | PostgreSQL operator and HA operations reference |
| [Zalando Postgres Operator](https://github.com/zalando/postgres-operator/commit/6fbf962b1c4207592aa7dae9d88e9382a66bd345) | `6fbf962b1c4207592aa7dae9d88e9382a66bd345` | PostgreSQL operator and lifecycle reference |

Any future source comparison must record its own full commit object ID rather
than silently treating a moving upstream branch or abbreviated prefix as
equivalent.
