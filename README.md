# pgshard

pgshard is a sharded PostgreSQL system written in Go, in the spirit of Vitess (for MySQL) and
Multigres (for PostgreSQL). It aims to provide:

- a Kubernetes operator that provisions multi-shard, multi-replica PostgreSQL with high
  availability, automatic tuning, zero-downtime rolling operations, and backups to object storage
  (full, incremental, PITR and restore);
- a `pgshard` control-plane database whose tables define the shard layout with ordinary SQL, and
  whose edits drive automatic resharding and table re-keying;
- a stateless query router (PostgreSQL wire protocol) that plans and routes SQL across shards,
  runs single-shard transactions single-phase and multi-shard transactions with two-phase commit,
  and fans DDL/DCL out to every shard;
- one merged change stream over all shards (VStream-style) built on logical decoding;
- zero-downtime resharding, online DDL and PostgreSQL major upgrades;
- an admin web UI showing topology and the progress of long-running operations.

## Documentation

- [User guide](docs/guide/getting-started.md) — install, define sharding, query, back up, restore.
- [Runbooks](docs/README.md#runbooks) — failover, rollouts, backup failures, in-doubt
  transactions, slot invalidation, disk pressure, barrier restores.
- [Capability matrix](docs/capability-matrix.md) — implemented / partial / planned per feature,
  with pointers to code and tests.
- [Documentation index](docs/README.md) — everything, including the component references.

## Status

**Early development — not yet suitable for production data.** The
[capability matrix](docs/capability-matrix.md) is the per-feature status of record; in short, what
exists today:

- PostgreSQL 18 and 19 images (`postgres/`) built from source with pgBackRest, `pgshard-agent` as
  PID 1 and `pgshard-pooler`, plus the router/control images.
- The `pgshard` catalog schema (`internal/catalog`): shard sets and ranges, databases, roles,
  transaction decision log, workflows and migrations, with integration tests against both majors.
- The Kubernetes operator (`docs/operator.md`, `docs/crd.md`): shard groups with replicas,
  failover, rolling restarts, automatic tuning (`docs/tuning.md`), pooler sidecars, router
  deployment, backups (`docs/backup.md`).
- The agent, pooler and controller (`docs/ha.md`, `docs/pooler.md`): member lifecycle, health,
  fencing, and the desired-state reconciler and resolver.
- The query router (`docs/router.md`): PostgreSQL wire protocol, PostgreSQL 18 parser, planner with
  explicit refusals for unsupported statements, scatter queries, single-shard transactions and
  two-phase commit, reference tables and sequences; DDL (`docs/ddl.md`).
- Change streams over logical decoding (`docs/streams.md`) and the admin UI (`docs/admin.md`).
- e2e (kind), chaos and perf harnesses under `test/`.

- Backups to object stores with PITR restore, certified cross-shard restore points and fenced
  barrier restores with two-phase reconciliation (`docs/backup.md`).

Not there yet: the resharding and re-keying workflow executors (edits are detected and recorded as
pending workflows, `docs/guide/resharding.md`), online major upgrades (`docs/guide/upgrades.md`),
and most chaos experiments (`test/chaos/README.md` lists the planned catalogue). Design documents
describe intended behaviour; where a document and the code disagree, the code and its tests are
authoritative — `docs/capability-matrix.md` is the summary.

Target PostgreSQL versions: 18 and 19.

## Building and testing

```sh
make tools    # install the pinned linters and code generators
make verify   # the fast gate: gofmt, go vet, golangci-lint, proto lint, go test -race, build
make gates    # every check CI gates on, except the secret scan
make build    # build only
```

Go 1.26 or newer is required, along with a C compiler: the SQL parser is a cgo
binding to libpg_query. `make verify` is deliberately the fast tier and says so
when it finishes; the PostgreSQL-backed tests need Docker and the Kubernetes
tests need envtest assets, and both **skip** when those are missing.
[docs/guide/testing.md](docs/guide/testing.md) says which tier a change needs
and how to make the skips fail instead. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution rules
(public repository: synthetic data only, no secrets) and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
