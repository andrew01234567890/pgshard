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

## Status

**Early development — nothing here serves traffic yet.** The repository currently contains the Go
module, the command skeletons (`pgshard-router`, `pgshard-agent`, `pgshard-pooler`,
`pgshard-controller`, `pgshard-operator`, `pgshard-admin`) and repository policy. Each command only
supports `--help` and `--version`; running one without arguments reports that runtime services are
not implemented yet. Design documents describe intended behaviour, not implemented guarantees.

Target PostgreSQL versions: 18 and 19.

## Building and testing

```sh
make verify   # gofmt check, go vet, golangci-lint, go test -race, build all commands into ./bin
make build    # build only
```

Go 1.26 or newer is required. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution rules
(public repository: synthetic data only, no secrets) and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
