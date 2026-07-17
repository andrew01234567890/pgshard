# pgshard

`pgshard` is an in-development PostgreSQL 18 sharding platform with a Rust data
plane and a Go Kubernetes operator.

Milestone 1 is building the first end-to-end system: multi-shard routing,
high availability, coordinated backup and restore, online DDL, online
resharding, distributed transactions, and `pgoutput`-based change streams.
Nothing in the current repository should be treated as production-ready.

## Project status

- Status: pre-alpha
- License: Apache-2.0
- Runtime target: Linux containers on Kubernetes
- PostgreSQL target: PostgreSQL 18
- Scope and release gates: [Milestone 1 implementation plan](plan/milestone-1.md)
- Compatibility and guarantees are documented in the
  [project documentation](https://andrew01234567890.github.io/pgshard/).

## Development

The repository is a multi-language workspace. The Rust foundation can be
checked with:

```bash
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the branch, testing, documentation,
privacy, and review requirements.

## Security

Do not report vulnerabilities through a public issue. Follow
[SECURITY.md](SECURITY.md).
