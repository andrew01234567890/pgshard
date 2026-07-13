# Contributing

pgshard is a public, pre-alpha distributed-systems project. Correctness claims,
tests, and documentation are part of every change.

## Workflow

1. Branch from current `main`; never commit directly to the default branch.
2. Use a Conventional Commit title for the pull request.
3. Add regression tests and update affected documentation.
4. Open a draft pull request and wait for all required GitHub checks.
5. Complete an independent adversarial review covering simplification,
   correctness, ACID, Jepsen/Elle histories, crash recovery and durability.
6. Squash-merge only after every required check passes.

All source-branch commits must use GitHub noreply author and committer addresses.
GitHub's final squash commit may retain the pull-request author's public address
only under the signed `web-flow` exception documented in the release policy; it
does not hide that metadata. Do not add secrets, credentials, private hostnames,
personal paths, internal-only information, production data, or row values
captured from a change stream.

## Supported development environment

Milestone 1 supports the repository's Linux container toolchain only. Native
macOS and Windows development and runtime support are out of scope.

## Local checks

```bash
make check
```

This is the same Rust, RustSec, license, protobuf, Go operator, documentation,
actionlint and public-history policy used by CI. It requires Rust 1.97,
cargo-deny 0.20.2, cargo-audit 0.22.2, Buf 1.71, Go 1.26, and Node.js 22. The Go
checks include module consistency, race tests, vet, build, vulnerability
analysis, and reproducible controller-generated manifests. UI, integration,
KIND, performance, and Jepsen/Elle targets join this command when their
workspaces land.
