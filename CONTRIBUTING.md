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

All commits must use a GitHub noreply address. Do not add secrets, credentials,
private hostnames, personal paths, internal-only information, production data,
or row values captured from a change stream.

## Supported development environment

Milestone 1 supports the repository's Linux container toolchain only. Native
macOS and Windows development and runtime support are out of scope.

## Local checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo run --locked -p pgshard-release -- audit --base origin/main --head HEAD
```

Additional Go, UI, documentation, integration, KIND, performance, and Jepsen
checks become required as those workspaces are introduced.
