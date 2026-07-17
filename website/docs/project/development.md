---
title: Development
description: Public-repository workflow, local docs checks, and review gates.
---

# Development

pgshard is developed in public. Never commit credentials, private infrastructure details, local filesystem paths, customer data, or internal logs.

## Change workflow

1. Update `main` and create a dedicated branch.
2. Keep the change focused and update documentation with behavior.
3. Run the relevant unit, integration, docs, and end-to-end checks.
4. Open a draft pull request with a Conventional Commit title.
5. Complete independent correctness and simplification review.
6. Re-review after material fixes.
7. Require the aggregate CI result.
8. Squash-merge and delete the branch.

Reviews cover ordinary code quality plus transactional correctness, crash recovery, split brain, durability, backup consistency, CDC gaps/replays, security, silent failures, and documentation honesty.

## Documentation locally

```bash
cd website
npm ci
npm run start
```

Before submitting:

```bash
npm run check
```

`check` type-checks the site and performs a production build with broken internal links treated as errors. Search indexing happens during the build and requires no hosted search service.

## Local container images

```bash
make images
```

This builds digest-pinned Linux/amd64 Docker-compatible image archives for the
Rust agent, orchestrator and pooler, the Go operator, and the PostgreSQL 18
bootstrap/agent image under `artifacts/images/`. The bake definition has no
registry output or push target. The current images are test inputs, not an
installable sharding product; the operator image can bootstrap only the
documented direct single-member PostgreSQL development slice. The selected
Buildx builder must support the
Docker exporter. Docker's default `docker` driver does not support this archive
exporter; use a `docker-container`, Kubernetes, or remote builder. The command
derives the full revision from `HEAD` and marks its development version `dirty`
when the worktree differs. Source archives must provide `PGSHARD_GIT_SHA`
explicitly. Direct Bake invocations must provide both build identity variables;
missing and all-zero identity is rejected.

`PGSHARD_IMAGE_TARGETS="operator orchestrator pooler postgres-agent" make images` builds the
subset used by the real-manager KIND smoke. After loading those `:dev` images
into KIND, `kubectl apply -k operator/config/admission` installs the restricted
self-managed admission manager. Direct PostgreSQL namespaces must carry the
`pgshard.io/pod-fencing=enabled` label. The admission install makes that opt-in
immutable until namespace deletion and authenticates a cluster handshake before
publishing PostgreSQL Pods. Binding and status each have a mutator plus a final
validator, and the PostgreSQL init container refuses PGDATA access without the
binding-time Node evidence. `operator/config/development` retains a
certificate-free path only for supporting-controller debugging and must not
manage direct PostgreSQL Pods. Neither path provides a routed database
quickstart or production distribution.

## Git identity and history

Source-branch commits use GitHub noreply author and committer identities. Direct
default-branch pushes, merge commits, and rebase merges are disabled. After
squash merge, automation verifies either those noreply identities or the narrow,
signed GitHub `web-flow` exception in the [release policy](./releases.md). That
exception may retain public author metadata and does not hide it.
