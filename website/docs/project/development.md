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

Reviews cover ordinary code quality plus ACID claims, crash recovery, split brain, durability, Jepsen/Elle histories, backup consistency, CDC gaps/replays, security, silent failures, and documentation honesty.

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

## Git identity and history

Source-branch commits use GitHub noreply author and committer identities. Direct
default-branch pushes, merge commits, and rebase merges are disabled. After
squash merge, automation verifies either those noreply identities or the narrow,
signed GitHub `web-flow` exception in the [release policy](./releases.md). That
exception may retain public author metadata and does not hide it.
