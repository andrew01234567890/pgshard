---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no installable pgshard cluster in the foundation release. The operator
chart, custom resources, container build and KIND environment are planned and
will appear here only after their end-to-end tests pass.

## Validate the current source

The supported development environment is Linux with Rust 1.97, Node.js 22 and
Buf 1.71. From a checkout:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
buf format --diff --exit-code
buf lint
buf build
cd website
npm ci
npm run check
npm audit --audit-level=high
```

These commands validate contracts, core types, release policy and documentation;
they do not start PostgreSQL or prove a sharding runtime. Follow [implementation
status](./project/status.md) for the first version with a real cluster quickstart.
