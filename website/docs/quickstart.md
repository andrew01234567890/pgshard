---
title: Quickstart
sidebar_position: 2
description: Validate the current pgshard foundation source.
---

# Quickstart

There is no installable pgshard database cluster yet. The source includes the Go
custom-resource API and safe supporting-resource reconciler plus fail-closed
Rust agent/orchestrator foundations and a local-only pooler catalog control
executable, but no PostgreSQL lifecycle, executable SQL pooler, chart, container
build, or KIND environment. A cluster quickstart will appear only after those
end-to-end tests pass.

## Validate the current source

The supported development environment is Linux with Rust 1.97, Go 1.26,
Node.js 22 and Buf 1.71. From a checkout:

```console
make check
```

This validates contracts, core and runtime foundations, generated Kubernetes
resources, release policy and documentation; it does not start PostgreSQL or
prove a sharding runtime. Follow [implementation status](./project/status.md)
for the first version with a real cluster quickstart.

The catalog migration has a separate opt-in live contract test against a
disposable PostgreSQL 18 `shardschema` database. See the
[`pgshard-catalog` README](https://github.com/andrew01234567890/pgshard/tree/main/crates/pgshard-catalog)
for its preconditions. CI runs that test with an ephemeral service database;
the operator does not bootstrap it yet.
