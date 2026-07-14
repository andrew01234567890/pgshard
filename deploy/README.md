# Local deployment artifacts

The deployment build produces Linux/amd64 Docker-compatible image archives for
the Rust agent, orchestrator and pooler plus the Go operator. Builder and
minimal runtime base images are digest-pinned. Every runtime uses numeric user
and group `10001`, a read-only-compatible root filesystem, and a direct binary
entrypoint.

Build all four archives from the repository root:

```console
make images
```

The selected Buildx builder must support the Docker exporter. Docker's default
`docker` driver does not support this archive exporter; use a
`docker-container`, Kubernetes, or remote builder. CI creates an isolated
`docker-container` builder.
`make images` derives the full source revision from `HEAD`; its development
version includes the abbreviated revision and a `dirty` marker when tracked or
untracked source changes are present. Source archives must provide
`PGSHARD_GIT_SHA` explicitly. Direct Bake invocations must provide both build
identity variables; the Bake definition rejects missing and all-zero identity.

The files are written under `artifacts/images/`. The bake definition has no
registry output or push target. CI builds the same archives, loads them into its
local Docker daemon, checks their platform, build labels, non-root identity and
entrypoints under a read-only root filesystem, then discards them with the
ephemeral runner. CI does not upload or publish the archives.

These images contain the current incomplete runtimes. They do not create a
PostgreSQL cluster, and no image is published by the Milestone 1 release job.
