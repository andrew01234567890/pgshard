# Local deployment artifacts

The deployment build produces Linux/amd64 Docker-compatible image archives for
the Rust agent, orchestrator and pooler, the Go operator, and the PostgreSQL 18
bootstrap/agent image. Builder and runtime images are
digest-pinned. The supporting runtimes use numeric user and group `10001`; the
PostgreSQL image uses the image's numeric `999:999` database identity. Every
image has a direct binary entrypoint and is tested with a read-only root
filesystem. PostgreSQL quarantine preflight is a point-in-time validation of
trusted paths; the lifecycle contract therefore requires an immutable image
and exclusive access to its disposable PGDATA volume. Its private socket
directory must be provisioned for UID `999` with mode `0700`, or owner-only
setgid mode `2700`. An `fsGroup` alone normally makes a volume root
group-writable and does not satisfy this contract; the future operator must
provision the exact ownership and mode explicitly. Group and world permission
bits remain forbidden.

Before spawning, the agent recursively verifies every PGDATA directory's owner,
permissions, and mount identity, and rejects symlinks or special files anywhere
in the tree. It also rejects nested mounts, including regular-file mount points,
and uses the immutable PostgreSQL 18 `pg_controldata` sibling to reject recovery
control states even when standby signal files are missing. A pre-existing
external PID path in the private socket directory must be an exact, bounded
regular file. Symlinks are refused, and the exact validated file is preserved
for PostgreSQL to overwrite only after PostgreSQL has established its own data
and socket lock ownership. This includes PostgreSQL-created `0600`/`0640` empty
or decimal-prefix startup residue; a final `0644` file still requires one
canonical PID. If a stale PostgreSQL lock
names one of the agent's own threads, the agent atomically replaces it only
after the parent PID line and PostgreSQL's remaining shared-memory/orphan
evidence are durable. The postmaster starts in a dedicated process group. Exit
is observed without reaping the group leader; the leader PID remains reserved
until no live descendant remains. Blocking preflight work has a bounded wait
and can be abandoned on shutdown before process creation. Shutdown signal
phases are bounded, but final process-tree cleanup deliberately holds the
PGDATA fence without a time bound when the kernel cannot prove termination.

Build the five standard archives from the repository root:

```console
make images
```

Set `PGSHARD_IMAGE_TARGETS` to a space-separated Bake target list when a local
test needs only a subset. For example, the manager KIND smoke builds
`operator orchestrator pooler postgres-agent`. The PostgreSQL image is both the
short-lived catalog/bootstrap init image and the input for separate lifecycle
tests.

The selected Buildx builder must support the Docker exporter. Docker's default
`docker` driver does not support this archive exporter; use a
`docker-container`, Kubernetes, or remote builder. CI creates an isolated
`docker-container` builder.
`make images` derives the full source revision from `HEAD`; its development
version includes the abbreviated revision and a `dirty` marker when tracked or
untracked source changes are present. Source archives must provide
`PGSHARD_GIT_SHA` explicitly. Direct Bake invocations must provide both build
identity variables; the Bake definition rejects missing and all-zero identity.

The files are written under `artifacts/images/`. The `postgres-agent` archive
contains PostgreSQL 18, the Rust agent, and the read-only source catalog
migration so initialization and lifecycle tests can exercise the real
postmaster without publishing an image. The bake definition has no
registry output or push target. CI builds the standard archives together and
independently rebuilds the PostgreSQL image for its lifecycle and manager
tests, loads them into local Docker daemons, checks their platform, build
labels, non-root identity, migration bytes, and entrypoints under a read-only
root filesystem, then discards them with the ephemeral runners. CI does not
upload or publish the archives.

These images contain the current incomplete runtimes. The operator can create
only the documented direct single-member development cluster, and no image is
published by the Milestone 1 release job.
The Kustomizations under `operator/config/development` and
`operator/config/admission` consume local `:dev` images after they are loaded
into KIND. The latter adds self-managed webhook certificates and fail-closed
admission. Both are tested source-validation boundaries, not deployable
databases or production distributions.
