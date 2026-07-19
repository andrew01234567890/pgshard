FROM docker.io/library/rust:1.97.0-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS build-base

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates

FROM build-base AS build

RUN PGSHARD_BUILD_VERSION="${PGSHARD_BUILD_VERSION}" \
    PGSHARD_GIT_SHA="${PGSHARD_GIT_SHA}" \
    cargo build --locked --release \
      --package pgshard-agent \
      --package pgshard-orch \
      --package pgshard-pooler && \
    install -D -m 0755 target/release/pgshard-agent /out/pgshard-agent && \
    install -D -m 0755 target/release/pgshard-orch /out/pgshard-orch && \
    install -D -m 0755 target/release/pgshard-pooler /out/pgshard-pooler

FROM build-base AS postgres-agent-build

RUN PGSHARD_BUILD_VERSION="${PGSHARD_BUILD_VERSION}" \
    PGSHARD_GIT_SHA="${PGSHARD_GIT_SHA}" \
    cargo build --locked --release --package pgshard-agent && \
    install -D -m 0755 target/release/pgshard-agent /out/pgshard-agent && \
    install -D -m 0755 target/release/pgshard-catalog-material-digest /out/pgshard-catalog-material-digest && \
    install -D -m 0755 target/release/pgshard-scram-verifier /out/pgshard-scram-verifier

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa AS runtime

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

LABEL org.opencontainers.image.source="https://github.com/andrew01234567890/pgshard" \
      org.opencontainers.image.version="${PGSHARD_BUILD_VERSION}" \
      org.opencontainers.image.revision="${PGSHARD_GIT_SHA}"

USER 10001:10001
STOPSIGNAL SIGTERM

FROM runtime AS agent
COPY --from=build /out/pgshard-agent /usr/local/bin/pgshard-agent
ENTRYPOINT ["/usr/local/bin/pgshard-agent"]

FROM runtime AS orchestrator
COPY --from=build /out/pgshard-orch /usr/local/bin/pgshard-orch
ENTRYPOINT ["/usr/local/bin/pgshard-orch"]

FROM runtime AS pooler
COPY --from=build /out/pgshard-pooler /usr/local/bin/pgshard-pooler
ENTRYPOINT ["/usr/local/bin/pgshard-pooler"]

FROM docker.io/library/postgres:18@sha256:32ca0af8e77bfb8c6610c488e4691f83f972a3e9e64d3b02facf3ab111ad5500 AS postgres-agent

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

LABEL org.opencontainers.image.source="https://github.com/andrew01234567890/pgshard" \
      org.opencontainers.image.version="${PGSHARD_BUILD_VERSION}" \
      org.opencontainers.image.revision="${PGSHARD_GIT_SHA}"

COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-agent /usr/local/bin/pgshard-agent
COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-catalog-material-digest /usr/local/bin/pgshard-catalog-material-digest
COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-scram-verifier /usr/local/bin/pgshard-scram-verifier
RUN install -d -o 0 -g 0 -m 0755 /etc/pgshard /usr/share/pgshard/migrations
COPY --chown=0:0 --chmod=0444 deploy/images/quarantine.pg_hba.conf /etc/pgshard/quarantine.pg_hba.conf
COPY --chown=0:0 --chmod=0444 crates/pgshard-catalog/migrations/0001_shardschema.sql /usr/share/pgshard/migrations/0001_shardschema.sql

USER 999:999
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pgshard-agent"]
