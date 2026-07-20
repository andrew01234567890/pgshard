FROM docker.io/library/rust:1.97.0-bookworm@sha256:8fa55b2f3ddf97471ab6a767bfa3f37e6bad0986ba823e75fea57e2a2a5c3073 AS build-base

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates
COPY contracts ./contracts

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

FROM postgres-agent-build AS postgres-fence-test-build

RUN find target/release/deps -maxdepth 1 -type f -name 'pgshard_agent-*' -perm -0100 -delete && \
    cargo test --locked --release --package pgshard-agent --lib --no-run && \
    test_binary="$(find target/release/deps -maxdepth 1 -type f -name 'pgshard_agent-*' -perm -0100 -print)" && \
    test -n "${test_binary}" && \
    test "$(printf '%s\n' "${test_binary}" | wc -l)" -eq 1 && \
    install -D -m 0755 "${test_binary}" /out/pgshard-agent-tests

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

FROM docker.io/library/postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a AS postgres-fence-build

RUN rm -f /etc/apt/sources.list.d/pgdg.list && \
    sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources && \
    printf 'Acquire::Check-Valid-Until "false";\n' >/etc/apt/apt.conf.d/99snapshot && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      bison build-essential ca-certificates curl flex libcurl4-openssl-dev \
      libicu-dev libkrb5-dev libldap2-dev liblz4-dev libpam0g-dev \
      libreadline-dev libssl-dev liburing-dev libxml2-dev libzstd-dev \
      pkg-config uuid-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/* && \
    test "$(dpkg-query --show --showformat='${Version}' postgresql-18)" = \
      '18.4-1.pgdg13+1'
WORKDIR /src/postgresql
RUN curl --fail --location --silent --show-error \
      "https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.gz" \
      --output /tmp/postgresql.tar.gz && \
    printf '%s  %s\n' \
      '450aa8f2da06c46f8221916e82ae06b04fb1040f8f00643dbf8b7d663caac0b9' \
      /tmp/postgresql.tar.gz | sha256sum --check --strict && \
    tar --extract --gzip --file /tmp/postgresql.tar.gz --strip-components=1 && \
    grep --fixed-strings --line-regexp \
      'AC_INIT([PostgreSQL], [18.4], [pgsql-bugs@lists.postgresql.org], [], [https://www.postgresql.org/])' \
      configure.ac && \
    CFLAGS='-O2 -fstack-protector-strong -fstack-clash-protection -Wformat -Werror=format-security -fcf-protection -fno-omit-frame-pointer' \
    CPPFLAGS='-D_FORTIFY_SOURCE=2' \
    LDFLAGS='-Wl,-z,relro -Wl,-z,now' \
    ./configure \
      --prefix=/usr \
      --bindir=/usr/lib/postgresql/18/bin \
      --datadir=/usr/share/postgresql/18 \
      --includedir=/usr/include/postgresql \
      --libdir=/usr/lib/postgresql/18/lib \
      --disable-rpath --with-pgport=5432 \
      --with-extra-version=' (Debian 18.4-1.pgdg13+1)' \
      --with-gssapi --with-icu --with-ldap --with-libcurl --with-liburing \
      --with-lz4 --with-openssl --with-pam \
      --with-system-tzdata=/usr/share/zoneinfo --with-uuid=e2fs --with-zstd && \
    make --jobs="$(nproc)" && \
    make install && \
    /usr/lib/postgresql/18/bin/postgres --version | \
      grep --fixed-strings '18.4 (Debian 18.4-1.pgdg13+1)'
COPY extensions/pgshard_fence /src/pgshard_fence
RUN make -C /src/pgshard_fence PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config && \
    make -C /src/pgshard_fence install DESTDIR=/out PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config

FROM docker.io/library/postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a AS postgres-agent

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

LABEL org.opencontainers.image.source="https://github.com/andrew01234567890/pgshard" \
      org.opencontainers.image.version="${PGSHARD_BUILD_VERSION}" \
      org.opencontainers.image.revision="${PGSHARD_GIT_SHA}"

COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-agent /usr/local/bin/pgshard-agent
COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-catalog-material-digest /usr/local/bin/pgshard-catalog-material-digest
COPY --from=postgres-agent-build --chown=0:0 /out/pgshard-scram-verifier /usr/local/bin/pgshard-scram-verifier
COPY --from=postgres-fence-build --chown=0:0 /out/usr/lib/postgresql/18/lib/pgshard_fence.so /usr/lib/postgresql/18/lib/pgshard_fence.so
COPY --from=postgres-fence-build --chown=0:0 /out/usr/share/postgresql/18/extension/pgshard_fence.control /usr/share/postgresql/18/extension/pgshard_fence.control
COPY --from=postgres-fence-build --chown=0:0 /out/usr/share/postgresql/18/extension/pgshard_fence--1.0.sql /usr/share/postgresql/18/extension/pgshard_fence--1.0.sql
RUN install -d -o 0 -g 0 -m 0755 /etc/pgshard /usr/share/pgshard/migrations
COPY --chown=0:0 --chmod=0444 deploy/images/quarantine.pg_hba.conf /etc/pgshard/quarantine.pg_hba.conf
COPY --chown=0:0 --chmod=0444 deploy/images/replication-bootstrap-primary.pg_hba.conf /etc/pgshard/replication-bootstrap-primary.pg_hba.conf
COPY --chown=0:0 --chmod=0444 crates/pgshard-catalog/migrations/0001_shardschema.sql /usr/share/pgshard/migrations/0001_shardschema.sql

USER 999:999
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pgshard-agent"]

FROM postgres-agent AS postgres-fence-test-runner

COPY --from=postgres-fence-test-build --chown=0:0 /out/pgshard-agent-tests /usr/local/bin/pgshard-agent-tests
ENTRYPOINT ["/usr/local/bin/pgshard-agent-tests"]
