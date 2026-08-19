# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG PG_MAJOR
ARG PG_VERSION
ARG PG_SHA256
ARG PGBACKREST_VERSION=2.59.1

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS pg-build
ARG PG_MAJOR
ARG PG_VERSION
ARG PG_SHA256
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential bison flex libreadline-dev zlib1g-dev libssl-dev libicu-dev \
      liblz4-dev libzstd-dev libxml2-dev libxslt1-dev uuid-dev libkrb5-dev libpam0g-dev \
      libldap2-dev pkg-config python3 perl gettext curl ca-certificates patch \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src
RUN curl -fsSLo postgresql.tar.bz2 "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" \
    && echo "${PG_SHA256}  postgresql.tar.bz2" | sha256sum -c - \
    && mkdir postgresql && tar -xjf postgresql.tar.bz2 -C postgresql --strip-components=1 \
    && rm postgresql.tar.bz2
COPY postgres/patches/${PG_MAJOR}/ /usr/src/patches/
WORKDIR /usr/src/postgresql
RUN set -eu; \
    if [ -f /usr/src/patches/series ]; then \
      grep -Ev '^\s*(#|$)' /usr/src/patches/series | while read -r p; do \
        echo "applying patch: $p"; patch -p1 --no-backup-if-mismatch < "/usr/src/patches/$p"; \
      done; \
    fi
RUN ./configure --prefix=/usr/local/pgsql --with-openssl --with-icu --with-lz4 --with-zstd \
      --with-libxml --with-libxslt --with-uuid=e2fs --with-gssapi --with-pam --with-ldap \
      --with-llvm=no --enable-debug=no \
    && make -j"$(nproc)" world-bin && make install-world-bin

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS pgbackrest-build
ARG PGBACKREST_VERSION
ARG PGBACKREST_SHA256
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential meson ninja-build libssl-dev libxml2-dev liblz4-dev libzstd-dev libbz2-dev \
      libyaml-dev libssh2-1-dev zlib1g-dev libpq-dev pkg-config python3 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src
RUN curl -fsSLo pgbackrest.tar.gz "https://github.com/pgbackrest/pgbackrest/archive/refs/tags/release/${PGBACKREST_VERSION}.tar.gz" \
    && echo "${PGBACKREST_SHA256}  pgbackrest.tar.gz" | sha256sum -c - \
    && mkdir pgbackrest && tar -xzf pgbackrest.tar.gz -C pgbackrest --strip-components=1 \
    && meson setup build pgbackrest -Dbuildtype=release \
    && ninja -C build \
    && install -m 0755 build/src/pgbackrest /usr/local/bin/pgbackrest

FROM golang:1.26-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36 AS agent-build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/pgshard-agent ./cmd/pgshard-agent \
    && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/pgshard-pooler ./cmd/pgshard-pooler

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241
ARG PG_MAJOR
ARG PG_VERSION
ARG PGBACKREST_VERSION
LABEL org.opencontainers.image.source="https://github.com/andrew01234567890/pgshard" \
      org.opencontainers.image.title="pgshard-postgres" \
      org.opencontainers.image.version="${PG_VERSION}" \
      org.opencontainers.image.description="PostgreSQL ${PG_MAJOR} built from source with pgBackRest ${PGBACKREST_VERSION} , pgshard-agent as PID 1 and pgshard-pooler" \
      io.pgshard.postgres.major="${PG_MAJOR}"
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libreadline8 zlib1g libssl3 libicu72 liblz4-1 libzstd1 libxml2 libxslt1.1 libuuid1 \
      libkrb5-3 libgssapi-krb5-2 libpam0g libldap-2.5-0 libbz2-1.0 libyaml-0-2 libssh2-1 libpq5 \
      ca-certificates locales \
    && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && groupadd -r -g 999 postgres && useradd -r -u 999 -g postgres -d /var/lib/postgresql -s /bin/bash postgres \
    && mkdir -p /var/lib/postgresql/data /etc/pgbackrest /var/log/pgbackrest /var/lib/pgbackrest \
    && chown -R postgres:postgres /var/lib/postgresql /etc/pgbackrest /var/log/pgbackrest /var/lib/pgbackrest \
    && chmod 0700 /var/lib/postgresql/data
COPY --from=pg-build /usr/local/pgsql /usr/local/pgsql
COPY --from=pgbackrest-build /usr/local/bin/pgbackrest /usr/local/bin/pgbackrest
COPY --from=agent-build /out/pgshard-agent /out/pgshard-pooler /usr/local/bin/
ENV LANG=en_US.utf8 \
    PATH=/usr/local/pgsql/bin:$PATH \
    PGDATA=/var/lib/postgresql/data
USER postgres
WORKDIR /var/lib/postgresql
EXPOSE 5432 8080 8082 9090 9091
ENTRYPOINT ["pgshard-agent"]
CMD ["run", "--config", "/etc/pgshard/agent.json"]
