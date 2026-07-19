FROM docker.io/library/golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS build

ARG TARGETARCH
ARG TARGETOS

WORKDIR /workspace/operator
COPY operator/go.mod operator/go.sum ./
RUN go mod download
COPY operator ./

RUN CGO_ENABLED=0 GOARCH="${TARGETARCH}" GOOS="${TARGETOS}" \
    go build -buildvcs=false -trimpath -ldflags="-s -w -buildid=" \
      -o /out/pgshard-operator ./cmd/manager

FROM gcr.io/distroless/static-debian12:nonroot@sha256:aef9602f8710ec12bde19d593fed1f76c708531bb7aba205110f1029786ead7b

ARG PGSHARD_BUILD_VERSION
ARG PGSHARD_GIT_SHA

LABEL org.opencontainers.image.source="https://github.com/andrew01234567890/pgshard" \
      org.opencontainers.image.version="${PGSHARD_BUILD_VERSION}" \
      org.opencontainers.image.revision="${PGSHARD_GIT_SHA}"

COPY --from=build /out/pgshard-operator /usr/local/bin/pgshard-operator

USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pgshard-operator"]
