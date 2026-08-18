#!/usr/bin/env bash
# Binds to 127.0.0.1 only; the container uses trust auth for local development.
# Usage: hack/pg/run.sh <major> [name] [port]
# Starts a throwaway PostgreSQL container for integration tests (trust auth, logical WAL, prepared xacts).
set -euo pipefail
major=${1:?usage: $0 <major> [name] [port]}
name=${2:-pgshard-pg${major}}
port=${3:-54${major}}
image="ghcr.io/andrew01234567890/pgshard-postgres:${major}"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "warning: $image not built locally (docker buildx bake postgres-${major} --load); falling back to postgres:${major}" >&2
  image="postgres:${major}"
fi
docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p "127.0.0.1:${port}:5432" -e PGDATA=/var/lib/postgresql/data --user postgres --entrypoint sh "$image" -c '
  set -e
  [ -s "$PGDATA/PG_VERSION" ] || initdb -D "$PGDATA" --auth=trust --username=postgres >/dev/null
  echo "host all all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"
  echo "host all all ::/0 trust" >> "$PGDATA/pg_hba.conf"
  exec postgres -D "$PGDATA" -c listen_addresses="*" -c wal_level=logical -c max_prepared_transactions=64
' >/dev/null
for _ in $(seq 1 60); do
  if docker exec "$name" pg_isready -U postgres -q 2>/dev/null; then
    echo "$name ready: postgres://postgres@localhost:${port}/postgres (image $image)"
    exit 0
  fi
  sleep 1
done
echo "error: $name did not become ready" >&2
docker logs "$name" >&2
exit 1
