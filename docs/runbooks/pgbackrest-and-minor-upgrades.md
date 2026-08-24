# Runbook: upgrading pgBackRest and PostgreSQL minors

pgBackRest and PostgreSQL are baked into the member image
(`postgres/Dockerfile*`, currently pgBackRest 2.59.1 on PostgreSQL 18 and
19 built from source). Upgrading either means rolling a new image.

## Build and publish the image

1. Bump the version in `postgres/` (the Dockerfile pins both) and let CI
   build, or locally: `docker buildx bake postgres --push`.
2. New tags land as `ghcr.io/andrew01234567890/pgshard-postgres:<major>` and
   `<major>.<minor>`.

## Roll it out

Set `spec.postgresql.image` on the cluster to the new tag (pin by digest
for production). The operator classifies an image change as a rolling
restart: standbys first, then a switchover per group
([rolling-restarts](rolling-restarts.md)). Writes pause per group only for
the shutdown-to-promotion window.

Order matters for PostgreSQL minors: a standby may stream from an older
primary, so the standbys-first order the operator uses is the safe one.
Same-major replication across one minor is supported by PostgreSQL.

## pgBackRest compatibility

- Repository format is stable across pgBackRest releases; newer pgBackRest
  reads existing repositories. Avoid *downgrading* pgBackRest across a
  repository written by a newer release.
- The agent re-runs the idempotent stanza step on startup
  (`stanza-create`, falling back to `stanza-upgrade`), which also covers a
  PostgreSQL minor that changes the control version.
- All members must be able to read the same repository during the rollout;
  since pgBackRest is backward-compatible with its repositories, the mixed
  window (old image restoring, new image archiving) is fine.

## Verify

```sh
kubectl get psc demo -o jsonpath='{.status.rollout} {.status.conditions[?(@.type=="Ready")]}'
kubectl exec <primary-pod> -c postgres -- psql -U postgres -c 'select version()'
kubectl exec <primary-pod> -c postgres -- pgbackrest \
  --config=/etc/pgbackrest/pgbackrest.conf --stanza=<stanza> check
```

Then take a full backup on the new versions and confirm `BackupHealthy`.
A failed rollout holds rather than proceeding
([rolling-restarts](rolling-restarts.md)); rolling *back* is setting the
previous image, which is just another rolling restart — safe for
pgBackRest, and safe for a PostgreSQL minor as long as the data directory
was not touched by a minor that changed on-disk state (minors do not).

Major upgrades are a different machine entirely:
[guide/upgrades.md](../guide/upgrades.md).
