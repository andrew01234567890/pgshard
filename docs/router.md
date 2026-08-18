# Router

`pgshard-router serve` is the PostgreSQL wire-protocol front door. Clients
connect to it as they would to PostgreSQL; the router authenticates them from
the catalog, plans each statement onto a shard and executes it through that
shard's `pgshard-pooler` over gRPC. This layer implements **single-shard
sessions for unsharded databases**: every statement of a session goes to the
database's home shard. The planner is a seam (`router.Planner.Plan`) so
sharded planning can replace it without touching the executor.

## Startup and authentication

- **Catalog.** `--catalog-dsn` must be able to read `pgshard.roles.verifier`
  (`pgshard_system`, `pgshard_admin` or a superuser). The router follows the
  catalog through the snapshot watcher (LISTEN/NOTIFY plus periodic reload)
  and refuses to listen until the first snapshot arrives (`--snapshot-wait`).
- **SCRAM.** Every session authenticates with SCRAM-SHA-256 against the
  verifier stored in `pgshard.roles`; verifiers are cached for `--roles-ttl`
  (5s) and reloaded on a miss so new roles work immediately. A wrong password
  or an unknown role is `28P01`. The keys recovered from the exchange
  (`ClientKey`/`ServerKey`) become the `UserIdentity` sent to the pooler on
  the first message of each stream, so no password ever leaves the client.
- **Database.** The startup `database` must exist in `pgshard.databases`
  (else `3D000`). Its `home_shard` in shard set `default` is the session's
  shard. The catalog database itself (`pgshard`) is routable: it maps to
  shard set `catalog`, shard 0, whose pooler is given with
  `--catalog-pooler`.
- **Poolers.** Endpoints come from `pgshard.shard_status.primary_endpoint`
  by default; `--pooler [SET/]ID=host:port` pins one statically. Pooler
  connections use mTLS (`--pooler-tls-cert/-key/-ca`) unless `--insecure-dev`.
- **Fencing.** Every pooler request is stamped with the snapshot's
  `shard_map_generation` and the shard's `primary_epoch`. A stale stamp comes
  back as `55000` to the client; the watcher's next reload clears it.

## Session model

- **Statements.** The simple protocol relays `Query` as one pooler
  `SimpleQuery`. Extended-protocol messages (`Parse`, `Bind`, `Describe`,
  `Execute`, `Close`) are buffered and shipped as one batch on `Sync`, since
  the pooler flushes only on `Sync`; results are streamed back in order and a
  backend error is reported once the batch is drained. Named prepared
  statements are created on the backend as `pgshard_<session>_<name>` (long
  names are hashed).
- **Refused.** `LISTEN`/`NOTIFY`/`UNLISTEN`, `WITH HOLD` cursors and
  temporary tables are refused with `0A000` before reaching a shard; multi-
  statement simple queries are refused by the wire layer. Text the bound
  PostgreSQL 18 grammar cannot parse is forwarded so the backend reports it.
- **Transactions.** `BEGIN` … `COMMIT`/`ROLLBACK` are forwarded; the pooler
  keeps the backend while its `ReadyForQuery` status is not idle. The
  router's own status indicator is the pooler's.
- **Session state.** Session-level `SET`/`RESET` (not `SET LOCAL`) and named
  prepared statements make the session *pinned*: the router calls `Reserve`
  and the pooler dedicates a backend. When a transaction ends the router
  releases the pin (`Release`, which rolls back and `DISCARD ALL`s) so the
  backend returns to the pool; the next statement re-pins and **replays** the
  committed GUCs and prepared statements onto whatever backend it gets. GUCs
  set inside a transaction that rolls back are not replayed. A lost pooler
  stream is reported as `08006` and the next statement reacquires and
  replays too.
- **Cancel.** A `CancelRequest` is verified against the session's key and
  forwarded as the pooler `Cancel` RPC; a query context that ends while a
  batch is in flight (drain) does the same. The batch is always drained to
  `ReadyForQuery` so the stream stays in sync.
- **COPY.** `COPY ... FROM STDIN` relays client chunks to the pooler until
  `CopyDone`/`CopyFail`; `COPY ... TO STDOUT` streams back.
- **Not yet.** `Flush`-driven pipelining (results before `Sync`),
  `PortalSuspended` (`Execute` with a row limit) and `ParameterStatus`
  forwarding are not supported by the pooler contract in this layer.

## Running

```
pgshard-router serve --listen 0.0.0.0:5432 --tls-cert router.crt --tls-key router.key \
  --catalog-dsn 'postgres://pgshard_system@catalog/pgshard' \
  --pooler-tls-cert router-client.crt --pooler-tls-key router-client.key --pooler-tls-ca ca.crt
```

For a local stack, `pgshard-router dev-bootstrap` migrates the catalog and
registers a database, a role (password from `PGSHARD_DEV_PASSWORD`) and shard
0's pooler endpoint, and creates the same role and database on the shard.
`hack/compose/docker-compose.yml --profile router` wires catalog, shard0,
`catalog-init`, `pooler-shard0` and `router` (port 6432) using
`Dockerfile.router`.

## Testing

`go test ./internal/router/` drives a real pgwire server and pgx client
against an in-process scripted pooler: SCRAM auth, `3D000`, refusals, error
and notice relay, transactions, GUC staging across rollback/commit, prepared
statement replay after release, cancel, COPY, stale generation and stream
loss. `go test -tags integration ./test/e2e/router/` builds the pooler and
router, starts a catalog and a shard in Docker, bootstraps them and runs
DDL/DML, prepared statements, rollback, replay-after-release (proved with an
advisory lock the release drops), COPY, cancel (`57014`), `28P01`, `3D000`,
`0A000` and psql. `PGSHARD_BENCH_ROUTER=1 go test -tags integration -bench
RouterSelect1 ./test/e2e/router/` compares `select 1` through the router with
a direct connection.
