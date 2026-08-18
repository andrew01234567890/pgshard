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
  `ReadyForQuery` so the stream stays in sync. Keys no local session owns
  are forwarded to peer routers (see *Operations*).
- **COPY.** `COPY ... FROM STDIN` relays client chunks to the pooler until
  `CopyDone`/`CopyFail`; `COPY ... TO STDOUT` streams back.
- **Not yet.** `Flush`-driven pipelining (results before `Sync`),
  `PortalSuspended` (`Execute` with a row limit) and `ParameterStatus`
  forwarding are not supported by the pooler contract in this layer.

## Operations

### Cancel forwarding between instances

Behind a Service a client's `CancelRequest` can land on any router pod,
while the session lives on the pod that minted the key. Every router
therefore runs a small `RouterPeer.Cancel` gRPC service
(`--peer-cancel-listen`, secured with the pooler client certificate and CA,
or plaintext under `--insecure-dev`) and knows its peers either statically
(`--peer ID=host:port`, repeatable) or through DNS (`--peer-service
host:port`, a headless Service whose A/AAAA records are the peers).

- A protocol 3.2 key embeds the minting router's `--instance-id`
  (32 bits; 0 draws a random one, which peers cannot address statically).
  A key that no local session matches is forwarded to the instance it
  names; when that id is not statically known it goes to every discovered
  peer; an unknown id with no discovery is ignored, as PostgreSQL itself
  ignores unmatched cancel keys.
- Protocol 3.0 keys carry no prefix, so a non-local one is sent to every
  peer. The receiving peer only cancels a local session whose secret
  matches; it never forwards again, so a misdirected cancel cannot bounce.
- Forwarded cancels are token-bucket limited (`--peer-cancel-rate`,
  default 50/s with an equal burst); excess ones are dropped and logged.
  Cancel is best effort end to end.

### Drain on SIGTERM

`SIGTERM`/`SIGINT` starts a drain designed for HPA scale-down and rolling
updates:

1. `/readyz` on `--health-listen` turns `503` at once (`/healthz` stays
   `200`) and the router waits `--drain-delay` (5s) with the listener still
   open so endpoint controllers stop routing new connections to it. Point
   the readiness probe at `/readyz`; no `preStop` hook is needed.
2. The listener closes; new connections are refused. Idle sessions get a
   `FATAL 57P01` immediately. Sessions inside an open transaction block, and
   statements in flight, run to the end of the transaction (or statement)
   and are then terminated with `57P01`.
3. After `--drain-timeout` (30s) whatever remains is closed forcibly and the
   process exits.

### Failover buffering

While a shard changes primaries the catalog's `shard_status` shows it as
`fenced` or `migrating` (or without a `primary_endpoint`), and a pooler may
answer `55000` to a stamp with a stale generation or epoch. The router
hides short failovers from clients where it can do so safely:

- A statement whose shard is blocking, or that came back `55000`, or whose
  pooler refused the connection outright, is **buffered** if nothing of it
  has reached the client yet and no transaction block is open: it waits
  until the snapshot shows the shard serving again (LISTEN/NOTIFY wakes it;
  status-only edits are picked up by a 200ms poll) or `--buffer-window`
  (10s) elapses, then runs once more against the refreshed endpoint. A
  window that expires with the shard still blocking is `08006`.
- Inside a transaction block the earlier statements ran on the old primary
  and cannot be replayed, so the statement fails with **`40001`
  (serialization_failure) "shard failover; retry the transaction"**, the
  transaction is dropped and the session returns to idle. `40001` was chosen
  over `08006`/`57P01` because the connection is intact and the transaction
  is retryable as a whole, which is exactly how clients already treat
  serialization failures; connection-class errors would make drivers
  reconnect for no reason.
- A statement that already produced output (rows, a command tag, a notice,
  COPY) is never retried; the original error is reported. A stream lost
  after a statement was sent is likewise not retried, since it may have
  run.
- At most `--buffer-cap` (256) statements wait per shard; the next one is
  refused with `53300`. A client cancel while buffered is honoured with
  `57014`.

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

`go test ./internal/router/...` drives a real pgwire server and pgx client
against an in-process scripted pooler: SCRAM auth, `3D000`, refusals, error
and notice relay, transactions, GUC staging across rollback/commit, prepared
statement replay after release, cancel, COPY, stale generation and stream
loss; two routers on one pooler with cancels forwarded through `RouterPeer`;
the drain sequence against a fake listener and its `/readyz` handler; the
buffering decision table (no output / in transaction / output sent / window
expiry / cap) and each outcome end to end (held select, `40001`, `53300`,
`08006`, `57014` while buffered); and the peer target selection and rate
limit in `cancelpeer`. `go test -tags integration ./test/e2e/router/` builds the pooler and
router, starts a catalog and a shard in Docker, bootstraps them and runs
DDL/DML, prepared statements, rollback, replay-after-release (proved with an
advisory lock the release drops), COPY, cancel (`57014`), `28P01`, `3D000`,
`0A000` and psql. `TestRouterOps` adds a second router that cancels a
session owned by the first, `SIGTERM`s a third with an open transaction
(readiness flips, listener stays open for the delay, the transaction commits,
new connections are refused, the process exits) and fences shard 0 in the
catalog (`update pgshard.shard_status set serving_state`) to show a select
held for the fence and a `40001` inside an open transaction. `PGSHARD_BENCH_ROUTER=1 go test -tags integration -bench
RouterSelect1 ./test/e2e/router/` compares `select 1` through the router with
a direct connection.
