# Pooler

`pgshard-pooler` fronts one shard's PostgreSQL. Routers reach it over gRPC
(`pgshard.v1.Pooler`) and never open PostgreSQL connections themselves.

## Execution model

- **Per-role pools.** Backends are keyed by PostgreSQL role. A router session's
  first `Execute` message carries `UserIdentity{username, scram_client_key,
  scram_server_key}`; the pooler dials PostgreSQL as that role using a
  SCRAM-SHA-256 client that proves possession of `ClientKey`/`ServerKey`
  (proof = `ClientKey XOR HMAC(H(ClientKey), authMessage)`, server signature
  checked with `ServerKey`). No password ever reaches the pooler.
- **Trust boundary.** The router authenticated the client; the pooler trusts
  the router (mTLS on the gRPC listener). Keys are needed to *dial* **and to
  reuse**: an idle backend is bound to a digest of the keys that
  authenticated it, and `popIdle` closes one whose digest does not match the
  caller's rather than handing it over; a session id is bound to (role,
  keys) and a reattach presenting different ones is refused with
  `PermissionDenied`. mTLS is not the only thing separating roles.
  Consequently the gRPC listener refuses to start without
  `--tls-cert/--tls-key/--tls-ca` unless `--insecure-dev` is passed.
- **Backend authentication.** A backend connection is accepted only after
  a complete SCRAM-SHA-256 exchange whose server signature verified against
  the forwarded ServerKey; an `AuthenticationOk` without it (trust or
  password authentication in `pg_hba.conf`) is refused. TCP backends can be
  upgraded to TLS with `--pg-sslmode require|verify-full` (the latter with
  `--pg-sslrootcert`); unix sockets are never upgraded. `require` follows
  libpq's definition: it encrypts the connection but does not authenticate
  the server, so it does not prevent a man in the middle between the pooler
  and PostgreSQL. `verify-full` is the setting that authenticates, checking
  the chain against `--pg-sslrootcert` and the host name, and refusing to
  start without a root certificate. This choice only arises for a TCP
  backend: the operator's rendered pods reach PostgreSQL over a unix socket
  in the same pod, and the flag defaults to `disable`.
- **Regular vs reserved.** By default a backend is held only from the first
  message of a batch until PostgreSQL reports `ReadyForQuery` with status
  `I`; a transaction (`T`/`E`) keeps it. `Reserve` pins the session's backend
  (or the next one it acquires) until `Release`, which rolls back any open
  transaction, runs `DISCARD ALL`, and returns it to the pool. A reserved
  session whose `Execute` stream has been gone for `--reserve-timeout`
  (5m) is released the same way, so a router that died without `Release`
  cannot hold a backend forever.

  An expiry that rolls back an **open transaction** is remembered for half
  an hour: the next `Execute` for that session id is refused with
  `Aborted`, naming the timeout and saying the transaction was rolled back,
  rather than being handed a fresh session that would let the client carry
  on as though its transaction were still open. The refusal is once -- the
  session id works again immediately afterwards, as a new session. An idle
  reservation expiring costs the client nothing and is silent.

  This does not stop the expiry taking the transaction: a client that sits
  idle inside one for longer than `--reserve-timeout`, with its router
  perfectly alive, still loses it. Keeping such a session is a router-side
  liveness ping and is tracked separately.
- **Asynchronous messages.** `NoticeResponse`, `ParameterStatus` and
  `NotificationResponse` (LISTEN payloads) from the backend are forwarded on
  the session's stream in order.
- **Budget.** `--max-backends` caps the shard; `--max-per-role` caps each
  role so a hot role cannot starve others. When the shard budget is full of
  idle backends of other roles one is evicted. Backends retire after
  `--backend-max-lifetime` and `--backend-max-idle`.
- **Fencing.** Every `Execute` message and every `Reserve` carries
  `Generation{shard_map_generation, primary_epoch}`. A mismatch with the
  pooler's view is refused *before* anything reaches PostgreSQL with
  SQLSTATE `55000` and message `stale routing generation` or `stale primary
  epoch`; a missing generation is `missing routing generation`. The view comes
  from a `Source`: static flags (`--generation`, `--epoch`) or the catalog
  (`--catalog-dsn --shard-set --shard-id`) through the snapshot watcher. The
  agent/operator will drive it later.
- **Cancel.** The `Cancel` RPC (or an in-stream `CancelRequest`) sends a
  PostgreSQL `CancelRequest` for the backend bound to that session over a
  fresh connection.
- **COPY.** `CopyInResponse` returns control to the router; `CopyData`,
  `CopyDone` and `CopyFail` are relayed; COPY OUT data is streamed back.
- **Health.** `Health` streams epoch, generation and `serving` (false once
  draining) from the `Source`. **`role` and `lag_bytes` are not derived**:
  the pooler is started with `role` fixed at `PRIMARY` and never revises it,
  so one beside a standby reports itself primary, and `lag_bytes` is always
  zero. Nothing in this repository consumes the stream today; do not read
  either field until they are computed.
- **Stream / Ack.** `Stream` opens the shard's logical slot (`slot`, or
  `pgshard_<stream>_<group>` derived from `stream` and `--stream-shard`) over
  a replication connection (`--stream-dsn`) and streams decoded pgoutput v4
  events in `ChangeBatch`es, one per transaction boundary (commit, prepare,
  streamed segment, non-transactional message) or per `batch_bytes`
  (64 KiB cap). `start_lsn` zero resumes from the slot's confirmed position;
  a `Keepalive` batch is sent when idle; a second reader on the same slot is
  refused with `FAILED_PRECONDITION`. `Ack(lsn)` advances the slot's
  `confirmed_flush_lsn` through the reader's standby status update and
  returns once that update **has been sent** — the replication protocol does
  not acknowledge a standby status, so what is known is that the kernel took
  the bytes, not that the walsender applied them. A stalled walsender or a
  half-open connection still lets `Ack` succeed; read
  `pg_replication_slots.confirmed_flush_lsn` if a durable answer is needed. `StreamChanges` is the same stream one
  event per message. See [streams.md](streams.md).
- **CopyTables.** The copy phase of an initial copy: exports a snapshot
  from the stream's slot (created on the spot) or a temporary one, and
  streams every table of the publication as seen by that snapshot in
  primary-key (or ctid) order with a `lastpk` checkpoint per batch; resumes
  after a checkpoint. See [streams.md](streams.md).

## Drain

On SIGINT/SIGTERM the pooler drains in two stages: it stops admitting new
sessions and reservations (`Unavailable`; new batches on idle sessions get
SQLSTATE `57P03`), lets sessions that hold a backend finish their transaction
until `--drain-timeout`, then closes every backend and exits.

## Key hygiene

The key bytes in the first `Execute` message are zeroised as soon as they are
copied; the session's copies are zeroised when the stream ends; the dial path
does not retain them; keys are never logged (a test greps the logs).

## Running

```
pgshard-pooler run --listen 0.0.0.0:15432 --pg-socket-dir /var/run/postgresql \
  --pg-database app --tls-cert pooler.crt --tls-key pooler.key --tls-ca ca.crt \
  --catalog-dsn 'postgres://...' --shard-set main --shard-id 0
```

`--pg-host/--pg-port` replace `--pg-socket-dir` for TCP. `--help` and
`--version` behave as for every pgshard command.

## Testing

`go test ./internal/pooler/` runs unit tests against an in-process fake
PostgreSQL (budget, fairness, fencing, reserve/release, drain, key
zeroisation, health). With Docker available it also runs the same relay
against real PostgreSQL 18 and 19: keys are derived from a role's password
plus the salt/iterations of its `pg_authid` verifier, then `select
current_user`, permission denied, prepared statements, COPY IN/OUT, stale
generation, cancel and drain are exercised end to end.
