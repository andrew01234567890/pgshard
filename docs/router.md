# Router

`pgshard-router serve` is the PostgreSQL wire-protocol front door. Clients
connect to it as they would to PostgreSQL; the router authenticates them from
the catalog, plans each statement onto a shard and executes it through that
shard's `pgshard-pooler` over gRPC. The planner (`internal/router/plan`)
resolves every statement to the shards it touches from the catalog snapshot;
the executor runs plans that need one shard directly, fans read-only
`SELECT`s over several shards out through a streaming merge (*Scatter*
below) and refuses the rest with `0A000` until cross-shard transactions
(M3.4) and multi-shard writes (M3.5) land. See *Routing* below.

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
  home shard, where unsharded tables live; sharded and reference tables
  take the session to other shards of the set (see *Routing*). The catalog
  database itself (`pgshard`) is routable: it maps to
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
  temporary tables are refused with `0A000` before reaching a shard, as are
  the shapes listed under *Routing*; multi-statement simple queries are
  refused by the wire layer. Text the bound PostgreSQL 18 grammar cannot
  parse is forwarded to the home shard so the backend reports it.
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

## Routing

The planner parses each statement with the bound PostgreSQL 18 grammar and
looks up every relation it references in the snapshot: `(database, schema,
table)` where an unqualified name is searched along the session's search
path: `public` by default, a startup `options=-c search_path=…`, then every
session-level `SET search_path` / `RESET search_path` / `RESET ALL` in order
(staged ones included, so a `SET` inside a transaction takes effect for the
next statement and is dropped on rollback). `pg_catalog`, `information_schema`
and `pg_temp` are always home-shard. `SET LOCAL search_path` and `SET
search_path FROM CURRENT` are refused, as is a shard-key literal in the ON
clause of an outer join (it filters one side only and does not pin the
statement). The effective
placement in `pgshard.table_status` decides:

| Placement | Reads | Writes | DDL |
|---|---|---|---|
| unsharded (or undeclared, database default `unsharded`) | home shard | home shard | home shard |
| reference | any shard (chosen per session) | refused, `0A000` "writes to reference tables are not available yet (planned for M3.5)" | refused, "DDL fan-out is not available yet" |
| sharded | shard of the key | shard of the key | refused, "DDL fan-out is not available yet" |

A plan has a `Kind`: `Unsharded`, `EqualUnique` (every sharded table pinned
to one key value), `In` (key in a list of values), `Scatter` (a sharded
table with no key predicate), `Reference`, `SessionLocal` (`BEGIN`, `SET`,
`SHOW`, … — runs wherever the session is) or `Refuse`. It carries the
shard ids, the resolved key values and the shard map generation it was
made against. Statements are refused before anything reaches a shard; every
refusal is `0A000` with a message naming the rule and a hint.

**Shard keys.** The key value comes from `WHERE` conjunctions of the form
`key = <const|$n>` or `key IN (<const|$n>, …)` (also `t.key`, casts, and
`USING (key)`/`NATURAL` joins); `OR`, ranges, expressions and subqueries do
not route and make the table a scatter. `INSERT` needs the key column in
its column list with a constant or parameter per `VALUES` row. Values are
hashed with the PostgreSQL extended hash (`internal/placement`) and located
in the `default` shard set's ranges. Because the catalog does not record the
key's type, the router types the value from the statement: integer literals
and `::int8` casts hash as `int8`, string literals and `::text` casts as
`text`; a **string literal that looks numeric is refused** ("shard key
literal '1' is untyped and looks numeric") until it is cast. Bind parameters
take the type the client declared at `Parse`, else the type the backend
reported in `ParameterDescription` for that statement (drivers that
prepare-and-describe, such as pgx's default mode and JDBC, therefore always
carry the right type), else a cast in the statement text; an undeclared
text-format value that looks numeric is refused with the same hint (`$1::int8`
or `$1::text`), never guessed.

**Bind-time routing.** A statement whose key is a parameter is planned at
`Parse` as *deferred*; the shard is computed at `Bind`, when the router also
switches the session's pooler stream to that shard. A statement prepared
against an older catalog snapshot is planned again at `Bind`. One extended
batch (everything up to `Sync`) must target one shard, else `0A000`
"statements of one batch target different shards".

**Transactions.** A session moves between shards freely while idle. Inside
a transaction block the session is pinned to the first shard a statement
touched; a statement for another shard is refused with `0A000` "multi-shard
transactions are not available yet: transaction is on shard default/0,
statement needs shard default/3" and the transaction stays open. `BEGIN` and
other session-local statements do not pin: they are recorded as the
transaction's *prelude* and replayed on the shard of the first real
statement (`BEGIN; INSERT INTO sharded …` works). Named prepared statements
and session GUCs are replayed on every shard the session moves to.

**Scatter (multi-shard reads).** A read-only `SELECT` over **one sharded
table** whose plan needs several shards (no key predicate, or `IN` spanning
shards) runs on every target shard at once: the router opens one pooler
stream per shard (session id `<sid>-x<shard>`, stamped with the shard's
generation, on a fresh unpinned backend), sends the same statement — possibly
rewritten as described below — and streams the merged rows to the client
with `SELECT <total>` as the command tag. The `RowDescription` comes from the
first shard and every other shard's must match it (name, type, typmod,
format), else `XX000` "shards … disagree on the result shape". The first
shard error cancels the other participants (pooler `Cancel`) and is
reported once every stream has drained; a client cancel cancels every
participant (each reports `57014`). Supported shapes:

| Shape | Shard statement | Router |
|---|---|---|
| plain scan (`WHERE` on non-key columns allowed) | unchanged | streams are concatenated in shard order; the interleave between shards is arbitrary, as it is for an unordered PostgreSQL scan |
| `ORDER BY` | unchanged, or with the sort expressions appended as hidden columns (`… AS __pgshard_sort_N`) when they are not in the select list; hidden columns are stripped before the client sees the row | streaming k-way merge on the text-format values: `int2/4/8`, `oid`, `float4/8` (NaN last), `numeric` (exact), `bool`, `date`, `timestamp`, `timestamptz` (±infinity, BC), `uuid`, `bytea`, `"char"`; `text`/`varchar`/`bpchar`/`name` **only with an explicit `COLLATE "C"` or `"POSIX"`** on the key (the router orders bytewise and cannot apply another collation) — else `0A000` "multi-shard ORDER BY on a text column needs an explicit COLLATE "C""; `ASC`/`DESC`, `NULLS FIRST`/`LAST` (PostgreSQL defaults), ties keep shard order |
| `LIMIT n [OFFSET k]` | `LIMIT n+k`, no `OFFSET` (saturating at `int8` max) | `OFFSET k` then `LIMIT n` after the merge; a bare `OFFSET` is removed from the shard statement and applied at the router |
| `count(*)`, `count(x)`, `sum(x)`, `min(x)`, `max(x)` without `GROUP BY` — every select-list entry must be one of these, unadorned | unchanged (`LIMIT`/`OFFSET` removed) | one row per shard is combined: counts and sums are added (`int8` and `numeric` exactly, `numeric` keeps the widest scale, `float4/8` in float arithmetic with PostgreSQL's shortest output format), `min`/`max` use the ORDER BY comparators; NULL inputs are skipped and an all-NULL input stays NULL; `LIMIT`/`OFFSET` then apply to the single row |
| `GROUP BY` including the shard key, `DISTINCT` (or `DISTINCT ON`) including the shard key | unchanged | every group or distinct row lives on one shard, so the streams are concatenated (or merged for `ORDER BY`); aggregates in such a query are computed on the shards |

Refused with `0A000` (message names the reason): `avg()` and every other
aggregate ("multi-shard avg() is not available yet" — compute `sum(x)` and
`count(x)`), aggregates with `DISTINCT`/`FILTER`/`ORDER BY`/`OVER`,
expressions around or beside an aggregate (`count(*) + 1`, `id, count(*)`),
`GROUP BY`/`DISTINCT` without the shard key, `HAVING` without such a
`GROUP BY`, `LIMIT`/`OFFSET` that are not integer constants (`$1`,
expressions), `FETCH … WITH TIES`, `ORDER BY … USING`, `ORDER BY` on a
type without a comparator (`jsonb`, arrays, …), `min()`/`max()` over a text
column, `sum()` over a non-numeric type, `SELECT DISTINCT` ordered by an
expression outside the select list, window functions, `FOR UPDATE/SHARE`,
`SELECT INTO`, set operations, CTEs, subqueries, joins (including with
reference tables) and function scans, and `EXPLAIN`/`DECLARE CURSOR` of a
scatter. `ORDER BY 3` past the select list is `42P10`, a negative `LIMIT`
`2201W`, as in PostgreSQL.

Session rules: a scatter runs on autocommit backends outside the session's
transaction, so it is allowed inside a transaction block only while the
transaction has **not touched a shard** yet (it does not pin the transaction
either), and refused ("multi-shard read inside a transaction pinned to shard
…") after the first write or keyed read; `BEGIN ISOLATION LEVEL REPEATABLE
READ`/`SERIALIZABLE`, `SET TRANSACTION …` in the prelude, or a session
`default_transaction_isolation`/`SESSION CHARACTERISTICS` of those levels
refuse every scatter ("multi-shard reads under REPEATABLE READ or
SERIALIZABLE isolation are not available yet"): the shards take independent
snapshots. Session GUCs (`SET`) are **not** replayed on the scatter backends.
Through the extended protocol a scatter statement must be the only statement
of its batch (`Bind` and `Execute` before one `Sync`; a `Parse`+`Describe`
round trip on its own runs on the session's shard, so drivers that prepare
first work), is rewritten onto the unnamed statement and portal on every
shard, and `Execute` with a row limit (partial portal fetch) is refused
("partial fetches … from a multi-shard portal are not available yet").
`--scatter-max-shards` (default 0 = all) caps the shards one statement may
touch and `--scatter-max-streams` (4096) the scatter streams open across
the router; a statement waits for capacity and a client cancel while
waiting is honoured.

**Refusals (all `0A000`).**

| Statement shape | Message |
|---|---|
| multi-shard `SELECT` outside the *Scatter* shapes below (window functions, FOR UPDATE/SHARE, SELECT INTO, set operations, CTEs, subqueries, joins, function scans; `EXPLAIN`/`DECLARE` of one) | multi-shard SELECT with … is not available yet; cross-shard join is not available yet; only a plain SELECT can run on multiple shards |
| `UPDATE`/`DELETE` without a key predicate | scatter UPDATE/DELETE without a shard key predicate is not available yet |
| tables that do not resolve to one shard (joins, subqueries, set operations, an unsharded table joined to a sharded row off the home shard) | cross-shard join is not available yet |
| `INSERT` without the key in the column list | insert requires the shard key |
| `INSERT` key that is not a constant or parameter; `INSERT … SELECT` | shard key of an INSERT must be a constant or a parameter; INSERT … SELECT into a sharded table is not available yet |
| multi-row `INSERT` whose rows hash to different shards | multi-row INSERT spanning shards is not available yet — M3.5 |
| `UPDATE … SET key`, `ON CONFLICT DO UPDATE SET key` | shard key is immutable |
| writes to reference tables | writes to reference tables are not available yet (planned for M3.5) |
| DDL, `TRUNCATE`, `VACUUM`, `LOCK`, `COPY` on sharded or reference tables | DDL fan-out is not available yet; COPY on sharded and reference tables is not available yet |
| `CREATE TABLE` of a declared sharded table without the key column, or with a PRIMARY KEY/UNIQUE that omits it | sharded table must define its shard key column; primary key or unique constraint (…) must include the shard key |
| `CREATE VIEW`/`CREATE TABLE AS` over sharded or reference tables | CREATE VIEW over sharded or reference tables is not available yet |
| `SET LOCAL search_path`, `SET search_path FROM CURRENT` | SET LOCAL search_path is not available yet; SET search_path FROM CURRENT is not available yet |
| SQL-level `PREPARE` touching sharded or reference tables; data-modifying CTEs | SQL-level PREPARE … is not available yet; data-modifying statements in WITH are not available yet |
| undeclared table in a database whose default placement is `sharded` | table is not declared in the catalog and the database defaults to sharded placement |

DDL on unsharded tables goes to the home shard, an interim until DDL
fan-out (M4). Reference reads pick a shard from the session id so they
spread across the shard set.

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
registers a database, a role (password from `PGSHARD_DEV_PASSWORD`) and one
shard's pooler endpoint (`--shard-id`, default 0), and creates the same role
and database on that shard; run it once per shard, the role keeps one SCRAM
verifier across runs.
`hack/compose/docker-compose.yml --profile router` wires catalog, shard0,
`catalog-init`, `pooler-shard0` and `router` (port 6432) using
`Dockerfile.router`.

## Testing

`go test ./internal/router/plan/` runs the golden plan table (over 200
statements against a fixture of four shards, an unsharded table, a reference
table and two sharded tables with an int8 and a text key: kind, shards,
deferred resolution and every refusal's SQLSTATE and message) and the
parameter decoding rules. `go test ./internal/router/...` drives a real
pgwire server and pgx client against an in-process scripted pooler — four
of them for the sharded tests: keyed inserts and selects reach the key's
shard through the simple and extended protocols, prepared statements follow
the key across shards and are re-planned when the snapshot changes,
transactions move on their prelude and refuse a second shard, one batch is
refused for two shards, and each refusal leaves the session usable; scatter
reads concatenate every shard, merge ordered streams with the pushed-down
`LIMIT` observed on each shard, combine `count`/`sum`/`min`/`max`,
refuse text ordering without `COLLATE "C"`, surface one shard's error and a
shape mismatch, cancel every participant on a client cancel, and refuse
mixed batches, partial fetches, pinned transactions and strict isolation —
plus SCRAM auth, `3D000`, refusals, error
and notice relay, transactions, GUC staging across rollback/commit, prepared
statement replay after release, cancel, COPY, stale generation and stream
loss; two routers on one pooler with cancels forwarded through `RouterPeer`;
the drain sequence against a fake listener and its `/readyz` handler;
`go test ./internal/router/scatter/` covers the typed comparators, NULLS
ordering and ties, the k-way merge with LIMIT/OFFSET and hidden columns,
the aggregate combiners and PostgreSQL float/numeric formatting, and
`internal/router/plan`'s merge tests the ORDER BY column resolution, the
LIMIT+OFFSET pushdown arithmetic and every scatter refusal; the
buffering decision table (no output / in transaction / output sent / window
expiry / cap) and each outcome end to end (held select, `40001`, `53300`,
`08006`, `57014` while buffered); and the peer target selection and rate
limit in `cancelpeer`. `go test -tags integration ./test/e2e/router/` builds the pooler and
router, starts a catalog and a shard in Docker, bootstraps them and runs
DDL/DML, prepared statements, rollback, replay-after-release (proved with an
advisory lock the release drops), COPY, cancel (`57014`), `28P01`, `3D000`,
`0A000` and psql. `TestRouterShardedRouting` adds a second shard container
and pooler, declares a sharded table in the catalog and proves through
direct connections to each PostgreSQL that keyed inserts, selects, updates
and deletes land on the key's shard only, that a transaction pins to its
first shard, that the refusal list holds end to end and that unsharded
tables stay on the home shard. `TestRouterScatterDifferential` starts an
oracle PostgreSQL and three shards, loads the same 5,000 rows into the
oracle and hash-partitioned into the shards, and runs a corpus of scatter
queries (ORDER BY asc/desc/nulls/multi-key, LIMIT/OFFSET, count/sum/min/max,
GROUP BY and DISTINCT on the key, plain scans compared as multisets)
through the router and against the oracle, requiring identical results in
both protocols; it also cancels a `pg_sleep` scatter and checks that every
shard reported `57014`. `TestRouterOps` adds a second router that cancels a
session owned by the first, `SIGTERM`s a third with an open transaction
(readiness flips, listener stays open for the delay, the transaction commits,
new connections are refused, the process exits) and fences shard 0 in the
catalog (`update pgshard.shard_status set serving_state`) to show a select
held for the fence and a `40001` inside an open transaction. `PGSHARD_BENCH_ROUTER=1 go test -tags integration -bench
RouterSelect1 ./test/e2e/router/` compares `select 1` through the router with
a direct connection.
