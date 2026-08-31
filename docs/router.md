# Router

`pgshard-router serve` is the PostgreSQL wire-protocol front door. Clients
connect to it as they would to PostgreSQL; the router authenticates them from
the catalog, plans each statement onto a shard and executes it through that
shard's `pgshard-pooler` over gRPC. The planner (`internal/router/plan`)
resolves every statement to the shards it touches from the catalog snapshot;
the executor runs plans that need one shard directly, fans read-only
`SELECT`s over several shards out through a streaming merge (*Scatter*
below), coordinates transactions that write to several shards with
two-phase commit (*Transactions* below), replicates reference-table writes
to every shard (*Reference tables* below), fills the global sequence
columns of sharded tables from the catalog (*Sequences* below) and refuses
the rest with `0A000`. See *Routing* below.

## Startup and authentication

- **Catalog.** `--catalog-dsn` must be able to read `pgshard.roles.verifier`
  and to write the decision log and the migration queue. The operator points
  it at `pgshard_router`, a login role the catalog schema creates for exactly
  that: `pgshard_admin` and `pgshard_reader` plus INSERT/UPDATE/DELETE on
  `pgshard.xact_decisions` and INSERT/UPDATE on `pgshard.migrations`, with no
  superuser, `CREATEROLE` or `CREATEDB`. Its password is generated per
  cluster into the Secret `<cluster>-router`.

  It is deliberately **not** the superuser: the router is the one component
  untrusted clients connect to, parsing their protocol and their SQL, and the
  superuser password is also the seed of the agent control-plane token
  (`internal/agentauth`) — one compromised router would otherwise be the
  whole cluster. Running the router by hand with a superuser DSN still works
  and is what the development harnesses do.

  The router follows the catalog through the snapshot watcher (LISTEN/NOTIFY
  plus periodic reload) and refuses to listen until the first snapshot
  arrives (`--snapshot-wait`).
- **SCRAM.** Every session authenticates with SCRAM-SHA-256 against the
  verifier stored in `pgshard.roles`; verifiers are cached for `--roles-ttl`
  (5s), reloaded on a miss so new roles work immediately, and reloaded once
  per TTL when a cached credential is the thing refusing -- a password
  renewed or a role re-enabled a moment ago otherwise looks disabled until
  the TTL comes round. A wrong password
  or an unknown role is `28P01`, and so is a role that may not log in or
  whose password has expired **until the client proves the password**: the
  exchange runs either way, and the refusal that names the role (`28000`,
  "not permitted to log in", "password has expired") is relayed only to a
  caller who got the password right. Answering it earlier told anyone who
  asked whether a role existed, since an unknown one gets a mock exchange.
  PostgreSQL draws the same line -- `rolcanlogin` is checked after
  authentication, not before. The keys recovered from the exchange
  (`ClientKey`/`ServerKey`) become the `UserIdentity` sent to the pooler on
  the first message of each stream, so no password ever leaves the client.
  That is not the same as the keys being safe from the shards, and the
  distinction matters -- see *Trust boundary* below.
- **Trust boundary.** The shards are inside it. Every group carries the
  same SCRAM verifier for a role (see [roles.md](roles.md)), and the pooler
  dials a shard as the real user by proving possession of the forwarded
  `ClientKey`. A shard therefore sees a SCRAM exchange from a party holding
  that user's key material, and a shard under an attacker's control is in a
  position to derive it from what it holds and what it observes. Once it
  has, it can authenticate as that user anywhere the same verifier is
  installed: the other shards, the catalog, and the router itself.

  So a compromised shard is a compromise of every role whose queries
  reached it, not of that shard's data alone. **Rotate the passwords of
  affected roles after any shard compromise**, and treat shard-to-shard and
  shard-to-catalog isolation as a containment measure rather than a
  boundary this authentication model enforces.

  This is a consequence of the design choice recorded in the charter:
  terminating SCRAM at the router and forwarding the client's keys, rather
  than connecting as a service role and issuing `SET ROLE`. Key forwarding
  keeps the shard's own privilege checks working against the real user,
  which `SET ROLE` gives up; what it costs is this. Removing the cost means
  a distinct pooler service identity with controlled role assumption, or
  per-shard short-lived delegated credentials -- neither of which exists
  today.

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
- **Reported version.** `server_version` is `--server-version`, default
  `18.6 (pgshard)`. It is a fixed string, not derived from what the shards
  run, so a cluster serving PostgreSQL 19 reports 18.6 unless this is set.
  Deriving it needs the router to learn the shards' version, and during a
  rolling major upgrade to decide which of two answers is the cluster's
  (PGS-471); until then it is at least correctable without a rebuild.
- **Fencing.** Every pooler request is stamped with the snapshot's
  `shard_map_generation` and the shard's `primary_epoch`. A pooler refuses a
  stale stamp with `55000` and says so is a fence; the router buffers and
  retries where it safely can, and where it cannot the client is told
  `40001` — retry, nothing was written — never the pooler's own `55000`,
  which names a state and no way out of it.

- **Payload limit.** A single Bind value, `DataRow` or COPY chunk travels
  between router and pooler as one protobuf message, capped at **4 MiB**
  (`pooler.MaxMessageBytes`, set explicitly on both sides). PostgreSQL's own
  protocol limit is far larger, so this is a deliberate narrowing: exceeding
  it is `54000` (`program_limit_exceeded`) naming the limit, not a
  connection error. Raising it waits on byte-weighted admission — row
  channels and writer flushes are bounded by item count, so a larger limit
  would let a few rows hold hundreds of megabytes in the router.

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
  router's own status indicator is the pooler's. A transaction that touches
  several shards is coordinated by the router; see *Transactions* below.
- **Session state.** Session-level `SET`/`RESET` (not `SET LOCAL`) and named
  prepared statements make the session *pinned*: the router calls `Reserve`
  and the pooler dedicates a backend. When a transaction ends the router
  releases the pin (`Release`, which rolls back and `DISCARD ALL`s) so the
  backend returns to the pool; the next statement re-pins and **replays** the
  committed GUCs and prepared statements onto whatever backend it gets. GUCs
  set inside a transaction that rolls back are not replayed, nor are those
  set after a savepoint the transaction rolled back to. SQL-level
  `PREPARE` pins and is replayed like a named protocol statement;
  `DEALLOCATE` (of either kind of statement, protocol names are rewritten
  to their physical name) and `DEALLOCATE ALL`/`DISCARD ALL` stop the
  replay of what they dropped. A lost pooler stream is reported as `08006`
  and the next statement reacquires and replays too.
- **Transaction-mode caveats.** As with PgBouncer in transaction mode,
  state that the router does not track is lost when the backend changes:
  the unnamed prepared statement lives only until the next `Sync` (a
  `Parse` of `""` in one batch and a `Bind` of it in a later one may land on
  different backends, so the router re-parses the unnamed statement ahead of
  a batch that binds without parsing it — carried rather than pinned, since
  pinning would cost every such session its transaction pooling),
  `SET LOCAL` is honoured only within its transaction,
  and `LISTEN` and temporary tables (both refused) do not survive a
  release. **Session advisory locks are refused** (`0A000`): PostgreSQL
  keeps one until it is unlocked or the session ends, but the backend
  holding it does not stay with the session, so granting one would leave
  it on a backend another session goes on to use — and two clients would
  each believe they held it, which for leader election or a migration
  means both proceed. The transaction-scoped forms
  (`pg_advisory_xact_lock` and friends) are allowed: the transaction is
  already pinned to one backend and PostgreSQL releases them with it.
- **Cancel grace.** A cancelled statement is drained to `ReadyForQuery` so
  the stream stays in sync, but only for 5s. The cancel is best-effort —
  it can fail, and a backend can be wedged somewhere PostgreSQL will not
  interrupt it — and past the grace the stream is aborted rather than
  waited on, since no answer is coming and an unbounded wait holds the
  session, its pooler session and the router's drain open.
- **Cancel.** A `CancelRequest` is verified against the session's key and
  forwarded as the pooler `Cancel` RPC; a query context that ends while a
  batch is in flight (drain) does the same. The batch is always drained to
  `ReadyForQuery` so the stream stays in sync. Keys no local session owns
  are forwarded to peer routers (see *Operations*).
- **COPY.** `COPY ... FROM STDIN` relays client chunks to the pooler until
  `CopyDone`/`CopyFail`; `COPY ... TO STDOUT` streams back.
- **`ParameterStatus`.** A GUC_REPORT setting a backend reports as changed
  is forwarded to the client, including the report PostgreSQL sends when a
  `SET LOCAL` is undone by `ROLLBACK` or a savepoint. Only changes: the
  router replays session state onto every backend it moves a session to, and
  a backend repeating what the session already asked for is not news. The
  values advertised at startup are the router's own and are not yet read
  back from a backend.
- **Not yet.** `Flush`-driven pipelining (results before `Sync`) and
  `PortalSuspended` (`Execute` with a row limit) are not supported by the
  pooler contract in this layer.

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
| unsharded (or undeclared, database default `unsharded`) | home shard | home shard | migration, home shard (see *DDL*) |
| reference | any shard (chosen per session) | every shard of the set, in the session's transaction (see *Reference tables*) | migration, every shard |
| sharded | shard of the key | shard of the key | migration, every shard |

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
a transaction block the session opens one backend per shard it touches
(see *Transactions*). `BEGIN` and other session-local statements do not
pin: they are recorded as the transaction's *prelude* and replayed on the
shard of the first real statement and on every further participant
(`BEGIN; INSERT INTO sharded …` works). Named prepared statements and
session GUCs are replayed on every shard the session moves to.

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
SERIALIZABLE isolation are not available yet").

Session state **is** carried. Every participant is reserved and given the
session's committed `SET`s before the statement runs, together with the
`search_path` the planner resolved relations under. `SET ROLE` in particular
decides which grants and row-level security policies apply, and a participant
that missed it would evaluate the query as the login role.

What a scatter does **not** give you is one snapshot. Each shard takes its
own, at the moment its participant runs, so a scatter is a set of
single-shard reads whose results are merged — not a read of the cluster at
one logical instant. Concurrent commits can therefore produce a combination
of rows that never existed together, which also makes a scatter under READ
COMMITTED weaker than PostgreSQL's statement-level READ COMMITTED: those
per-shard snapshots need never have coexisted. Read-your-writes within one
shard is unaffected.

That is why `REPEATABLE READ` and `SERIALIZABLE` are refused rather than
approximated, and the same levels also refuse a **keyed transaction that
reaches a second shard** ("a transaction under REPEATABLE READ or
SERIALIZABLE isolation cannot span shards"), for reads and writes alike.
Two-phase commit makes the outcome atomic; it does not make the snapshots
one snapshot. Each shard would run its own PostgreSQL transaction at that
level and each could choose a locally valid serialization order while the
combined history has a cycle neither can see, so the transaction would
commit with write skew rather than raising `40001`. Cross-shard invariants
-- balances, quotas, uniqueness, authorization -- cannot be protected by
PostgreSQL isolation levels here: keep them behind one shard key, or
serialise them in the application. Honouring them needs a cluster read
epoch: a global timestamp and cross-shard read certification, which pgshard
does not have.
Through the extended protocol a scatter statement must be the only statement
of its batch (`Bind` and `Execute` before one `Sync`; a `Parse`+`Describe`
round trip on its own runs on the session's shard, so drivers that prepare
first work), is rewritten onto the unnamed statement and portal on every
shard, and `Execute` with a row limit (partial portal fetch) is refused
("partial fetches … from a multi-shard portal are not available yet").
`--scatter-max-shards` (default 0 = all) caps the shards one statement may
touch and `--scatter-max-streams` (4096) the scatter streams open across
the router; a statement waits for capacity and a client cancel while
waiting is honoured. That wait is bounded by `--scatter-max-wait` (30s;
negative waits for ever), after which the statement is refused with
`53300` and a retry hint. Unbounded, a burst of wide scatters parks every
later statement indefinitely, and a statement needing many streams is
overtaken for ever by smaller ones taking each stream as it is freed --
the client sees a session that stopped answering rather than an error it
could act on.

**Refusals (all `0A000`).**

| Statement shape | Message |
|---|---|
| multi-shard `SELECT` outside the *Scatter* shapes below (window functions, FOR UPDATE/SHARE, SELECT INTO, set operations, CTEs, subqueries, joins, function scans; `EXPLAIN`/`DECLARE` of one) | multi-shard SELECT with … is not available yet; cross-shard join is not available yet; only a plain SELECT can run on multiple shards |
| `UPDATE`/`DELETE` without a key predicate | scatter UPDATE/DELETE without a shard key predicate is not available yet |
| tables that do not resolve to one shard (joins, subqueries, set operations, an unsharded table joined to a sharded row off the home shard) | cross-shard join is not available yet |
| `INSERT` without the key in the column list | insert requires the shard key |
| `INSERT` key that is not a constant or parameter; `INSERT … SELECT` | shard key of an INSERT must be a constant or a parameter; INSERT … SELECT into a sharded table is not available yet |
| multi-row `INSERT` whose rows hash to different shards | multi-row INSERT spanning shards is not available yet |
| `UPDATE … SET key`, `ON CONFLICT DO UPDATE SET key` | shard key is immutable |
| reference-table write that reads a sharded or unsharded table, or calls a volatile function | a write to reference table … cannot read sharded or unsharded tables; … cannot call now() (see *Reference tables*) |
| `TRUNCATE`, `VACUUM`, `LOCK`, `COPY` on sharded or reference tables | TRUNCATE/LOCK TABLE/VACUUM and ANALYZE on sharded and reference tables is not available yet; COPY on sharded and reference tables is not available yet |
| `CREATE TABLE`, `CREATE UNIQUE INDEX`, `ALTER TABLE ADD PRIMARY KEY/UNIQUE` on a declared sharded table without the key column, or with a PRIMARY KEY/UNIQUE that omits it | sharded table must define its shard key column; primary key or unique constraint (…) must include the shard key |
| `SET`, `SET LOCAL`, `set_config` or `ALTER ROLE … SET` of `standard_conforming_strings` | changing standard_conforming_strings is not permitted through pgshard — the router parses and hashes shard keys with it on, so a session reading literals differently would place rows on a shard the router would not look on |
| DDL forms the migration model refuses (transaction block, rewrite class, shard key changes, mixed sharded/unsharded objects, `CREATE TABLE AS` over sharded tables) | see *DDL* |
| `SET LOCAL search_path`, `SET search_path FROM CURRENT` | SET LOCAL search_path is not available yet; SET search_path FROM CURRENT is not available yet |
| SQL-level `PREPARE` touching sharded or reference tables; data-modifying CTEs | SQL-level PREPARE … is not available yet; data-modifying statements in WITH are not available yet |
| undeclared table in a database whose default placement is `sharded` | table is not declared in the catalog and the database defaults to sharded placement |

Reference reads pick a shard from the session id so they spread across the
shard set.

### DDL

The session never runs DDL or DCL itself. `CREATE`/`ALTER`/`DROP` of tables,
indexes, schemas, views, sequences and types, `CREATE`/`DROP DATABASE`,
`CREATE`/`ALTER`/`DROP ROLE|USER`, `GRANT`/`REVOKE`, `REINDEX` and
`ALTER … RENAME|SET SCHEMA|OWNER TO` are validated by the planner, written to
`pgshard.migrations` as a *migration* and applied on every target shard by
the controller's applier (`docs/ddl.md`). The client waits, as with
PostgreSQL, until the statement is done everywhere and gets the command tag,
or the error of the shard that failed. `SET pgshard.ddl_async = on` makes the
session return at once with a NOTICE that names the migration id.

The planner decides the *scope* from the catalog: a statement on sharded or
reference tables, and every schema/type/sequence/database/role/grant
statement, targets every shard; a statement on unsharded tables the home
shard; `DROP`/`ALTER`/`REINDEX INDEX` and `DROP`/`ALTER VIEW`, whose owning
table the router cannot resolve, target every shard and skip the shards where
the object does not exist. `CREATE INDEX CONCURRENTLY`, `DROP INDEX
CONCURRENTLY` and `REINDEX … CONCURRENTLY` run outside a transaction with
invalid-index detection. `CREATE ROLE … PASSWORD 'plain'` and `ALTER ROLE …
PASSWORD` are rewritten so every shard receives the same SCRAM verifier, which
the applier also mirrors into `pgshard.roles`.

Refused (`0A000`): DDL inside a transaction block (the fan-out cannot be
rolled back with the transaction); rewrite-class `ALTER TABLE` (`ALTER COLUMN
… TYPE`, `SET LOGGED/UNLOGGED`, `SET TABLESPACE`, `ADD COLUMN` with a
volatile `DEFAULT`) until online schema change; dropping, renaming or retyping
the shard key column; renaming or moving a sharded/reference table to another
schema (the catalog declares it by name); one statement touching both sharded
and unsharded tables; `CREATE TABLE AS` over sharded or reference tables.
Sessions on the catalog database run DDL on the catalog directly.

### Reference tables

A reference table exists on every shard, so `INSERT`, `UPDATE` and `DELETE`
on one run **the same statement with the same parameters on every shard of
the set**, inside the session's transaction. Every shard becomes a writing
participant, so `COMMIT` is two-phase (*Transactions* below); outside a
transaction the router opens one around the statement and commits it the
same way before reporting the statement complete. `RETURNING` rows and the
command tag come from the lowest shard (all shards produce the same). If
the statement fails on any shard, the shards where it succeeded are put
into the aborted state as well (an explicit transaction can only be rolled
back; an implicit one is rolled back by the router), so a reference table
never diverges. In `pgshard.transaction_mode = single` a reference write on
a set of two or more shards is refused like any second writable shard.

Because the statement is evaluated independently per shard, the planner
refuses what would produce different rows on different shards:

- volatile function calls anywhere in the statement — `now()`,
  `clock_timestamp()`, `statement_timestamp()`, `transaction_timestamp()`,
  `timeofday()`, `random()`, `random_normal()`, `gen_random_uuid()`,
  `uuidv4()`, `uuidv7()`, `nextval()`, `currval()`, `lastval()`,
  `setval()`, `pg_backend_pid()`, `txid_current()`, `pg_current_xact_id()`,
  `pg_current_wal_lsn()`, `inet_server_addr()`, and `CURRENT_DATE`,
  `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`
  (`0A000` "a write to reference table … cannot call now(): its value would
  differ between shards"; compute the value in the client and pass it as a
  literal or parameter);
- reading a sharded or unsharded table (`INSERT … SELECT`, `UPDATE … FROM`,
  a subquery): those rows live on one shard only. Reading other reference
  tables is fine.

This rule is conservative and only covers what the router can see. Column
defaults are evaluated by each shard: a reference table whose column
default is volatile (`DEFAULT now()`, `DEFAULT gen_random_uuid()`, a
per-shard `serial`) diverges when an `INSERT` omits that column, and the
router cannot detect it because it does not read the shards' catalogs.
Declare defaults on reference tables as constants, or always supply the
column. DDL on reference tables goes through the migration model (see *DDL*).
`SELECT … FOR UPDATE` on a reference table locks the row on the
session's shard only.

### Sequences

A `serial`, `bigserial` or `GENERATED … AS IDENTITY` column on a sharded
table is a per-shard sequence: two shards hand out the same numbers. For
global uniqueness the router owns the allocation instead. A sharded table
declares its sequence columns in `pgshard.tables.sequence_columns` (a text
array, desired state, user-editable), for example

```sql
UPDATE pgshard.tables SET sequence_columns = '{id}'
 WHERE database = 'app' AND schema_name = 'public' AND table_name = 'tickets';
```

`currval()`, `setval()` and `lastval()` are **refused** (`0A000`) over a
global sequence. The router's counter is the sequence; the per-shard
sequence objects the DDL fanned out are not it, so those functions would
read or write an unrelated physical counter and return an answer that looks
ordinary and is about something else. `lastval()` is refused outright — it
names whichever sequence `nextval` last touched, and the router cannot tell
from the statement which that was. Keep the value `INSERT … RETURNING`
gave you.

`nextval()` over a global sequence is answered by the router in exactly two
places: `SELECT nextval('<name>')` as the whole statement, and a value of an
INSERT's registered sequence column (written out, or given as `DEFAULT` or
`nextval()`). **Anywhere else it is refused** (`0A000`) rather than sent to a
shard — `SELECT nextval('g') + 1`, `UPDATE … SET c = nextval('g')`,
`INSERT … SELECT nextval('g')` and `SELECT nextval('g') FROM t` all take the
value from that shard's own sequence object, so two shards would hand out
the same numbers from a sequence declared global. That matters more than the
`currval()` refusal above: `currval` only reads the wrong counter, while
`nextval` allocates from it, and the duplicates arrive later as a primary
key violation or as two rows sharing an id that could not. Select the value
first and bind it, or let the INSERT fill the column.

One gap remains: a `nextval()` on a global sequence written into a VALUES
position of a column that is **not** a registered sequence column is still
forwarded to the shard, because which columns a fill claims is known only
after the relation is resolved and the refusal runs before that.

A sequence that is not registered as global is untouched: it
lives on one shard and means there what it says.

Each column is backed by a row of `pgshard.sequences` named
`<database>.<schema>.<table>.<column>` (`app.public.tickets.id`), created
on first use with `next_value = 1` and `block_size = 1000`; both fields can
be edited (`UPDATE pgshard.sequences SET block_size = 100 WHERE name = …`)
and a sequence can also be declared ahead of time with an `INSERT`. Routers
allocate through `pgshard.allocate_sequence_block(name, n)` (migration
0005): one `UPDATE … RETURNING` that moves `next_value` past a block of `n`
values (the row's `block_size` when `n` is `NULL`) and returns
`[block_start, block_end]`, so concurrent routers always get disjoint
blocks. Every router caches one block per sequence and hands values out
from it in order; a block is never reused, so a router restart or an
aborted `INSERT` leaves gaps, exactly as PostgreSQL sequences do. Values
are increasing within one router; across routers they interleave by block.

On an `INSERT … VALUES` into such a table the router fills every registered
column that is absent from the column list, or given as `DEFAULT` or
`nextval(...)`: the statement is rewritten with one extra bind parameter
per row and column (`INSERT INTO tickets (tenant_id, body) VALUES ($1,
$2)` becomes `… (tenant_id, body, id) VALUES ($1, $2, $3)`) and the
allocated values are bound as `int8`. `RETURNING id` therefore works, the
client's parameter description is unchanged (the injected parameters are
stripped from `ParameterDescription`), and when the sequence column **is
the shard key** the injected value routes the row. An `INSERT` that supplies
the column itself is not touched. Simple-protocol statements are executed
as an unnamed extended-protocol batch for the same reason.

`SELECT nextval('<name>')` with a literal name is answered by the router
from its block, without visiting a shard, when the name is a registered
column sequence (`nextval('tickets.id')`, `nextval('public.tickets.id')`;
an unqualified table is resolved along the search path) or a row of
`pgshard.sequences` known to the snapshot (`nextval('invoice_numbers')`).
The result is one `int8` row. Any other `nextval` (a native sequence such
as `items_id_seq`) goes to the shard as usual — native sequences on sharded
tables are **not** global. In the extended protocol such a statement must
be alone in its batch. Both features need the router's catalog connection
to be allowed to execute `pgshard.allocate_sequence_block` (`pgshard_system`
or `pgshard_admin`); otherwise they are refused with `0A000` "global
sequences are not available".

## Transactions

A transaction starts on the shard of its first real statement (session-local
statements such as `BEGIN` and `SET` are replayed there). Every further shard
it touches gets its own backend, on which the router replays the session's
GUCs and the transaction prelude (`BEGIN`, `SET LOCAL`, ...), and is tracked
as a *participant* with a read/write flag. A read on another shard is always
allowed and needs no prepare. Reads run at READ COMMITTED on independent
snapshots per shard; there is no cross-shard snapshot.

### Modes

`SET pgshard.transaction_mode = twopc | single` (session GUC, default
`twopc`; also replayed with the session's other GUCs):

- **twopc.** The first write to a *second* shard escalates the transaction:
  the router allocates a global identifier
  `pgshard-<router instance>-<session id>-<seq>` and checks, once per shard
  and cached, that the shard's PostgreSQL has `max_prepared_transactions > 0`
  (`0A000` otherwise, naming the shard). Nothing else changes until
  `COMMIT`.
- **single.** The second writable shard is refused with `0A000`; the
  transaction stays open on its shards and can be rolled back or committed.

`SAVEPOINT`/`RELEASE`/`ROLLBACK TO` and `COMMIT`/`ROLLBACK AND CHAIN` are
refused (`0A000`) once a transaction spans several shards. Client-issued
`PREPARE TRANSACTION`, `COMMIT PREPARED` and `ROLLBACK PREPARED` are always
refused: those statements belong to the coordinator.

### Commit

`COMMIT` on a transaction with **at most one writing participant** commits
that shard plainly and rolls back the read-only participants; the decision
log is never touched. This is the common path.

A participant counts as a writer when the planner classified one of its
statements as a write, **or** when its backend assigned a transaction id
anyway: before `COMMIT` the router runs
`SELECT pg_current_xact_id_if_assigned() IS NOT NULL` on every
planner-classified reader (in parallel, one round trip), so a `SELECT` that
called a function which inserted or updated rows is promoted to a writer and
takes part in two-phase commit instead of being rolled back behind a
successful `COMMIT`. In `single` mode such a promotion that produces a
second writer makes `COMMIT` fail with `0A000` and roll back everywhere.

With **two or more writers** the router runs two-phase commit against the
catalog table `pgshard.xact_decisions`:

1. `INSERT` a decision row `state = preparing` with the writer shard ids,
   committed synchronously (`synchronous_commit = on`) so it is durable on
   the catalog before anything is prepared. Failure here rolls the
   transaction back on every shard and reports `08006`.
2. `PREPARE TRANSACTION '<gid>'` on every writer in parallel; readers roll
   back. PostgreSQL itself waits for the shard's synchronous standbys. Any
   failure: `ROLLBACK PREPARED` where prepared, `ROLLBACK` elsewhere, the
   row is marked `abort` and deleted, the client gets the shard's error.
3. **The decision.** `UPDATE ... SET state = 'commit' WHERE gid = $1 AND
   state = 'preparing'`, again synchronously committed. This single-row
   update is the point of no return. If it updates no row the resolver
   already aborted the transaction (a router presumed dead): the router
   rolls the prepared transactions back and reports `40000`. If the update
   *fails* (catalog unreachable) the router does not know whether the
   decision landed: it reports `08007` (`transaction_resolution_unknown`),
   leaves the participants prepared and increments the in-doubt counter;
   the resolver finishes the transaction either way.
4. `COMMIT PREPARED '<gid>'` on every writer. A failure here is logged and
   counted as in-doubt but does **not** fail the client: the decision is
   durable and the resolver commits the remaining participants.
5. When every participant committed the decision row is deleted.

**What success means.** The client sees `COMMIT` only after step 3 is
durable, so a successful `COMMIT` is committed on every shard eventually,
even if the router dies right afterwards. An error from `COMMIT` other than
`08007` means the transaction rolled back everywhere. `ROLLBACK` rolls back
every participant and never touches the decision log.

The `/metrics` endpoint on the health listener exposes
`pgshard_router_in_doubt_transactions_total`.

### Resolver

The controller (`pgshard-controller run --shard-dsn-template ...` or
repeated `--shard-dsn <set>/<id>=<dsn>`) runs a resolution pass every
`--resolve-interval` (default 5s) and on demand through the
`ResolveTransactions` RPC. A pass is idempotent and safe to run beside a
live router because every step is guarded by the row's state:

- a `preparing` row older than 10s belongs to a router that died before
  deciding: it is moved to `abort` (guarded `UPDATE ... WHERE state =
  'preparing'`, so a router that is merely slow and decides commit first
  wins), and its prepared transactions are rolled back;
- a `commit` row: `COMMIT PREPARED` on every participant that still holds
  the gid, then the row is deleted;
- an `abort` row: `ROLLBACK PREPARED` likewise, then the row is deleted;
- every shard's `pg_prepared_xacts` is swept for `pgshard-` gids: one with
  no decision row is an orphan and is rolled back (a row is written before
  any prepare, so a missing row means the transaction was never decided);
  one whose row says `commit` is committed. A commit-decided gid is never
  rolled back. Prepared transactions with other gids are left alone.

The crash matrix in `test/e2e/router` (`TestRouterCrashMatrix`) kills a
router built with `-tags pgshard_crashpoints` (`PGSHARD_TEST_CRASH_POINT`
= `before_prepare`, `after_prepare`, `after_decision`,
`during_commit_prepared`) and checks that the resolver brings both shards to
the recorded decision with no prepared transaction left.

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

- A statement whose shard is blocking, or that came back `55000`, is
  **buffered** if nothing of it has reached the client yet and no
  transaction block is open: it waits until the snapshot shows the shard
  serving again (LISTEN/NOTIFY wakes it; status-only edits are picked up by
  a 200ms poll) or `--buffer-window` (10s) elapses, then runs once more
  against the refreshed endpoint. A window that expires with the shard
  still blocking is `08006`; one that expires with the map still moving, or
  a retry that meets the next flip, is `40001` — nothing ran, so the answer
  is to run it again.
- A pooler that refused the connection outright (gRPC `Unavailable`) while
  the snapshot still shows the shard serving is a transport fault, not a
  failover: the statement is retried once after a new snapshot or
  `--buffer-transport-window` (1s), whichever comes first, and fails with
  `08006` if the pooler still refuses. The refusal takes the full
  `--buffer-window` only when the snapshot has meanwhile fenced the shard.
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

### Write fence

`pgshard.shard_map_generation.write_fence` is the cluster-wide write pause
the controller raises while it takes a certified barrier (see
[backup.md](backup.md#certified-barriers)). The snapshot carries it as
`WriteFence`; while it is set:

- A statement the planner classes as a write (DML, DDL, COPY, locking
  SELECT) that starts a new write, or the first write of an open transaction,
  waits like a buffered statement: until the fence clears (notification or the
  200ms poll) or `--buffer-window` elapses, then is refused with **`57P03`
  (cannot_connect_now) "cluster write pause for a certified restore point"**.
  Nothing of it reaches a shard. Reads pass.
- Later statements of a transaction that already wrote before the fence are
  not held, so open transactions finish; a single-shard COMMIT is never held.
- A two-phase COMMIT waits the same way and, if the window passes, rolls the
  transaction back with `57P03` before any participant prepares, so no
  distributed transaction straddles the barrier's restore points.
- At most `--buffer-cap` statements wait behind the fence cluster-wide; the
  next is refused with `53300`.

The fence is what holds writes the planner can see. Underneath it the
primaries themselves run with `default_transaction_read_only = on` for the
length of the pause, which catches a statement whose write the planner does
not recognise -- a volatile function, `SELECT set_config(...)`. So that a
session cannot lift that guard, the router refuses `SET` (and `SET LOCAL`,
`RESET`, `set_config`) of `transaction_read_only` and
`default_transaction_read_only` with **`42501` (insufficient_privilege)**.

The same override spelled as a transaction mode -- `BEGIN READ WRITE`,
`START TRANSACTION READ WRITE`, `SET TRANSACTION READ WRITE`, `SET SESSION
CHARACTERISTICS AS TRANSACTION READ WRITE` -- is not refused but neutralised:
the mode is dropped and, where nothing else remains, the statement becomes a
`SET ... = DEFAULT` that puts the cluster's own value back. With no pause
running the session is read-write exactly as it asked; during one it stays
paused. Refusing these would break ordinary clients -- pgjdbc sends the
session-characteristics form whenever an application calls
`setReadOnly(false)`, which a connection pool does on every connection it
hands back.

`READ ONLY` in every form is untouched: a session may make itself more
restrictive, never less. The rewritten text is what reaches the shards and
what the router replays onto every other shard session it opens.

`pgshard.table_status.migrating` is the same pause scoped to one table: a
placement workflow raises it while it swaps the table's shadow in. Only
statements whose plan resolved that table wait (the plan lists every
catalog table it touched); after the window they are refused with
**`57P03` "write pause for a table placement change"**.

## Running

```
pgshard-router serve --listen 0.0.0.0:5432 --tls-cert router.crt --tls-key router.key \
  --catalog-dsn 'postgres://pgshard_system@catalog/pgshard' \
  --pooler-tls-cert router-client.crt --pooler-tls-key router-client.key --pooler-tls-ca ca.crt
```

`--tls-cert` **requires** TLS: once a certificate is configured, a client that
never sends `SSLRequest` is refused with `28000` rather than served in the
clear, so a misconfigured or downgraded client cannot send its SCRAM exchange
and its SQL in plaintext to a deployment that asked to be protected.
`--allow-plaintext` lifts that for development. Cancel requests are exempt, as
they are in PostgreSQL: libpq before 17 sends them without negotiating TLS.

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
transactions move on their prelude, escalate to two-phase commit on a second
writable shard (decision recorded before any prepare, decided before any
commit prepared, prepare failure aborts everywhere, single mode and missing
prepared-transaction capacity refuse), one batch is refused for two shards, and each refusal leaves the session usable; scatter
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
DDL/DML, prepared statements, rollback, replay-after-release, the refusal
of session advisory locks, COPY, cancel (`57014`), `28P01`, `3D000`,
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
a direct connection. `TestRouterVStream` and
`TestRouterVStreamFailoverContinuity` start a controller and a router with
`--vstream-listen`/`--controller` on the two-shard stack (poolers with
`--stream-dsn`): the first creates a two-phase stream, inserts through the
router including a 2PC transaction, consumes with a consumer killed and
resumed from its last VGtid (every id exactly once, Prepare/CommitPrepared
on both shards), acks and checks `confirmed_flush_lsn`, then drops the
stream; the second clones shard 1 into a slot-synchronizing standby, waits
for the failover slot to be synced and persistent, promotes it, publishes
the new pooler with a higher epoch and shows the stream continuing on the
promoted standby with no gap. See [streams.md](streams.md).
