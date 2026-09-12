# 16. Four properties that are not to be traded away

Status: accepted

## Context

Reading PlanetScale's Neki documentation end to end (Platform Preview,
read 2026-09-11) put a number on where pgshard stands against the most
credible sharded-PostgreSQL competitor. In most places they are ahead of
us on query power: they support multi-shard `UNION` and `avg()` before we
did, they route from secondary indexes, and they target replicas by
recency and locality. Those are gaps we are closing, and the tickets
exist.

In four places pgshard is ahead, and each of those four is a property that
is cheap to give away by accident while chasing one of the gaps. An
optimisation that drops two-phase commit from a reference write, a faster
hash, a simpler online-DDL swap: each is a reasonable-sounding local
change that costs a guarantee nobody wrote down as a guarantee.

So they are written down here, with the contrast that makes them
deliberate.

## Decision

These four are load-bearing. A change that removes one is a change to what
pgshard is, not a local optimisation, and needs its own ADR superseding
this one.

### 1. Cross-shard commit is atomic

Neki says plainly that theirs is not: *"Neki commits each shard
separately. It does not use two-phase commit to make every shard succeed
or fail together. If one shard fails after another commits, the statement
can leave different outcomes on the participating shards."* Their
limitations page repeats it: *"Atomic distributed transaction mode is not
supported."*

pgshard makes two-phase commit first-class: a durable decision row in the
catalog before any participant is asked to prepare, `PREPARE TRANSACTION`
on each, and a resolver (`internal/controller/resolver.go`) that drives
every in-doubt GID from the decision log and each group's
`pg_prepared_xacts`. An unknown GID with no record rolls back; a known
`COMMIT` commits; never the reverse.

This is the single largest correctness advantage we have, and the one most
likely to be eroded in the name of latency — 2PC costs a round trip and a
`fsync` on every participant, and every prepared transaction pins WAL and
blocks logical slot creation until it resolves. Those costs are the price
of the property, not a defect to optimise away.

### 2. A reference-table write is atomic across every shard

Neki: *"The router coordinates one backend transaction per participating
shard. Those transactions commit independently, so a commit can succeed on
some shards and fail on another."*

pgshard runs a reference write as one two-phase transaction across every
shard (`isReferenceWrite` and `referenceWrite`, `internal/router/reference.go`),
so the copies cannot diverge. A reference table exists precisely so that a
join can be answered locally on any shard; copies that disagree make every
such join return a different answer depending on where it ran, and nothing
in the system would report it.

The tempting optimisation here is obvious — a reference write touches
every shard, so it is the most expensive write in the system, and dropping
2PC would make it much cheaper. That trade is the whole property.

### 3. The shard hash is PostgreSQL's own

Neki hashes with xxhash. pgshard uses PostgreSQL's `hash*extended` family
with the hash-partition seed (`internal/placement/hash.go`,
`PartitionSeed = 8816678312871386365`, goldens in `hash_test.go` checked
against a running server in `pg_test.go`).

This is not a detail and it is not about hash quality — xxhash is a better
hash. It is that the shard key has to be expressible in a `PUBLICATION`
row filter, because that is what lets resharding move exactly one key
range using **native logical replication** instead of a bespoke copier
(ADR 8). A row filter may only call `IMMUTABLE` built-ins, which
`hashint8extended` and its siblings are and a Go xxhash is not.

**Changing the hash function is changing the resharding architecture.**
Anything that proposes a faster hash has to say what replaces row-filtered
publications.

### 4. Online DDL preserves the table's OID

Neki's online DDL is the shadow-table swap: *"swaps the original and
shadow tables in one Postgres transaction"*. That changes which OID the
table's name resolves to.

pgshard rejected swap-as-default for that reason (ADR 9): publications
name relations by OID, so a swapped table silently leaves its publication
— and publications are the mechanism resharding and change streams both
depend on. The measurement is recorded on PGS-782: after a swap, foreign
keys, views and the publication follow the abandoned table, with nothing
reporting it.

## A draw worth recording

Multi-shard DDL is not atomic in either system. Neki: *"Multi-shard DDL is
not atomic across the deployment. A failure can leave the change applied
on some shards but not others."* pgshard's applier has the same property
and reports it as `DEGRADED` with the failing shard named.

Two projects reaching the same conclusion independently is mild evidence
it is the right one — atomic DDL across shards would need the DDL to ride
the 2PC path, and a prepared DDL holds an `ACCESS EXCLUSIVE` lock with no
backend behind it until someone resolves it.

## Consequences

- Each of the four has a cost, and the cost is not a bug report. 2PC's
  round trip, the reference write's fan-out, PostgreSQL's slower hash, and
  the in-place rewrite's working column and backfill are all bought
  deliberately.
- A benchmark that shows one of them is expensive is evidence about the
  price, not an argument about the purchase.
- Where we are behind Neki — multi-shard set operations, secondary-index
  routing, replica targeting, `COPY FROM STDIN` into a sharded table — the
  gap is tracked and is not covered by this ADR.
