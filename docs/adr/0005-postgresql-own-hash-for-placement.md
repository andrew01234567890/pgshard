# 5. Placing rows with PostgreSQL's own extended hash

Status: accepted

## Context

A sharded table maps each row to a shard by hashing its shard key into an
`int8` keyspace, which shards own as contiguous `[start, end)` ranges. The
router computes that hash in Go to route a statement. The question is which
hash function.

A custom hash — anything we write — is the obvious choice for a Go router.

## Decision

Use PostgreSQL's own extended hash functions with the hash-partition seed:
`hashint8extended`, `hashtextextended`, `uuid_hash_extended`, and port them
to Go bit-exactly.

The reason is resharding. Data moves between shard groups over native
logical replication, and the only way native replication moves *part* of a
table is a publication row filter. A row filter may call only `IMMUTABLE`
built-ins, which a hash of ours is not. With PostgreSQL's own hash the
filter is legal and the server evaluates it:

```sql
CREATE PUBLICATION ... FOR TABLE t
  WHERE (hashint8extended(tenant_id, 8816678312871386365) BETWEEN ... AND ...)
```

A custom hash would mean moving data with an applier we write, for every
reshard, forever, instead of with the server's own replication.

The Go port is differential-tested against a live server rather than
trusted. Goldens captured on PostgreSQL 18:

```
hashint8extended(42, 8816678312871386365) =  7363975540656877951
hashint8extended(42, 0)                   =  8010225493015854792
hashint8extended(-1, 0)                   = -1888257769727981238
```

## Consequences

- A shard key must be a type whose PostgreSQL hash is defined over a
  representation the router also has: integers hashed as `int8`, character
  types as `text`, `uuid` over its raw bytes. Other types are refused with
  a distinct SQLSTATE rather than routed by guesswork.
- Blank-padded `character(n)` keys are refused today: their equality
  ignores trailing spaces and the `::text` cast in the row filter strips
  them, so the filter would hash a trimmed value while the router hashes
  the client's bytes. Supporting them needs the column type in the routing
  snapshot so the router can normalise the same way.
- The Go port is a compatibility surface with PostgreSQL. A change to
  `hashfunc.c` in a future major is a change to where rows live, and the
  differential test is what will catch it.
