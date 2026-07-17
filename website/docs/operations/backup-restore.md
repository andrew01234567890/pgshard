---
title: Backup, restore, and database recovery
description: Database-targeted coordinated backups, exact-topology restores, and online return moves.
---

# Backup, restore, and database recovery

:::info Preflight implemented; execution remains a design contract
`PgShardRestore` now verifies an immutable Ed25519 key Secret, a signed
PostgreSQL 18 backup-manifest projection, canonical full-keyspace ranges, and
any caller-supplied topology expectation without creating restore targets. It
does not yet read authoritative destination catalog state, so it remains
`Pending` rather than claiming that the destination is absent or compatible.
Coordinated backup publication, physical materialization, logical import,
activation, and database moves are not implemented; see
[implementation status](../project/status.md).
:::

Milestone 1 backups target one logical database. That database may have a shard
count and placement entirely different from every other database in the same
fleet. pgBackRest still operates at physical PostgreSQL-cell granularity: the
backup set references one physical backup for every cell containing a shard of
the selected database.

The preferred source is the healthiest, most caught-up secondary. pgBackRest's
standby backup still coordinates with the primary. If no safe secondary exists,
pgshard falls back to the primary and records that decision in status, events,
metrics, and the immutable manifest.

## Coordinated database backup set

```mermaid
flowchart LR
  T[Select database and active topology] --> B[Back up every referenced cell concurrently]
  B --> V[Verify base backups]
  V --> G[Database mutation barrier]
  G --> D[Drain that database's distributed transactions]
  D --> C[Capture signed database catalog projection]
  C --> L[Record restore LSN and timeline per database shard]
  L --> W[Verify required WAL and dependency closure]
  W --> P[Persist cross-stanza retention pins]
  P --> M[Publish immutable database manifest]
```

The monotonically fenced operation lives in `shardschema`. Poolers stop new
writes only for the selected logical database and drain its 2PC transactions.
Its DDL, role/grant, reshard, restore, move, and topology reconcilers stop
starting or activating mutations at the same barrier. Other databases may keep
serving, including databases colocated in the same physical cells. A safety
failover of any referenced cell invalidates the attempt and forces a fresh
backup set rather than silently changing timeline.

The drain is complete only after every transaction admitted before the barrier
has committed or aborted, every durable 2PC decision has been applied on every
immutable participant database shard, no matching GID remains in
`pg_prepared_xacts`, and the coordinator-recovery backlog for the database is
empty. The coordinator is the lowest-ID participating database shard; its row
is stored in that shard's current physical cell. A merely durable `COMMIT`
decision with participants still prepared is not a backup boundary.

While the barrier is held, the controller records the database's frozen
routing, schema, authorization, topology, placement, and fencing epochs before
capturing each shard's restore LSN and timeline. It also reads a repeatable-read,
database-scoped catalog projection from live `shardschema` on `cell-0000`, even
when the selected database has no data shard in that cell. The canonical
projection and final manifest are content-hashed and signed by the fleet backup
key; a restore must be configured to trust that key. A physical base backup of
the catalog cell is therefore not silently implied by every database backup.

The manifest includes:

- source fleet and logical-database UUID, source name, PostgreSQL major, schema
  epoch, role/grant epoch, and backup barrier identity;
- a topology fingerprint covering hash algorithm/version and seed, shard count,
  ordered shard ordinals, and every range boundary;
- the physical cell and database OID for each database shard, its source restore
  database-shard UUID and restore incarnation as provenance, PostgreSQL system
  identifier, timeline, and selected backup source role;
- the signed database-scoped catalog projection and its object-store version;
- every pgBackRest cell stanza, repository and backup ID, the complete
  full/differential/incremental reference closure, checksums, and the required
  WAL interval.

A backup is usable only when every referenced cell backup, catalog projection,
WAL object, retention pin, and the final signed manifest exist. Retention treats
the complete set as one unit.

### Cross-stanza retention

[pgBackRest retention](https://pgbackrest.org/command.html) is stanza-local,
while one database backup set can depend on several stanzas. pgshard therefore
disables pgBackRest's automatic expire after backup and is the only component
allowed to invoke repository expiry. A durable pin graph in `shardschema`,
mirrored by the signed manifest in MinIO, maps each published backup set to every
referenced backup chain and WAL interval. Publication occurs only after all pins
are durable.

Deletion is two phase. The controller first tombstones the database backup set,
then recomputes reference counts and safe per-stanza backup and archive floors
from every non-released manifest. Only unreferenced dependency closures may be
offered to pgBackRest expiry. The controller re-reads pgBackRest metadata and
MinIO object versions before releasing the tombstone. A crash at any point is
reconciled from the pin graph. Missing metadata, an unknown dependency, a
concurrent backup, or any proposed deletion intersecting a live pin stops
reclamation and raises an alert; it never guesses. Direct user-initiated
`expire` against an operator-managed repository is unsupported.

:::caution Physical backup scope
If `A` and `B` share a PostgreSQL cell, a physical backup needed by `A`
necessarily contains `B`'s bytes too. Repository encryption and authorization
remain cell-scoped. Restore quarantines those bytes and exposes only the
requested logical database. Dedicated placement avoids this coupling.
:::

:::caution Recovery point boundary
Milestone 1 restores coordinated backup-set points. Arbitrary wall-clock
cross-shard PITR is not supported because a distributed commit can straddle a
timestamp.
:::

## Database-targeted restore

A restore names one source database from the manifest and one destination
database name. For example, source `A` may restore as `B` while the current `A`
continues serving. If `A` does not exist at restore time, the destination may be
`A`. Every destination receives a fresh logical-database UUID, fresh
database-shard UUIDs, and fresh restore incarnations; the source database and
shard UUIDs remain immutable provenance and are never rebound to the new
destination.

The current preflight API records the signed projection directly. Repository
ingestion will replace that transport without changing the signed manifest or
topology contract:

```yaml
apiVersion: pgshard.io/v1alpha1
kind: PgShardRestore
metadata:
  name: restore-a-as-b
spec:
  manifest:
    manifestVersion: 1
    backupSetID: backup-a-2026-07-16
    sourceDatabase: A
    topology:
      postgresqlMajor: "18"
      hashVersion: 1
      hashSeed: "7"
      shardCount: 1
      shards:
        - ordinal: 0
          start: "0"
          end: "18446744073709551616"
  manifestSignature: <canonical-base64-ed25519-signature>
  verificationKeySecretRef:
    name: fleet-backup-verification-key
  destinationDatabase: B
  destinationTopology: # optional caller expectation; omission proves nothing
    postgresqlMajor: "18"
    hashVersion: 1
    hashSeed: "7"
    shardCount: 1
    shards:
      - ordinal: 0
        start: "0"
        end: "18446744073709551616"
```

The verification Secret must be immutable, type `Opaque`, and contain only
`ed25519.pub` with the raw 32-byte public key. The signature is canonical RFC
4648 base64 for the Ed25519 signature over the version-1 binary manifest
encoding: fixed field order, big-endian integers, and big-endian
u32-length-prefixed string bytes. Go and Rust golden vectors pin the same
payload; the Go vector also pins the SHA-256 digest, public key, and Ed25519
signature. The first observed Secret UID is pinned in status and cannot be
rebound by deleting and recreating the Secret; a replacement requires a new
`PgShardRestore`. Manifest/topology SHA-256 digests are also checkpointed, and
restore execution must revalidate them.

The destination name must either be absent or reserved by an empty, non-serving
`PgShardDatabase`. Restore does not overwrite an active database. Replacing an
existing name is a later, explicit online database move.

Milestone 1 restore materialization accepts `Dedicated` placement only. A
restore request using `SharedCell`, `SharedNode`, or `Explicit` returns the
permanent `RestorePlacementUnsupported` reason before target mutation. Once the
exact dedicated restore is serving, the separate online move engine may place
it onto shared cells or shared Nodes. This keeps physical staging bytes out of
already-serving PostgreSQL cells and gives the first implementation one
well-defined logical import boundary.

### Exact-topology preflight

Restore never performs implicit resharding. Before creating Secrets, PVCs,
PostgreSQL cells, Jobs, or repository writes, the completed controller must
resolve the destination through authoritative `shardschema` state and compare
the manifest topology fingerprint with that result:

- An absent destination is created with the exact fingerprint from the backup.
- A pre-created destination must match PostgreSQL major, hash
  algorithm/version and seed, shard count, ordered shard ordinals, and every
  range boundary.
- Any mismatch returns the permanent error code `RestoreTopologyMismatch`. An
  authoritative-destination mismatch checkpoints the manifest digest plus the
  manifest and authoritative destination topology fingerprints in status,
  identifies each differing field, and leaves the restore `Ready=False` with
  the same reason. A defensively detected caller-request mismatch leaves the
  authoritative destination fingerprint empty. There is no target mutation. A
  five-shard backup cannot restore into a three-shard destination, even if the
  destination is empty.

CRD validation rejects explicit PostgreSQL/hash/count differences, and the
fail-closed validating webhook compares every ordered ordinal and boundary.
Either admission rejection returns Kubernetes API `Invalid` with
`RestoreTopologyMismatch`; no `PgShardRestore` exists, so there is no status or
fingerprint record. The webhook-disabled development overlay retains the cheap
CRD checks, while the controller defensively repeats the complete comparison
for any persisted request after it verifies the signature.
It currently cannot prove destination absence or load a live destination
topology, so a valid request remains `Pending` with
`DestinationTopologyResolverUnavailable` and `Ready=False`. Once the catalog
resolver is implemented, only proven absence or an exact live topology may set
`PreflightPassed=True`; restore execution will still remain unavailable until
the materialization slice exists.

Physical cell IDs and Kubernetes Node names are placement rather than logical
topology. Future execution will initially materialize the exact five database
shards into five replacement dedicated cells. They will use fresh
database-shard UUIDs while retaining the backup's five ordinals and ranges
beneath the destination's fresh database UUID. A later online move may
consolidate them onto compatible shared cells without changing what restore
accepted.

The implemented unit oracle intercepts controller writes and permits only the
`PgShardRestore` status update. The live Kubernetes test also checks selected
namespaced resource kinds and a sentinel ConfigMap. The full Milestone 1 oracle
must additionally snapshot operator-owned cluster-scoped objects, live catalog
rows and epochs, PV data identity, pgBackRest metadata, and MinIO object
versions. Repository reads may be allowed; creating credentials, reservations,
Jobs, cells, catalog rows, or object-store writes must not be.

### Restore execution

1. Verify the manifest signature, catalog projection, exact destination
   topology, PostgreSQL 18 compatibility, backup dependency closure, checksums,
   and required WAL before target mutation.
2. Create a private, non-serving staging cluster with the exact source database
   shard configuration and one restored cell for every referenced physical
   backup.
3. Restore every physical data cell with pgBackRest to its recorded target LSN
   and timeline with archiving disabled. No staging Service, pooler endpoint,
   or application credential is published. The live fleet's `shardschema` is
   never overwritten by a restored physical catalog.
4. Cross-check the signed catalog projection against the recovered system
   identifiers, database OIDs, timelines, shard ordinals, ranges, and row-level
   validation queries.
5. Create one empty, non-serving dedicated PostgreSQL 18 destination cell per
   database shard. Physical database names are UUID-derived and do not reuse
   the source name or OID.
6. Run the PostgreSQL 18 logical importer from each quarantined source database
   to its corresponding destination. The importer uses `pg_dump`/`pg_restore`
   TOC data plus bounded parallel `COPY`, but accepts only the documented
   extension and object allowlist. It copies schemas, tables and partitions,
   sequence value plus `is_called`, large objects, comments, declarative
   ownership/ACL mappings, and only database or role settings present in a
   versioned inert-GUC allowlist. Unknown settings fail preflight. Preload or
   callback libraries, archive/recovery commands, `role`, session authorization
   or replication role, search/library/file paths, access methods, and default
   or temporary tablespaces are always denied before destination mutation.
   Tablespaces, foreign servers, unapproved extensions, cluster-global objects,
   and executable restore hooks also fail closed. The importer generates its
   own canonical DDL; it never replays raw `ALTER DATABASE` or `ALTER ROLE`
   settings and never asks pgBackRest to merge one physical cluster into
   another.
7. Persist per-object and per-chunk checksums and resumable copy receipts, then
   validate schema, roles, grants, row counts, sequences, large objects, and
   complete range coverage on all destination shards.
8. In one live-`shardschema` transaction, allocate the destination's fresh
   logical-database UUID, fresh database-shard UUIDs and restore incarnations,
   initial topology and placement generation, and new consumer checkpoint
   generations. Source UUIDs remain immutable provenance; old resume tokens
   cannot authorize the copy.
9. Arm the destination generation fences, publish the destination catalog
   generation, wait for every pooler to acknowledge it, and only then release
   serving. Destroy quarantined staging and all unselected physical bytes after
   the durable cleanup receipt.

Interruption is resumable from durable per-cell restore and per-database copy
receipts. A retry with the same operation ID and canonical request resumes; a
changed destination, topology, placement, backup ID, or source database is a
conflict rather than a new interpretation of the old receipt.

## Returning `B` to `A` online

After validation, `B` can move back to the user-facing name `A` using the
[online database move](./online-resharding.md#online-database-move). The move
keeps the source generation serving during snapshot and `pgoutput` catch-up.
At the final barrier, poolers buffer eligible queries, drain old-generation
transactions, fence the old generation in every source PostgreSQL cell, apply
the final LSN vector, publish the new name/topology/placement generation in
`shardschema`, arm it in every destination cell, and release buffered traffic
only after all poolers and destination cells acknowledge the same generation.
Existing backend sessions remain pinned to their original generation; after a
fence they receive the documented retry/disconnect outcome and must reconnect,
never silently continue on another history.

If `A` still exists, replacement must be explicit. The old `A` generation is
read-fenced and quarantined rather than destroyed at cutover. Selecting an
older restored history intentionally does not merge post-backup writes from the
replaced generation; the operation records that discard boundary for audit.

A topology change is allowed in this separate online move. It is never folded
into restore. For example, restore five-shard `A` as exact five-shard `B`, then
move/reshard `B` into a new three-shard `A` generation while traffic continues.

## Required end-to-end coverage

The Milestone 1 KIND and Docker Desktop suites must use MinIO and cover:

- standby backup selection, explicit primary fallback, interrupted uploads,
  missing or corrupt objects, incomplete WAL, and retry receipts;
- `A` with five shards restored as absent `B` with five shards on dedicated
  cells, followed by separate online moves onto shared cells and shared Nodes;
- `A` restored as `A` only when that name is absent;
- rejection of five-to-three restore before any non-status mutation, including
  count-equal but range/hash-seed mismatches, with the stable
  `RestoreTopologyMismatch` error and condition reason and the complete
  Kubernetes/catalog/PV/pgBackRest/MinIO no-mutation oracle;
- a physical backup from a cell shared by `A` and `B`, proving only `A` is
  registered or queryable after restore;
- continuous load while restored `B` moves back to `A`, including process,
  primary, pooler, and operator failures before and after the atomic cutover;
- a separate online move that changes shard count and placement after exact
  restore, with old cells quarantined and no failed client request.
- retention-pin races, concurrent backup and expiry, controller crashes at
  every tombstone/refcount/expiry boundary, and proof that no retained backup
  or required WAL interval is deleted.

Unit tests and the Kubernetes manager test now cover signed request verification
up to the resolver-unavailable `Pending` state, five-to-three admission
rejection, range/hash mismatch typing, and a no-target-mutation oracle. They do
not prove resolver-backed exact preflight success. MinIO, pgBackRest,
materialization, activation, mobility, and failure-injection coverage remain to
be implemented.
