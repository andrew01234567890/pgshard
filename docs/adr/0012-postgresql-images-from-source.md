# 12. Building PostgreSQL images from source, per major, with a patch series

Status: accepted

## Context

pgshard targets PostgreSQL 18 and 19. Official images exist for both, and
using them would remove a build from the pipeline.

## Decision

Build `pgshard-postgres:<major>` from SHA-pinned source tarballs, with a
per-major patch directory (`postgres/patches/<major>/series`, applied in
order) and pgBackRest built from source alongside. The patch series exists
from the first milestone and is **empty**.

Three things need to be in one image and one filesystem: the server,
pgBackRest at a version that knows the server's catalogue version, and
`pgshard-agent`, which is PID 1 and runs both. Composing that from an
official image is a derived image anyway; building from source makes the
version relationship explicit instead of implied.

The empty patch series is the point of the decision. The charter allows
patching PostgreSQL where core cannot do the job, and a patching mechanism
introduced at the moment it is first needed is introduced under pressure,
in the same change as the patch it carries. Building it first, and proving
it by carrying nothing, means a future patch is an ordinary pull request
adding a file — reviewable on its own merits, with the question "should
this be upstream instead?" asked before the mechanism is a fait accompli.

Research for the MVP found nothing on 18 or 19 that requires a core patch:
DDL replication is ours because we drive DDL, sequences are catalog-owned,
failover slots are configuration, hashes in row filters are built-ins,
protocol 3.2 is the router's, and the online-DDL primitives exist
(PostgreSQL 18's `NOT VALID` `NOT NULL`, 19's `REPACK CONCURRENTLY`).

## Consequences

- The image pipeline is a build, with the cache and supply-chain surface a
  build has. Sources are SHA-pinned and the build is content-hashed.
- Tracking a minor release is a tarball bump and a rebuild, not a base
  image tag change, so it is visible in the diff.
- PostgreSQL 19 is beta until it is not. The pin is what makes moving to
  RC and GA a single reviewable change, and pgBackRest's own support for 19
  is tracked with it.
