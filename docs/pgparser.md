# pgparser: the router's SQL parser

`internal/pgparser` parses SQL with the *real* PostgreSQL grammar, not an
approximation. It binds [libpg_query](https://github.com/pganalyze/libpg_query),
which extracts the parser from the PostgreSQL source tree, and pins one
libpg_query release per PostgreSQL major.

## Layout

| Path | Role |
| --- | --- |
| `third_party/libpg_query/18/` | Vendored libpg_query 18 C sources plus a minimal cgo package (`libpgquery18`) that exposes the raw entry points. `VERSION` records the upstream tag and commit; `LICENSE`, `COPYRIGHT.postgresql` and `NOTICE` carry attribution. |
| `internal/pgparser/pg18/` | Typed PostgreSQL 18 API: `Parse`, `Scan`, `Deparse`, `Fingerprint`, `Normalize`. |
| `internal/pgparser/pg18/pgquerypb/` | Go protobuf types generated from libpg_query's `pg_query.proto` (`make pgparser-proto`, `buf.gen.pgparser.yaml`). |
| `internal/pgparser/` | Version-neutral facade with limits, an LRU cache and a metrics hook. `engine_pg18.go` selects the grammar; a future `engine_pg19.go` behind `//go:build pg19` adds PostgreSQL 19 without touching callers. |
| `hack/pgparser/sync.sh` | Re-vendors libpg_query at a pinned tag **and commit** (`make pgparser-sync`). |
| `hack/pgparser/verify.sh` | Checks the vendored trees against the checksums sync recorded (`make vendor-check`, part of `make verify` and of CI). |

The cgo package lives next to the C sources because cgo only compiles `.c`
files found in the package directory; keeping a second copy under
`internal/` would double a 14 MB tree.

## API

```go
p := pgparser.New(pgparser.Options{}) // 1 MiB SQL, 1000 statements, 4096-entry / 64 MiB cache
res, err := p.Parse(ctx, "SELECT 1; INSERT INTO t VALUES (1)")
res.Kinds()          // []string{"SelectStmt", "InsertStmt"}
res.Stmts[0].RawStmt // *pgquerypb.RawStmt (immutable, shared with the cache)
pgparser.Deparse(res.Tree)
pgparser.Fingerprint(sql)
pgparser.Normalize(sql)
```

Errors are `*pgparser.Error` with `SQLState` (`42601` for grammar errors,
`54000` for limit violations) and a 1-based `Position`. Unsupported syntax is
never swallowed: it surfaces as a `42601` error exactly as the server would
report it. `Parse` honours `ctx` (the C call runs on its own goroutine, so a
cancelled context returns immediately).

`pgshard-router parse "SQL"` is a hidden diagnostic subcommand that prints the
grammar version, fingerprint and statement kinds.

## Building

The parser is cgo. `make verify` and the router image need:

* `CGO_ENABLED=1` (Go's default when a C compiler is present),
* `gcc` (or clang) and glibc headers.

Static router binaries are optional:
`go build -ldflags '-extldflags -static' ./cmd/pgshard-router`. glibc's
static linking warnings do not apply because libpg_query uses no NSS/dlopen
functions. Cross-compiling requires a target C toolchain (`CC=...`).

`golangci-lint` skips `pgquerypb` because the files carry the
`Code generated ... DO NOT EDIT` header.

## Upgrading libpg_query

1. Bump `LIBPG_QUERY_18_TAG` **and `LIBPG_QUERY_18_COMMIT`** in the
   `Makefile` (or add a `19` pair). Read the commit from the upstream tag
   yourself — `git ls-remote https://github.com/pganalyze/libpg_query
   refs/tags/<tag>` — and record it in the same commit as the vendored
   sources, so the review sees both.
2. `make pgparser-sync` — verifies the tag resolves to that commit before
   copying anything, re-vendors the sources, writes `SHA256SUMS` and
   regenerates `pgquerypb`. A tag that has moved fails here and copies
   nothing.
3. `go test ./internal/pgparser/...` — the goldens pin deparse output and
   fingerprints; changes there are deliberate upstream drift and must be
   reviewed, not blindly updated.

## Why the commit and the checksums

A tag is a name upstream can move. The vendored C is compiled into the
router and parses every statement a client sends, and nothing else in the
repository would notice a change to it: the secret scan looks for
credentials, `govulncheck` reads Go, and a diff of 14 MiB of generated
parser is not something a reviewer reads. So `sync.sh` refuses a tag that
does not resolve to the pinned commit, and `SHA256SUMS` — written at sync
time, checked by `make vendor-check` — makes an edit made afterwards
visible. The secret scan covers the directory too: its exception is one
upstream header whose xxHash constants trip the generic rule, not the whole
tree.

## Testing

* Goldens for PostgreSQL 18-only syntax (`RETURNING OLD/NEW`, `NOT NULL ...
  NOT VALID`, virtual generated columns, `NOT ENFORCED` foreign keys, `COPY ...
  ON_ERROR / REJECT_LIMIT`), classic DML/DDL, deparse round trips, statement
  kinds and fingerprints.
* Error position, limit and cache tests.
* `TestDifferentialAgainstPostgres18` starts
  `ghcr.io/andrew01234567890/pgshard-postgres:18` in Docker and checks that the
  server's syntax verdict (SQLSTATE `42601` vs anything else) matches ours for
  a mixed corpus. It skips only when Docker or the image is unavailable.
