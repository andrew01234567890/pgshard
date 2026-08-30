# Testing: what each gate covers

A pull request is gated on several checks, and only some of them run from a
plain `go test`. This page says which is which, so a green local run means
what you think it means.

## The tiers

| Tier | Command | Needs | What it covers |
| --- | --- | --- | --- |
| Fast | `make verify` | Go, a C compiler (the parser is cgo), `golangci-lint`, `buf` | `gofmt`, `go vet`, lint, proto lint, `go test -race ./...`, and building every command |
| Kubernetes | `make envtest` | envtest assets, downloaded by the target itself | CRD validation, the operator and the admin server against a real API server |
| Generated code | `make proto-drift` | `buf`, `protoc-gen-go`, `protoc-gen-go-grpc` | that `internal/gen` matches `proto/` |
| Vulnerabilities | `make govulncheck` | `govulncheck` | known advisories in the dependency graph |
| Workflows | `make actionlint` | `actionlint`, python3 with PyYAML | workflow syntax, this repository's action policy, and the CI shell helpers' own tests |
| All of the above | `make gates` | everything above | what CI gates on, except the secret scan |
| End to end | `make e2e` | a kind cluster and the images | the suites under `test/e2e` |

The secret scan runs only in CI: it reads the repository history, which a
working tree does not have.

`make tools` installs the pinned versions of every tool above into
`$(go env GOPATH)/bin`. The pins live in `hack/tools/versions.env`, which CI
reads too, so a local gate and a CI gate cannot disagree about the same code.

## A green run that ran nothing

The PostgreSQL-backed tests need a Docker daemon and the images, and the
envtest suites need a Kubernetes control plane. When those are missing the
tests **skip**, so that a contributor without them still gets a usable
`go test ./...` — and a run that skipped half the suite still says `ok`.

Two environment variables turn those skips into failures, and CI sets both:

```sh
PGSHARD_REQUIRE_DOCKER=1 make test     # container-backed tests must run
PGSHARD_REQUIRE_ENVTEST=1 make envtest # the API control plane must be there
```

Run the first before concluding that a change to anything touching
PostgreSQL is tested.

## Which tier does my change need

- **Anything at all**: `make verify`. It is the gate CONTRIBUTING asks for
  before a pull request, and it prints what it did not cover.
- **CRDs, the operator, the admin server** (`api/`, `internal/operator`,
  `internal/admin`): add `make envtest`. The fast tier skips these suites
  entirely without the assets.
- **`proto/`**: add `make proto-drift`. Generated code is committed, and CI
  fails on drift rather than regenerating it for you.
- **Dependencies** (`go.mod`): add `make govulncheck`.
- **`.github/workflows`**: add `make actionlint`.
- **Router, pooler, controller, agent, catalog**: run the fast tier with
  `PGSHARD_REQUIRE_DOCKER=1`, so the PostgreSQL-backed tests actually run.
- **Behaviour a user can see end to end**: `make e2e` against a kind
  cluster, or rely on the e2e matrix CI runs on the pull request.

Running `make gates` covers every row but the last two.
