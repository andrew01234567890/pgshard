# Continuous integration

Workflows live under `.github/workflows`; `hack/check-actions.sh` enforces the
policy (SHA-pinned actions with version comments, `docker://` steps pinned by
digest, no top-level `contents: write`, no `pull_request_target`, no untrusted
event fields interpolated into `run:` or `with:`). `hack/test-check-actions.sh`
runs the checker against the fixtures under `hack/testdata`.

| Workflow | What it does |
| --- | --- |
| `ci.yml` | gofmt, vet, golangci-lint, `go test -race`, build; `buf lint` plus a drift check that fails when `make proto` changes `internal/gen`; govulncheck; gitleaks; actionlint and the policy checker; PR title check |
| `images.yml` | builds the PostgreSQL 18 and 19 images with `docker buildx bake` and pushes them to GHCR on `main` |
| `e2e-kind.yml` | kind-based smoke, operator, backup and reshard suites for both majors, plus upgrade, reshard-split and reshard-merge on pg18 |
| `perf.yml` | benchstat comparison against the base branch; only benchmarks tagged `Gate` can fail a PR (see `hack/perf/gate.sh`) |
| `repeat.yml` | manual: repeats one Go package up to 50 times and reports the pass rate |
| `repeat-e2e.yml` | manual: repeats one kind e2e suite up to 12 times, against an optional `ref`, and reports the pass rate |
| `chaos.yml` | Chaos Mesh experiments (`test/chaos`) |
| `dependency-review.yml`, `dependabot-automerge.yml` | dependency hygiene |

## Container images on GHCR (one-time bootstrap)

`images.yml` pushes `ghcr.io/andrew01234567890/pgshard-postgres:{18,18.x,19,19betaN}`
from `main` only: `workflow_dispatch` can build any branch but cannot publish
one. The `19` tag tracks whatever PostgreSQL 19 the build uses, which today is
**Beta 3** -- a major-number tag is a channel, not a promise of a release
using `GITHUB_TOKEN` with `packages: write`. For a user-owned namespace the very
first push is rejected with `denied: permission_denied: write_package` until the
package exists and is linked to the repository. Bootstrap once, by hand:

1. `docker login ghcr.io -u <github-user>` with a personal access token that has
   `write:packages` (do not store the token anywhere in the repository).
2. From a checkout of `main`: `docker buildx bake postgres --push`
   (`docker-bake.hcl` tags for `ghcr.io/andrew01234567890`).
3. GitHub, Packages, `pgshard-postgres`, Package settings: link the `pgshard`
   repository with **Write** access and set the package visibility to public.
4. Re-run `images.yml` on `main`; subsequent pushes use `GITHUB_TOKEN`.

Once the package is public, `docker pull ghcr.io/andrew01234567890/pgshard-postgres:18`
works anonymously. The catalog integration test then runs against the project
images for both majors; set `PGSHARD_REQUIRE_PROJECT_IMAGES=1` (planned for
`ci.yml` after the bootstrap) to fail instead of falling back to Docker Hub
`postgres:*` tags when a project image is missing.

## Measuring a flake

A test is not flaky until a rate says so, and not fixed until the same run
says so again: one green run proves nothing about a test that fails one time
in five. `repeat.yml` repeats a Go package (up to 50 runs); `repeat-e2e.yml`
repeats one kind e2e suite (up to 12, because each run builds images and
stands up its own cluster). `repeat-e2e.yml` takes a `ref`, so a suspect
branch can be compared against `main` on the same suite.

Twelve runs bounds a failure rate at roughly one in twelve. It cannot see a
rarer flake, so a green twelve is not evidence of zero — the run summary says
so, and neither number should be reported as more than it is.

## Required checks

What `main` requires **today** is a list of job names:

    Go build and test, PR title, Secret scan, Workflow lint and policy,
    govulncheck, dependency-review,
    e2e (pg18|pg19, smoke), e2e (pg18|pg19, operator), e2e (pg18|pg19, backup)

Two aggregate jobs exist to replace that list: **CI gate** fails unless every
job in `ci.yml` succeeded, and **e2e gate** fails unless every cell of the
`e2e-kind.yml` matrix did. Requiring those two, rather than a list of job
names, is what keeps a newly added job gated from the moment it exists:
naming jobs individually is how `Proto lint and generated code drift` came to
run on every PR without ever being able to block one, and it is why
`Envtest`, `Integration` and the reshard and upgrade e2e cells cannot block
one now either, though each is in a gate's `needs`.

Neither gate is required yet. Switching protection to them is a deliberate
step, not a tidy-up: it makes every cell of both workflows blocking at once,
including the suites whose failure rate is still being measured.

The reshard and upgrade e2e suites run single-replica clusters
(`unsafeSingleReplica: true`, see [crd.md](crd.md)) so their pods fit the
hosted runner. They are covered by **e2e gate** rather than by name, so
switching branch protection over to it requires their failure rate to be
known first — otherwise a flaky suite blocks every merge. That measurement
is what `repeat.yml` exists for.

Note that the `smoke` cells prove only that kind comes up: they start a
`busybox` pod and log `PG_MAJOR`, and build no pgshard image. They are not
evidence that the product works, and should not be read as such when
looking at a green matrix.
