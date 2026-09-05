# Continuous integration

Workflows live under `.github/workflows`; `hack/check-actions.sh` enforces the
policy (SHA-pinned actions with version comments, `docker://` steps pinned by
digest, no top-level `contents: write`, no `pull_request_target`, no untrusted
event fields interpolated into `run:` or `with:`). `hack/test-check-actions.sh`
runs the checker against the fixtures under `hack/testdata`.

| Workflow | What it does |
| --- | --- |
| `ci.yml` | gofmt, vet, golangci-lint, `go test -race`, build; `buf lint`, `buf breaking` and a drift check that fails when `make proto` changes `internal/gen`; govulncheck; gitleaks; actionlint and the policy checker; PR title check |
| `images.yml` | builds the PostgreSQL 18 and 19 images with `docker buildx bake` and pushes them to GHCR on `main` |
| `e2e-kind.yml` | kind-based smoke, operator, backup and reshard suites for both majors, plus upgrade, reshard-split and reshard-merge on pg18 |
| `perf.yml` | benchstat comparison against the base branch; only benchmarks tagged `Gate` can fail a PR (see `hack/perf/gate.sh`) |
| `repeat.yml` | manual: repeats one Go package or the integration suite up to 50 times, against an optional `ref`, and reports the pass rate |
| `repeat-e2e.yml` | manual: repeats one kind e2e suite up to 12 times, against an optional `ref`, and reports the pass rate |
| `chaos.yml` | Chaos Mesh experiments (`test/chaos`) |
| `release.yml` | on a `v*` tag: verifies the commit, builds every image from it, publishes them with an immutable version tag, and signs a build provenance attestation for each |
| `mirror-base-images.yml` | copies the pinned Docker Hub base images into GHCR so a build does not depend on an anonymous pull |

The e2e builds take their bases from that mirror. `hack/ci/mirror-args.sh`
emits a `--build-context` override per image, which redirects a `FROM` to the
mirrored copy of the **same digest** without touching any Dockerfile: the
pins stay where they are, and a build with no overrides -- a fork, or a base
added before the mirror workflow ran -- goes to Docker Hub exactly as
before. `hack/ci/test-mirror-args.sh` checks every override names a `FROM`
that exists and redirects it to the same digest, because buildx accepts an
override that matches nothing and silently ignores it.

The mirror tag ends in the first twelve characters of the digest
(`golang-1.27-bookworm-ded31c68586d`), so **one name:tag at two digests is
two mirror tags**. That happens routinely rather than exceptionally:
dependabot raises one pull request per directory, so a `golang` bump reaches
`Dockerfile.control` and `Dockerfile.router` in one and `postgres/Dockerfile`
in another, and between them the tree pins the same tag at both digests. A
tag derived from the name alone made those collide, `hack/ci/base-images.sh`
reported *two images share a mirror tag*, and neither pull request could
merge -- each was blocked by the state only the other could clear. Consumers
reference the mirror by digest, so the tag is a label rather than an address
and lengthening it changes nothing else.
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

## What `pgshard.v1` promises

`buf breaking` runs on every pull request, against `main`, at the **WIRE**
category. That is a decision, not a default.

`FILE` — buf's strictest — forbids removing an rpc or a message.
`pgshard.v1` has never been released: no tag, no release, and the images
carry moving tags, so nothing outside this repository generates clients from
it. Holding the proto to a promise nothing depends on costs real work;
PGS-629 removed an rpc that was declared and implemented nowhere, and `FILE`
would have refused it.

What `WIRE` still forbids is the change that breaks a deployment silently: a
field number reused for a different type, or a type changed under a field
that keeps its number. A router and a pooler of different versions then
disagree about the bytes on the wire, and no amount of "it is pre-1.0" makes
that acceptable.

Tighten it to `FILE` when `pgshard.v1` is released and something outside this
repository depends on it.

## Cutting a release

Branch pushes publish moving tags -- `:latest` and `:<sha>` -- and the deploy
path uses `:latest`. A moving tag cannot tell a consumer that the image behind
it was replaced, so a release is what pins one.

Push a `v*` tag. `release.yml` then:

1. runs `make verify` on that commit, so a tag pointing at a tree that does not
   build publishes nothing. The e2e and chaos matrices are not repeated here --
   they belong to the commit's own CI run, and rerunning them would put an hour
   between a tag and its images;
2. builds every image -- both PostgreSQL majors and all four control-plane
   images -- from that one commit, and tags each with the version as well as
   the moving tags;
3. attaches an SBOM and maximum-mode provenance to each image;
4. signs a build provenance attestation per image and pushes it to the
   registry;
5. records every immutable digest in the job summary and in the release notes.

`workflow_dispatch` builds the same way and publishes nothing unless `dry_run`
is turned off, so the path can be exercised without cutting a release.

### Verifying an image against the source

Every published image carries a signed attestation naming the commit it was
built from:

```
gh attestation verify oci://ghcr.io/andrew01234567890/pgshard-operator@sha256:<digest> \
    --repo andrew01234567890/pgshard
```

Use the digest, not the tag: verifying `:latest` verifies whatever it points at
now, which is the property a release exists to remove. The digests are in the
release notes.

## Measuring a flake

A test is not flaky until a rate says so, and not fixed until the same run
says so again: one green run proves nothing about a test that fails one time
in five. `repeat.yml` repeats a Go package (up to 50 runs); `repeat-e2e.yml`
repeats one kind e2e suite (up to 12, because each run builds images and
stands up its own cluster). Both take a `ref`, so a suspect branch can be
compared against `main` on the same target.

`repeat.yml`'s `integration` target covers the suite `make integration`
runs -- `test/e2e/router`, the agent and pgtune -- which is where the
tests that flake in practice live, since they drive real PostgreSQL
through a real router. Note that `router` means `./internal/router/...`:
the cutover and two-phase-commit tests are under `test/e2e/router` and
belong to `integration`. With a `pattern` it repeats one of them:

    gh workflow run repeat.yml -f target=integration -f runs=20 \
      -f pattern=TestReshardCutoverUnderLoad -f ref=my-branch

Twelve runs bounds a failure rate at roughly one in twelve. It cannot see a
rarer flake, so a green twelve is not evidence of zero — the run summary says
so, and neither number should be reported as more than it is.

## An archived dependency we cannot drop

`github.com/json-iterator/go v1.1.12` is in the module graph and its upstream
was archived on 2025-12-15, so a future parsing vulnerability there may never
get a fix. It is not ours to remove:

```
$ go mod why github.com/json-iterator/go
github.com/andrew01234567890/pgshard/api/v1alpha1
k8s.io/apimachinery/pkg/runtime
sigs.k8s.io/structured-merge-diff/v6/value
github.com/json-iterator/go
```

Every controller-runtime user reaches it the same way. Dropping it means
Kubernetes finishing its own move off json-iterator in
`structured-merge-diff`; until then the honest position is that the risk is
accepted rather than mitigated. Its `modern-go/concurrent` and
`modern-go/reflect2` dependencies are old for the same reason.

What bounds it: govulncheck runs in CI and reports on the packages actually
reachable from our code, so a vulnerability in a json-iterator path we call
fails a required check rather than sitting unnoticed; `dependency-review`
covers what a pull request adds. Neither makes an archived upstream
maintained, which is why this is written down instead of closed.

Revisit when `structured-merge-diff` no longer imports it -- the check is the
`go mod why` above returning nothing.

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
