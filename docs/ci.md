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
| `e2e-kind.yml` | kind-based smoke, operator and backup suites for both majors |
| `perf.yml` | benchstat comparison against the base branch; only benchmarks tagged `Gate` can fail a PR (see `hack/perf/gate.sh`) |
| `chaos.yml` | Chaos Mesh experiments (`test/chaos`) |
| `dependency-review.yml`, `dependabot-automerge.yml` | dependency hygiene |

## Container images on GHCR (one-time bootstrap)

`images.yml` pushes `ghcr.io/andrew01234567890/pgshard-postgres:{18,18.x,19,19betaN}`
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
