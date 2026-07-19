---
title: Releases and versioning
description: SemVer rules and source-only GitHub releases.
---

# Releases and versioning

Every successful new commit on `main` at or after the repository's release-start
marker receives exactly one SemVer tag and a source-only GitHub Release.
Milestone 1 releases use `0.x` prerelease versions. The initial foundation
commit predates the release-start marker and remains an untagged bootstrap
commit rather than bypassing the exact-head CI release gate.

## Version calculation

- The first green squash commit containing the release-start marker is `v0.1.0`.
- Before 1.0, `feat`, `!`, or a `BREAKING CHANGE:` footer increments the minor version.
- `fix`, `perf`, `refactor`, `revert`, `docs`, `test`, `build`, `ci`, and `chore` increment patch.
- Promotion to 1.0 is an explicit maintainer decision and is not performed by the automated pre-1.0 calculator.
- Pull request titles follow Conventional Commits because squash merge makes the title the `main` commit subject.

The release planner is serialized and idempotent. Queue order is not trusted:
it finds the nearest SemVer tag on the selected commit's first-parent history
and considers every untagged first-parent commit oldest-first. Strict exact-SHA
publication waits for every planned ancestor's exact aggregate check and fails
if a completed aggregate failed. Normal main reconciliation, described below,
instead stops before the first unchecked commit. Neither mode releases an
unchecked gap or creates a version-bump commit.
Documentation-only and CI-only default-branch commits still receive patch
releases.

The complete CI workflow is serialized for every non-pull-request run. Main
pushes, scheduled validation, and exact-SHA Dependabot dispatches share a
maximum-depth concurrency queue, so only one such run builds at a time while
pull requests retain independent CI capacity. GitHub processes this queue
first-in-first-out by the time each run starts waiting, rather than by dispatch
or commit order.

Publication runs in a separate trusted `workflow_run` workflow after successful
CI and retains its own serialized queue. Normal main publication resolves the
live main tip and publishes only its oldest contiguous CI-green prefix. A
reordered descendant therefore defers at the first unchecked ancestor without
holding or failing the build queue; every later successful completion retries
the live gap. Exact-SHA Dependabot publication retains its strict wait before
deleting the temporary tag. The release planner always publishes untagged
first-parent commits oldest-first.

Pages deployment is reconciled after publication under that same serialized
workflow. It resolves the live main SHA, requires an exact source release at
that commit, locates the `pages-site` artifact from successful CI on the same
SHA, and deploys only that artifact. A reordered older run therefore either
deploys the same live content or defers; it cannot overwrite the site with an
older commit.

Verified Dependabot updates to the unattended file allowlist are squash-merged
by the trusted default-branch workflow when every dependency has explicit patch
or minor metadata and their exact pull-request head passes the aggregate CI gate
with every reported check terminal without a failure. Missing or major update
metadata requires manual review. The GitHub Advanced Security CodeQL aggregate
must be completed successfully; a neutral summary, including "configurations
not found", fails closed instead of authorizing an unattended merge. The pull
request must also be based on the current `main` commit. Merge attempts are
[queued and serialized](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency#using-concurrency-in-different-scenarios)
so concurrent green updates recheck that base after the preceding squash instead
of racing it.
[GitHub suppresses ordinary `push` workflow runs](https://docs.github.com/en/actions/concepts/security/github_token#when-github_token-triggers-workflow-runs)
for merges authenticated with the repository `GITHUB_TOKEN`, so that workflow
creates a temporary tag at the exact squash commit and dispatches this same CI
workflow on the immutable tag. A dispatch runs every component, records an
aggregate result on the exact squash commit, and may publish releases under the
same serialized, idempotent release job. The tag is deleted only after both the
aggregate and publication succeed; a failure retains it for an exact rerun.
The publisher independently requires that the requested commit is reachable
from the live `main` ref, so a lookalike tag cannot authorize an unmerged
release. This uses GitHub's documented `workflow_dispatch` exception rather
than a personal access token or repository secret.

Unattended merging is limited to two exact dependency-file pairs:
`operator/go.mod` with `operator/go.sum`, and
`crates/pgshard-pgwire/fuzz/Cargo.toml` with its colocated `Cargo.lock`. npm,
Docker, root-workspace Cargo, other Cargo, and GitHub Actions updates still
require a normal reviewed squash because default-branch workflows can deploy
the website or execute the Rust publisher, image builds, and pinned actions
with a write token. All still receive Dependabot pull requests and CI. The
allowlist also rejects renamed files and any unexpected bot-authored source or
workflow change.

The public-history audit scans every added or modified historical blob as raw
bytes. Safe non-UTF-8 assets are permitted, while forbidden ASCII credential and
private-path signatures remain rejected inside text or binary content, including
content removed by a later commit in the pull request.

Runtime version strings are derived from the exact release tag when building a
tagged commit. Untagged builds report a development SemVer containing the commit
SHA; workspace package metadata is not presented as the running release version.

GitHub's generated source archives do not contain Git metadata. A reproducible
archive build therefore supplies the tag version and the full commit SHA shown
on the release page explicitly:

```bash
make release-build VERSION=0.1.0 SHA=<40-character-release-commit>
```

The build rejects malformed explicit SHAs or versions. CI extracts a source
archive without `.git`, builds it with explicit inputs, and asserts that the
compiled identity matches both values.

## Publishing boundary

Milestone 1 does **not** publish container images, Helm charts, operator bundles, binaries, crates, npm packages, SBOMs, or provenance to a registry or GitHub Release. Releases contain generated notes and GitHub's source archives only. Image archives remain local to the CI job that builds and inspects them; they are not uploaded as workflow artifacts.
