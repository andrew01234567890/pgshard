---
title: Releases and versioning
description: SemVer rules and source-only GitHub releases.
---

# Releases and versioning

Every successful new commit on `main` receives exactly one SemVer tag and a source-only GitHub Release. Milestone 1 releases use `0.x` prerelease versions.

## Version calculation

- The first foundation squash commit is `v0.1.0`.
- Before 1.0, `feat`, `!`, or `BREAKING CHANGE` increments the minor version.
- `fix`, `perf`, `refactor`, `revert`, `docs`, `test`, `build`, `ci`, and `chore` increment patch.
- Promotion to 1.0 requires explicit authorization and the configured major-version label.
- Pull request titles follow Conventional Commits because squash merge makes the title the `main` commit subject.

The release job is serialized and idempotent. It creates no version-bump commit, preventing release loops. Documentation-only and CI-only default-branch commits still receive patch releases.

## Publishing boundary

Milestone 1 does **not** publish container images, Helm charts, operator bundles, binaries, crates, npm packages, SBOMs, or provenance to a registry or GitHub Release. Releases contain generated notes and GitHub's source archives only. Short-lived CI artifacts are used internally for KIND jobs.
