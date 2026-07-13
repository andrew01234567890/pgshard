---
title: Releases and versioning
description: SemVer rules and source-only GitHub releases.
---

# Releases and versioning

Every successful new commit on `main` at or after the repository's release-start
marker receives exactly one SemVer tag and a source-only GitHub Release.
Milestone 1 releases use `0.x` prerelease versions. The initial foundation
commit predates the usable post-squash identity policy and remains an untagged
bootstrap commit rather than bypassing the exact-head CI release gate.

## Version calculation

- The first green squash commit containing the release-start marker is `v0.1.0`.
- Before 1.0, `feat`, `!`, or a `BREAKING CHANGE:` footer increments the minor version.
- `fix`, `perf`, `refactor`, `revert`, `docs`, `test`, `build`, `ci`, and `chore` increment patch.
- Promotion to 1.0 is an explicit maintainer decision and is not performed by the automated pre-1.0 calculator.
- Pull request titles follow Conventional Commits because squash merge makes the title the `main` commit subject.

The release job is serialized and idempotent. Queue order is not trusted: each
invocation finds the nearest SemVer tag on the selected commit's first-parent
history and publishes every untagged first-parent commit oldest-first. A
descendant job may therefore safely run before its ancestor's job. It creates no
version-bump commit, preventing release loops. Documentation-only and CI-only
default-branch commits still receive patch releases.

Source-branch commits must use GitHub noreply author and committer addresses.
GitHub's squash operation may retain the account's public author address when
an author override is unavailable. The default-branch audit accepts that narrow
exception only when GitHub's commit API reports a valid signed commit created by
the `web-flow` committer and the local committer uses GitHub's noreply address.
The exception does not rewrite or hide author metadata.

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

Milestone 1 does **not** publish container images, Helm charts, operator bundles, binaries, crates, npm packages, SBOMs, or provenance to a registry or GitHub Release. Releases contain generated notes and GitHub's source archives only. Short-lived CI artifacts are used internally for KIND jobs.
