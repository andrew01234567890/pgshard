# Contributing

pgshard is being developed in public. The public repository describes intended
behavior until code and tests demonstrate it; contributors must not present a
design proposal as an implemented guarantee. Start with the [architecture
overview](docs/architecture.md), [guarantees and limits](docs/guarantees-and-limits.md),
and [threat model](docs/threat-model.md).

## Public-repository rules

- Use only synthetic data, public information, and reproducible fixtures.
- Never commit passwords, API keys, access tokens, private certificates,
  connection strings, customer data, or internal hostnames. Use obvious test
  placeholders and rotate a secret immediately if one is exposed.
- Do not add private planning notes, local filesystem paths, or copied
  material from a restricted source tree.
- Keep security reports out of public issues and pull requests; see
  [SECURITY.md](SECURITY.md).

## Branches and pull requests

Work on a topic branch and submit a pull request. Related changes should be
delivered as a small, ordered stack of dependent pull requests. Each pull
request must have one clear scope, target its parent branch, and be reviewable
and testable on its own. Explain the stack order and any dependency in the PR
description. Do not push directly to the default branch.

The official gh-stack delivery sequence is part of the release contract:

1. Verify every stack member's exact head SHA and exact base SHA before review; do
   not rely on branch names or abbreviated prefixes.
2. Obtain complete-stack independent Sol and Opus reviews against those exact
   heads.
3. Resolve a blocker with a forward commit on the affected branch. Do not
   rewrite a published stack; rerun both reviews and all checks against the new
   exact heads.
4. Run performance work against an exact recorded commit SHA, including the
   benchmark result's tested SHA.
5. After every stack member is green and the exact heads are re-verified, use
   the official atomic merge command:

   ```text
   gh stack merge --yes --merge
   ```

The command is used only for a fully green, fully reviewed stack. Do not
manually merge individual members in its place.

The one-time M0 bootstrap has a documented limitation: workflows exist only
in the top layer, so lower pull requests do not have hosted checks. Before the
bootstrap stack is merged, require full top-tree local and hosted evidence and
both independent reviews for the complete stack. Enable default-branch
protection immediately after that atomic merge; this document does not claim
that protection already exists. Future stacks inherit CI on every pull request.

Design changes should update the relevant [ADR](docs/adrs/0007-stacked-pr-delivery.md)
or add a focused ADR when a decision changes. Keep public documentation honest
about what is planned, implemented, and validated.

## Tests and checks

For any Go change, run `gofmt` on changed Go files and `go test ./...`; include
the commands and results in the pull request. Add regression tests for changed
behavior. If a requested check cannot run because its component is not yet
present, say so explicitly rather than claiming a passing result.

Documentation-only changes should still be checked for valid relative links,
correct Markdown, and accidental secrets or private paths.
