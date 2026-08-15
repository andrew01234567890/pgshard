# ADR 0007: Stacked pull-request delivery

- Status: Accepted repository workflow decision.

## Context

The design spans runtime, protocol, operations, and documentation concerns.
Large all-at-once changes hide dependencies and make review and rollback
difficult.

## Decision

Deliver related public work as an ordered stack of small dependent pull
requests. Before review, verify every member's exact head SHA and exact base
SHA. Obtain
complete-stack independent Sol and Opus reviews against those exact heads.

If a blocker is found, add a forward commit on the affected branch. Do not
rewrite a published stack. Rerun both reviews and every check against the new
exact heads. Run performance tests against a recorded exact commit SHA, never
against a moving branch name. Once the whole stack is green and the exact
heads have been re-verified, merge it atomically with the official command:

```text
gh stack merge --yes --merge
```

Documentation and tests travel with the behavior they describe.

The one-time M0 bootstrap is an explicit exception in hosted-check coverage:
workflows exist only in the top layer, so lower pull requests have no hosted
checks. Require full top-tree local and hosted evidence plus both independent
reviews before the atomic merge, then enable default-branch protection
immediately. Protection is not claimed to exist before that step. Future
stacks inherit CI on every pull request.

## Consequences

Changes remain reviewable and dependencies are visible. Contributors must keep
parent branches current, explain stack order, and avoid direct pushes to the
default branch. A stack is a delivery mechanism, not a substitute for
integration tests or independent review.
