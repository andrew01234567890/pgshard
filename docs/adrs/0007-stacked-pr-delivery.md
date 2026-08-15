# ADR 0007: Stacked pull-request delivery

- Status: Accepted repository workflow decision.

## Context

The design spans runtime, protocol, operations, and documentation concerns.
Large all-at-once changes hide dependencies and make review and rollback
difficult.

## Decision

Deliver related public work as an ordered stack of small dependent pull
requests. Each branch names its parent, each pull request has one scope and a
reproducible test result, and reviewers merge the stack in order. Documentation
and tests travel with the behavior they describe.

## Consequences

Changes remain reviewable and dependencies are visible. Contributors must keep
parent branches current, explain stack order, and avoid direct pushes to the
default branch. A stack is a delivery mechanism, not a substitute for
integration tests or independent review.
