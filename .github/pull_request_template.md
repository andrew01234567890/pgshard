## Summary

<!-- Explain what changed and why. Keep public-repository content free of private or internal information. -->

## Validation

- [ ] Unit tests added or updated and run
- [ ] Integration or KIND coverage added when behavior crosses component boundaries
- [ ] Performance impact measured when the pooler, routing, protocol, or CDC fast path changes
- [ ] Documentation and compatibility tables updated with the implementation
- [ ] No credentials, private paths, private email addresses, internal hostnames, or sensitive logs are included

## Correctness review

- [ ] Failure, retry, crash-recovery, and idempotency behavior considered
- [ ] ACID guarantees and limitations remain accurate
- [ ] Jepsen/Elle histories or checkers updated when distributed behavior changes
- [ ] Backup, restore, DDL, resharding, and CDC safety considered where applicable
- [ ] The change has been simplified as far as behavior permits

## Release and review

- [ ] PR title is a valid Conventional Commit and describes the squash commit
- [ ] Commit author and committer use the repository-approved GitHub noreply identity
- [ ] Independent `gpt-5.6-sol high` review requested, or the nearest available high-reasoning model is disclosed
- [ ] Material review fixes received a fresh independent review
- [ ] This PR will be squash-merged only
