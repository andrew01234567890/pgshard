# Security policy

Security issues include vulnerabilities in the runtime, control plane, data
plane, transaction recovery, backup and restore paths, deployment manifests,
and public-repository automation. The [threat model](docs/threat-model.md)
records the design assumptions and boundaries.

## Reporting a vulnerability

Please do **not** file a vulnerability, exploit details, credentials, or a
proof of concept in a public issue, discussion, pull request, or commit.

When it is enabled for this repository, use GitHub's **private vulnerability
reporting** feature from the repository Security tab. Include the affected
commit or release, impact, reproduction steps, and any mitigation that can be
shared safely. Do not include real secrets in the report.

If private vulnerability reporting is not enabled, do not publish the details.
Use an already documented private maintainer channel, or open a public issue
that contains only a request for a private reporting channel and no sensitive
technical information.

## Repository and deployment hygiene

The repository must contain only synthetic fixtures and public information.
Never commit secrets, private keys, customer data, internal endpoints, or
unredacted production logs. Remove and rotate any credential that may have
been exposed, even if the commit is later deleted. CI credentials must be
scoped to the minimum permissions and must not be printed in logs.

The security policy does not promise that the planned architecture is already
implemented or production-ready. Until the relevant code and tests exist,
security properties in the public design documents are requirements and review
boundaries, not evidence of a deployed guarantee.
