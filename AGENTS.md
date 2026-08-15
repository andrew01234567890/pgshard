# Contributor and agent guidance

## Scope

This is a public repository. Keep the initial foundation buildable and
honest: command skeletons may expose help and version metadata, but must not
pretend that unimplemented services work.

All runtime code in this repository is written in Go. Keep commands under
`cmd/` and shared build metadata under `internal/buildinfo/` unless a later
change explicitly expands that layout.

## Public-repository safety

- Never commit secrets, credentials, access tokens, private keys, private
  endpoints, customer data, or local machine paths.
- Deployment configuration must use references to managed Secret objects, not
  embedded secret values.
- Tests, examples, and fixtures must use synthetic data and credentials.
- Do not add generated binaries, local build output, or private milestone
  notes to commits.
- Before committing, inspect staged paths and content for accidental private
  data.

## Verification

Before opening a pull request, run:

```text
make verify
```

This checks formatting, vet, normal tests, race-detector tests, and builds all
public command skeletons. Keep changes limited to the requested scope and
document any verification limitation rather than claiming a check passed.
