# Contributing

## Public repository rules

This repository is public. Every file, commit message, issue and pull
request must follow these rules:

- Use only synthetic data. No real hostnames, endpoints, addresses,
  usernames or database contents; use obvious placeholders such as
  `db.example.internal` or `<token>`.
- Never commit secrets or credentials of any kind, including expired ones.
- No private notes, personal paths or internal links.

## Pull requests

- Ship work as a stack of small, single-concern pull requests.
- Every change must be reachable from a `main.go` under `cmd/`; do not
  land capabilities without a caller.
- Tests are required, and `gofmt` must be clean. Run `make verify` before
  opening a PR, and the tier your change needs on top of it:
  [docs/guide/testing.md](docs/guide/testing.md) maps areas to gates.
  `make verify` skips the PostgreSQL-backed tests without Docker and the
  Kubernetes ones without envtest assets, so a green fast run is not on its
  own evidence that your area was covered.
- Titles follow Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`,
  `chore:`, `refactor:`), written in the imperative mood.

## Development

```sh
make tools    # pinned linters and code generators, once
make verify   # the fast gate
make gates    # everything CI gates on, except the secret scan
```

[docs/guide/testing.md](docs/guide/testing.md) describes every tier, what it
needs and which change calls for it.
