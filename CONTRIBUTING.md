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
  opening a PR.
- Titles follow Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`,
  `chore:`, `refactor:`), written in the imperative mood.

## Development

```sh
make verify
```
