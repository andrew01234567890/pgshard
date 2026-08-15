# pgshard

pgshard is a Go project for a sharded PostgreSQL system. The public
repository currently contains only the initial build foundation: command
skeletons, shared build metadata, and repository checks. The command
skeletons do not start services or claim to implement routing, pooling,
control-plane, operator, or change-data-capture behavior yet.

## Requirements

- Go 1.26.4 (the version pinned by `go.mod`)
- GNU Make

## Commands

Run the complete local verification gate with:

```text
make verify
```

The available commands are:

```text
make fmt-check  # Check Go formatting without changing files.
make vet        # Run go vet.
make test       # Run the unit tests.
make test-race  # Run the tests with the race detector.
make build      # Build the command skeletons into the ignored bin/ directory.
```

Each command supports `--help` and `--version`. It reports that runtime
behavior is not yet configured and exits without starting a service.

## Public repository safety

Never commit credentials, tokens, private keys, private URLs, customer data,
or other private material. Deployment configuration should refer to managed
Kubernetes Secret objects by name; tests and examples should use synthetic
fixtures. Review staged changes for secrets and private data before creating a
commit or pull request.

See [AGENTS.md](AGENTS.md) for contributor and automation guidance.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
