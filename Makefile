SHELL := /bin/sh

GO ?= go
BIN_DIR ?= bin
# Go cannot discover VCS metadata from every linked worktree layout. The
# shared buildinfo package has explicit fields for release metadata, so keep
# the foundation build independent of implicit VCS stamping. Release jobs may
# override this variable when they provide their own metadata.
GO_BUILD_FLAGS ?= -buildvcs=false

COMMANDS := \
	pgshard-operator \
	pgshard-gateway \
	pgshard-pooler \
	pgshard-controller \
	pgshard-cdc

.PHONY: fmt-check vet test test-race build verify

fmt-check:
	@set -eu; \
	offenders="$$(gofmt -l $$(find . -type f -name '*.go' -not -path './$(BIN_DIR)/*'))"; \
	if [ -n "$$offenders" ]; then \
		printf '%s\n' "$$offenders"; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

build:
	@set -eu; \
	mkdir -p $(BIN_DIR); \
	for command in $(COMMANDS); do \
		$(GO) build $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$$command ./cmd/$$command || { status=$$?; exit $$status; }; \
	done

verify: fmt-check vet test test-race build
