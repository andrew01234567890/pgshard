MODULE   := github.com/andrew01234567890/pgshard
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE     ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)
CMDS     := $(notdir $(wildcard cmd/*))

.PHONY: build vet fmt-check lint test verify clean proto proto-lint proto-breaking pgparser-sync pgparser-proto

build:
	@mkdir -p bin
	@for c in $(CMDS); do echo "building $$c"; go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$$c ./cmd/$$c || exit 1; done

vet:
	go vet ./...

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

lint:
	golangci-lint run ./...

test:
	go test -race ./...

verify: fmt-check vet lint proto-lint test build

clean:
	rm -rf bin dist

proto:
	buf generate

proto-lint:
	buf lint

proto-breaking:
	@if git ls-tree -r --name-only main -- proto | grep -q '\.proto$$'; then \
		buf breaking --against '.git#branch=main'; \
	else \
		echo "proto-breaking: no proto files on main, skipping"; \
	fi

.PHONY: kind-up kind-down e2e perf-bench

kind-up:
	hack/kind/up.sh

kind-down:
	hack/kind/down.sh

e2e:
	go test -tags e2e -count=1 -v ./test/e2e/...

perf-bench:
	hack/perf/benchstat.sh $(PERF_BASE_REF) $(PERF_OUT_DIR)

# libpg_query vendoring: one pinned tag per PostgreSQL major.
LIBPG_QUERY_18_TAG := 18.0.0

pgparser-sync:
	hack/pgparser/sync.sh 18 $(LIBPG_QUERY_18_TAG)
	$(MAKE) pgparser-proto

pgparser-proto:
	buf generate --template buf.gen.pgparser.yaml
