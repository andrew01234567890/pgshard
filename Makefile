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

# -p bounds how many PACKAGES run at once; PGSHARD_TEST_PG_PARALLEL bounds the
# PostgreSQL-backed tests within each. Containers in flight is the product of
# the two, so they are set together here: raising one without the other swamps
# the runner. See internal/dockertest/parallel.go.
test: PGSHARD_TEST_PG_PARALLEL ?= 4
test:
	PGSHARD_TEST_PG_PARALLEL=$(PGSHARD_TEST_PG_PARALLEL) go test -race -p 4 ./...

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

.PHONY: kind-up kind-down e2e perf-bench admin-image deploy-admin undeploy-admin

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

CONTROLLER_GEN_VERSION ?= v0.21.0
SETUP_ENVTEST_VERSION  ?= v0.24.1
ENVTEST_K8S_VERSION    ?= 1.34
ENVTEST_ASSETS_DIR     ?= $(CURDIR)/bin/k8s
CONTROLLER_GEN          = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

.PHONY: generate manifests envtest-assets envtest

generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

manifests:
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

envtest-assets:
	SETUP_ENVTEST_VERSION=$(SETUP_ENVTEST_VERSION) ENVTEST_K8S_VERSION=$(ENVTEST_K8S_VERSION) \
		hack/envtest/setup-envtest.sh $(ENVTEST_ASSETS_DIR)

envtest: envtest-assets
	KUBEBUILDER_ASSETS="$$(hack/envtest/setup-envtest.sh $(ENVTEST_ASSETS_DIR))" PGSHARD_REQUIRE_ENVTEST=1 go test -race -count=1 ./api/... ./internal/operator/... ./internal/admin/...

IMG ?= ghcr.io/andrew01234567890/pgshard-operator:latest

.PHONY: install uninstall deploy undeploy operator-image

install:
	kubectl apply -f config/crd/bases

uninstall:
	kubectl delete --ignore-not-found -f config/crd/bases

operator-image:
	docker build -f Dockerfile.control --build-arg CMD=pgshard-operator \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t $(IMG) .

ADMIN_IMG ?= ghcr.io/andrew01234567890/pgshard-admin:latest

admin-image:
	docker build -f Dockerfile.control --build-arg CMD=pgshard-admin \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t $(ADMIN_IMG) .

deploy-admin:
	kubectl apply -f config/admin/service_account.yaml -f config/admin/rbac.yaml
	sed 's|image: ghcr.io/andrew01234567890/pgshard-admin:latest|image: $(ADMIN_IMG)|' config/admin/deployment.yaml | kubectl apply -f -

undeploy-admin:
	kubectl delete --ignore-not-found -f config/admin

deploy: install
	kubectl apply -f config/manager/namespace.yaml
	kubectl apply -f config/rbac
	sed 's|image: ghcr.io/andrew01234567890/pgshard-operator:latest|image: $(IMG)|' config/manager/manager.yaml | kubectl apply -f -

undeploy:
	kubectl delete --ignore-not-found -f config/manager/manager.yaml
	kubectl delete --ignore-not-found -f config/rbac
	kubectl delete --ignore-not-found -f config/manager/namespace.yaml
