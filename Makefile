MODULE   := github.com/andrew01234567890/pgshard
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE     ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  := -X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
            -X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
            -X $(MODULE)/internal/buildinfo.Date=$(DATE)
CMDS     := $(notdir $(wildcard cmd/*))

.PHONY: build vet fmt-check lint test verify vendor-check gates tools proto-drift govulncheck actionlint clean proto proto-lint proto-breaking pgparser-sync pgparser-proto

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

# verify is the fast gate: everything that needs only Go, a C compiler and
# the pinned linters. It deliberately does not run the gates that need a
# Kubernetes control plane, Docker images or a network, so it can be fast
# enough to run before every push -- `make gates` is the whole set, and
# docs/guide/testing.md says which tier a change needs.
# vendor-check asserts the vendored libpg_query trees are the bytes
# hack/pgparser/sync.sh recorded. It is in the fast gate because a change
# there is a change to the parser every statement goes through, and nothing
# else in the repository would notice one.
vendor-check:
	hack/pgparser/verify.sh

verify: fmt-check vet lint proto-lint vendor-check test build
	@echo "verify: fast gate passed. Not run here: envtest, e2e, generated-code drift, govulncheck, workflow policy. See 'make gates'."

# gates runs every check CI gates a pull request on except the secret scan,
# which runs as a GitHub action over the repository history.
gates: verify envtest proto-drift govulncheck actionlint

tools:
	hack/tools/install.sh

proto-drift: proto
	@if ! git diff --exit-code -- internal/gen; then \
		echo "internal/gen is out of date; run 'make proto' and commit the result"; exit 1; \
	fi

# mod-check keeps the module graph honest: verify proves the downloaded
# sources match go.sum, and tidy -diff fails on a go.mod that does not match
# what the code imports -- a direct dependency left marked indirect, or an
# indirect one that has become direct.
mod-check:
	go mod verify
	go mod tidy -diff

govulncheck:
	govulncheck ./...

actionlint:
	actionlint -color
	hack/check-actions.sh
	hack/test-check-actions.sh
	hack/ci/test-retry.sh
	hack/tools/test-install-retry.sh
	hack/e2e/test-suites.sh

migration-check:
	hack/test-check-migration-numbers.sh
	hack/check-migration-numbers.sh

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

.PHONY: kind-up kind-down dev-up dev-down e2e integration perf-bench admin-image deploy-admin undeploy-admin

kind-up:
	hack/kind/up.sh

# dev-up is the whole getting-started path in one command: a kind cluster,
# every image this repository builds, loaded into it, the operator deployed
# against those images, and a small cluster applied.
#
# It exists because the guide's sequence could not be followed. CI publishes
# only the PostgreSQL images, so the router and operator tags it named were
# never pushed; kind-up created a cluster and loaded nothing; and nothing
# told the operator to run a locally built router. Each step worked and the
# path through them did not, which is the kind of thing only an executable
# target keeps true.
dev-up:
	hack/kind/dev-up.sh

kind-down:
	hack/kind/down.sh

# e2e runs one suite the way CI runs it: the images that suite needs, a kind
# cluster, the images loaded into it, and the one go test invocation with the
# timeout and filter that suite is known to need. `go test ./test/e2e/...`
# is not that -- see the comment at the top of hack/e2e/run.sh.
E2E_SUITE ?=
E2E_PG    ?= 18

e2e:
	hack/e2e/run.sh $(E2E_SUITE) $(E2E_PG)

# integration runs the suites that need Docker and the PostgreSQL image but
# not Kubernetes: the router system tests against a real pooler and real
# servers, the agent's lifecycle and backup suites, and the live check that
# every setting pgtune renders exists on the server it renders for.
#
# They carry their own build tag, and for a long time nothing ran them --
# not this Makefile and not any workflow -- which is how a live test that
# could never pass, and refusal messages that had changed underneath their
# assertions, went unnoticed. The timeout is the suite's, not Go's ten
# minutes: the router suite alone takes longer than that.
integration:
	PGSHARD_REQUIRE_DOCKER=1 go test -tags integration -count=1 -timeout 45m ./test/e2e/router/... ./internal/agent/... ./internal/pgtune/...

perf-bench:
	hack/perf/benchstat.sh $(PERF_BASE_REF) $(PERF_OUT_DIR)

# libpg_query vendoring: one pinned tag per PostgreSQL major, and the commit
# that tag must resolve to. A tag is a movable name and this is 14 MiB of C
# in the router's SQL parser, so the commit is what is actually pinned.
LIBPG_QUERY_18_TAG    := 18.0.0
LIBPG_QUERY_18_COMMIT := 204fbdbd3ed5f8691ab358e49f1fc5397b4679e2

pgparser-sync:
	hack/pgparser/sync.sh 18 $(LIBPG_QUERY_18_TAG) $(LIBPG_QUERY_18_COMMIT)
	$(MAKE) pgparser-proto

pgparser-proto:
	buf generate --template buf.gen.pgparser.yaml

CONTROLLER_GEN_VERSION ?= v0.21.0
SETUP_ENVTEST_VERSION  ?= v0.24.1
ENVTEST_K8S_VERSION    ?= 1.34
ENVTEST_ASSETS_DIR     ?= $(CURDIR)/bin/k8s
CONTROLLER_GEN          = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

.PHONY: mod-check
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

# The admin requires a credential, so the Secret comes first -- generated
# once and left alone, or the token would change under whoever holds it.
deploy-admin:
	kubectl get secret pgshard-admin >/dev/null 2>&1 || \
		kubectl create secret generic pgshard-admin --from-literal=token="$$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
	kubectl apply -f config/admin/service_account.yaml -f config/admin/rbac.yaml
	sed 's|image: ghcr.io/andrew01234567890/pgshard-admin:latest|image: $(ADMIN_IMG)|' config/admin/deployment.yaml | kubectl apply -f -
	@echo "admin token: kubectl get secret pgshard-admin -o jsonpath='{.data.token}' | base64 -d"

undeploy-admin:
	kubectl delete --ignore-not-found -f config/admin
	kubectl delete --ignore-not-found secret pgshard-admin

deploy: install
	kubectl apply -f config/manager/namespace.yaml
	kubectl apply -f config/rbac
	sed 's|image: ghcr.io/andrew01234567890/pgshard-operator:latest|image: $(IMG)|' config/manager/manager.yaml | kubectl apply -f -

undeploy:
	kubectl delete --ignore-not-found -f config/manager/manager.yaml
	kubectl delete --ignore-not-found -f config/rbac
	kubectl delete --ignore-not-found -f config/manager/namespace.yaml
