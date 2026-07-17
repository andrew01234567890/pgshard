.PHONY: check rust-check rust-static rust-test pgwire-fuzz-static catalog-test orch-catalog-test orch-slot-observer-test pgwire-postgres-test pooler-postgres-test planner-postgres-test proto-check go-check go-format-check go-generated-check docs-check actions-check public-check images release-build

PGSHARD_GIT_SHA ?= $(shell git rev-parse HEAD 2>/dev/null)
PGSHARD_BUILD_VERSION ?= 0.0.0-dev+local.$(shell printf '%.12s' "$(PGSHARD_GIT_SHA)")$(shell test -z "$$(git status --porcelain --untracked-files=normal 2>/dev/null)" || printf '.dirty')
PGSHARD_IMAGE_OUTPUT ?= artifacts/images
PGSHARD_IMAGE_TAG ?= dev
PGSHARD_IMAGE_TARGETS ?= ci

check: rust-check proto-check go-check docs-check actions-check public-check

rust-check: rust-static rust-test

rust-static: pgwire-fuzz-static
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo deny --locked check
	cargo audit --deny warnings

pgwire-fuzz-static:
	cargo fmt --manifest-path crates/pgshard-pgwire/fuzz/Cargo.toml --all -- --check
	cargo clippy --manifest-path crates/pgshard-pgwire/fuzz/Cargo.toml --all-targets --locked -- -D warnings
	cargo deny --locked --manifest-path crates/pgshard-pgwire/fuzz/Cargo.toml check
	cargo audit --deny warnings --file crates/pgshard-pgwire/fuzz/Cargo.lock

rust-test:
	cargo test --workspace --all-features --locked

catalog-test:
	@test -n "$(PGSHARD_TEST_DATABASE_URL)" || (echo "PGSHARD_TEST_DATABASE_URL is required" >&2; exit 1)
	cargo test --locked -p pgshard-catalog --test postgres18 -- --ignored --test-threads=1

orch-catalog-test:
	@test -n "$(PGSHARD_TEST_DATABASE_URL)" || (echo "PGSHARD_TEST_DATABASE_URL is required" >&2; exit 1)
	cargo test --locked -p pgshard-orch --test postgres18_catalog -- --ignored --test-threads=1

orch-slot-observer-test:
	@test -n "$(PGSHARD_TEST_DATABASE_URL)" || (echo "PGSHARD_TEST_DATABASE_URL is required" >&2; exit 1)
	@test -n "$(PGSHARD_TEST_LEGACY_DATABASE_URL)" || (echo "PGSHARD_TEST_LEGACY_DATABASE_URL is required" >&2; exit 1)
	@test -n "$(PGSHARD_TEST_STANDBY_DATABASE_URL)" || (echo "PGSHARD_TEST_STANDBY_DATABASE_URL is required" >&2; exit 1)
	cargo test --locked -p pgshard-orch --test postgres18_slots -- --ignored --test-threads=1

pgwire-postgres-test:
	@test -n "$(PGSHARD_PGWIRE_TEST_ADDRESS)" || (echo "PGSHARD_PGWIRE_TEST_ADDRESS is required" >&2; exit 1)
	cargo test --locked -p pgshard-pgwire --test postgres18 -- --ignored --test-threads=1

pooler-postgres-test:
	@test -n "$(PGSHARD_POOLER_TEST_ADDRESS)" || (echo "PGSHARD_POOLER_TEST_ADDRESS is required" >&2; exit 1)
	cargo test --locked -p pgshard-pooler --lib frontend::tests::relays_a_live_postgres18_session -- --ignored --exact --test-threads=1

planner-postgres-test:
	@test -n "$(PGSHARD_TEST_DATABASE_URL)" || (echo "PGSHARD_TEST_DATABASE_URL is required" >&2; exit 1)
	cargo test --locked -p pgshard-planner --test postgres18 -- --ignored --test-threads=1

proto-check:
	buf format --diff --exit-code
	buf lint
	buf build

go-check: go-format-check go-generated-check
	cd operator && go mod tidy
	git diff --exit-code -- operator/go.mod operator/go.sum
	cd operator && go mod verify
	cd operator && go vet ./...
	cd operator && go test -race ./...
	cd operator && go build ./...
	cd operator && go tool govulncheck ./...

go-format-check:
	@files="$$(find operator -type f -name '*.go' -print)"; unformatted="$$(gofmt -l $$files)"; test -z "$$unformatted" || (printf 'Unformatted Go files:\n%s\n' "$$unformatted" >&2; exit 1)

go-generated-check:
	cd operator && go tool controller-gen object paths=./...
	cd operator && go tool controller-gen crd:allowDangerousTypes=false paths=./... output:crd:artifacts:config=config/crd/bases
	cd operator && go tool controller-gen rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
	cd operator && go tool controller-gen webhook paths=./... output:webhook:artifacts:config=config/webhook
	git diff --exit-code -- operator/api/v1alpha1/zz_generated.deepcopy.go operator/config/crd/bases operator/config/rbac operator/config/webhook

docs-check:
	cd website && npm ci
	cd website && npm run check
	cd website && npm audit --audit-level=high

actions-check:
	# actionlint v1.7.12 predates GitHub's official concurrency queue key.
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -ignore 'unexpected key "queue" for "concurrency" section'

public-check:
	cargo run --locked -p pgshard-release -- audit --base origin/main --head HEAD

images:
	@test -n "$(PGSHARD_GIT_SHA)" || (echo "PGSHARD_GIT_SHA is required outside a Git checkout" >&2; exit 1)
	@mkdir -p "$(PGSHARD_IMAGE_OUTPUT)"
	PGSHARD_BUILD_VERSION="$(PGSHARD_BUILD_VERSION)" \
	PGSHARD_GIT_SHA="$(PGSHARD_GIT_SHA)" \
	PGSHARD_IMAGE_OUTPUT="$(PGSHARD_IMAGE_OUTPUT)" \
	PGSHARD_IMAGE_TAG="$(PGSHARD_IMAGE_TAG)" \
	docker buildx bake --file deploy/docker-bake.hcl $(PGSHARD_IMAGE_TARGETS)

release-build:
	@test -n "$(VERSION)" || (echo "VERSION is required" >&2; exit 1)
	@test -n "$(SHA)" || (echo "SHA is required" >&2; exit 1)
	PGSHARD_BUILD_VERSION="$(VERSION)" PGSHARD_GIT_SHA="$(SHA)" cargo build --workspace --all-targets --all-features --locked
