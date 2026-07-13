.PHONY: check rust-check rust-static rust-test proto-check go-check go-format-check go-generated-check docs-check actions-check public-check release-build

check: rust-check proto-check go-check docs-check actions-check public-check

rust-check: rust-static rust-test

rust-static:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo deny --locked check
	cargo audit --deny warnings

rust-test:
	cargo test --workspace --all-features --locked

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

release-build:
	@test -n "$(VERSION)" || (echo "VERSION is required" >&2; exit 1)
	@test -n "$(SHA)" || (echo "SHA is required" >&2; exit 1)
	PGSHARD_BUILD_VERSION="$(VERSION)" PGSHARD_GIT_SHA="$(SHA)" cargo build --workspace --all-targets --all-features --locked
