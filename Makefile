.PHONY: check rust-check rust-static rust-test proto-check docs-check actions-check public-check release-build

check: rust-check proto-check docs-check actions-check public-check

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

docs-check:
	cd website && npm ci
	cd website && npm run check
	cd website && npm audit --audit-level=high

actions-check:
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12

public-check:
	cargo run --locked -p pgshard-release -- audit --base origin/main --head HEAD

release-build:
	@test -n "$(VERSION)" || (echo "VERSION is required" >&2; exit 1)
	@test -n "$(SHA)" || (echo "SHA is required" >&2; exit 1)
	PGSHARD_BUILD_VERSION="$(VERSION)" PGSHARD_GIT_SHA="$(SHA)" cargo build --workspace --all-targets --all-features --locked
