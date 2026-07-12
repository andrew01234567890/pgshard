.PHONY: check fmt lint test public-check

check: fmt lint test public-check

fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace --all-targets

public-check:
	cargo run --locked -p pgshard-release -- audit --base origin/main --head HEAD
