fmt:
	@echo "Formatting code..."
	cargo fmt

test:
	@echo "Running tests..."
	cargo test

clippy:
	@echo "Running clippy checks..."
	cargo clippy --all-targets --all-features -- -D warnings

verify: fmt clippy test
	@echo "Project verified!"

ci: clippy test
	@echo "All checks passed!"

run:
	@echo "Starting block scanner..."
	cargo run --bin rs-block-data-scanner -- --config config.yaml

run-api:
	@echo "Starting API service..."
	cargo run --bin api_main -- --config config.yaml

.PHONY: fmt clippy test verify ci
