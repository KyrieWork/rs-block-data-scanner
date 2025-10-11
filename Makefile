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

.PHONY: fmt clippy test verify ci