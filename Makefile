export RUST_MIN_STACK ?= 8388608

.PHONY: build check clippy fmt test service-check service-no-default-check

build:
	cargo build --workspace

check:
	cargo check --workspace --locked

service-check:
	cargo check -p eden-service --locked --features server-runtime

service-no-default-check:
	cargo check -p eden-service --locked --no-default-features --tests

clippy:
	cargo clippy --workspace --locked --all-features --all-targets -- -D warnings

fmt:
	cargo fmt --all

test:
	cargo test --workspace --locked
