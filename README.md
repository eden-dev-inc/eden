# Eden

[![CI](https://github.com/eden-dev-inc/eden/actions/workflows/ci.yml/badge.svg)](https://github.com/eden-dev-inc/eden/actions/workflows/ci.yml)

Eden is a Rust gateway and service runtime for connecting to databases, model
providers, and infrastructure APIs through a consistent control plane. The
gateway/runtime layer is called Eve in the codebase and product surfaces.

The repository is organized as a Cargo workspace. The main service crate is
`eden-service`; protocol gateway implementations live under `eden_gateway/`;
endpoint implementations live under `endpoints/` and `endpoint-core/`.

## Protocol Support Status

Redis is the only production-ready gateway protocol in this release. Other
protocol and endpoint crates are included for development, evaluation, API
review, and community contribution, but they should not be treated as
production-ready until they are explicitly promoted in the documentation.

## License

Eden is licensed under the Apache License, Version 2.0. See [LICENSE](./LICENSE).

## Requirements

- Rust 1.91, managed by [rust-toolchain.toml](./rust-toolchain.toml)
- Cargo
- Protocol Buffers compiler
- System libraries needed by the enabled endpoint crates, such as OpenSSL and
  CMake on common Linux distributions

On macOS:

```bash
brew install protobuf cmake openssl pkg-config
```

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler cmake libssl-dev pkg-config
```

## Build

Build the workspace:

```bash
cargo build --workspace
```

Build the main service:

```bash
cargo build -p eden-service
```

Build with the standard server feature bundle:

```bash
cargo build -p eden-service --features server-runtime
```

Build a focused Redis gateway surface:

```bash
cargo check -p eden-service --no-default-features --features redis
```

## Test And Lint

Format:

```bash
cargo fmt --all
```

Run clippy with the same strict warning policy used for this cleanup:

```bash
cargo clippy --workspace --locked --all-features --all-targets -- -D warnings
```

Run tests:

```bash
cargo test --workspace
```

Run the no-default service test compile:

```bash
cargo check -p eden-service --locked --no-default-features --tests
```

## Workspace Layout

- [eden_service](./eden_service) - HTTP service, API handlers, auth, and runtime wiring
- [eden_gateway](./eden_gateway) - gateway entry point and per-protocol gateway crates
- [eden_core](./eden_core) - shared core types, auth, telemetry, logging, and responses
- [database](./database) - persistent storage and analytics storage integrations
- [endpoint-core](./endpoint-core) - endpoint schemas, shared endpoint traits, and core protocols
- [endpoints](./endpoints) - concrete endpoint implementations
- [ep-runtime](./ep-runtime) - runtime execution layer
- [benchmark](./benchmark) - benchmark harnesses and workload tools

## Configuration

Use [eden.example.toml](./eden.example.toml) as the starting point for local
configuration. Environment variables override file values; see
[eden_config/README.md](./eden_config/README.md) for the configuration loader
rules.

Do not commit real credentials, customer data, private infrastructure details,
or machine-specific runtime files.

## Documentation

Public documentation should be kept clear, reproducible, and current. Start
with:

- [docs/README.md](./docs/README.md)
- [docs/api_docs.md](./docs/api_docs.md)
- [docs/OPENAPI.md](./docs/OPENAPI.md)
- [docs/FEATURES.md](./docs/FEATURES.md)
- [docs/BENCHMARKS.md](./docs/BENCHMARKS.md)
- [docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md)
- [docs/CODING-STYLE.md](./docs/CODING-STYLE.md)
- [docs/COMMIT-STANDARDS.md](./docs/COMMIT-STANDARDS.md)
- [docs/METRICS.md](./docs/METRICS.md)
- [docs/SECURITY_MODEL.md](./docs/SECURITY_MODEL.md)

## Contributing

Before opening a change:

1. Keep the change focused.
2. Update tests and documentation when behavior changes.
3. Run `cargo fmt --all`.
4. Run the relevant `cargo check`, `cargo test`, or `cargo clippy` command.
5. Avoid committing generated artifacts, secrets, local config, or benchmark output.
