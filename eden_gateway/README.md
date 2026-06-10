# Eden Gateway Layout

`eden_gateway` is the root gateway crate. It wires shared gateway behavior and protocol-specific crates into one runtime.

## Shared Root

- `src/`: Cargo entry point and shared root gateway plumbing only.
- `src/lib.rs`: public entry point and re-exports.
- `src/bridge.rs`, `src/processor.rs`, `src/protocol.rs`, `src/validation.rs`: shared connection, dispatch, protocol selection, and validation glue.
- `core/`: protocol-neutral gateway traits and shared runtime pieces.

## Feature Areas

- `agent.rs`: transport-neutral agent connection registry, identity fingerprints, usage windows, and gateway-side rate-limit decisions.
- `llm.rs` and `llm/`: LLM HTTP gateway adapter and support code.
- `redis/`: standalone Redis gateway crate.
- `redis/root.rs` and `redis/root/`: root-crate Redis adapter code that re-exports legacy/direct interlay entry points.
- `postgres/`: PostgreSQL gateway crate.
- `mongo/`: MongoDB gateway crate.

## Module Style

Keep protocol- or product-specific code out of `src/`. `src/` and `core/` are for shared behavior only. Put feature code in its feature area, such as `agent.rs`, `llm.rs`/`llm/`, or `redis/root.rs`/`redis/root/`. Prefer named module files and avoid new `mod.rs` files in this tree.
