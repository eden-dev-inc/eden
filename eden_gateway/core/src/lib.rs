pub mod audit;
pub mod connection;
pub mod response;
pub mod shard_capacity;
pub mod shard_dispatch;
pub mod traits;

/// Runtime-affinity helpers (`mark_shard_thread`, `is_shard_runtime`,
/// `spawn_on_current_runtime`). The implementation lives in `ep-core`
/// so `redis-core`'s multiplexer dispatch path can read the same
/// thread-local marker as the higher-level gateway code; this re-export
/// keeps the existing `eden_gateway_core::runtime` import path working
/// for callers in `eden_gateway`, `eden_service`, etc.
pub use ep_core::runtime;
