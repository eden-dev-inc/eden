//! Unified cache operations trait backed by the shared internal ShardMap cache.
//!
//! Consumer code in `eden_service` should use `CacheOps` methods on
//! `DatabaseManager` instead of calling cache backend internals directly.

use eden_core::error::ResultEP;

/// String-based point-key operations provided by the internal cache.
pub trait CacheOps: Send + Sync {
    /// Set a key with a TTL in seconds.
    fn kv_set_ex(&self, key: String, value: String, ttl_secs: u64) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    /// Get a value by key, returning `None` if missing or expired.
    fn kv_get(&self, key: &str) -> impl std::future::Future<Output = ResultEP<Option<String>>> + Send;

    /// Delete a key.
    fn kv_del(&self, key: &str) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    /// Atomically get and delete a key. Returns the value before deletion.
    fn kv_get_del(&self, key: &str) -> impl std::future::Future<Output = ResultEP<Option<String>>> + Send;

    /// Refresh a key's TTL without changing its value.
    fn kv_expire(&self, key: &str, ttl_secs: u64) -> impl std::future::Future<Output = ResultEP<()>> + Send;

    /// Clear the underlying shared cache backend.
    fn clear_all(&self) -> impl std::future::Future<Output = ResultEP<()>> + Send;
}

mod backend_impl;
