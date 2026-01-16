// Storage Backends for Redis
//
// Feature-flagged storage implementations for different Redis data structures.
// Only one storage feature can be enabled at compile time.

use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Instant;
use tracing::{error, info};

#[cfg(feature = "storage-redisjson")]
use tracing::debug;

use crate::metrics::AppMetrics;

// Compile-time feature validation - exactly one storage feature must be enabled
#[cfg(not(any(
    feature = "storage-json",
    feature = "storage-hash",
    feature = "storage-list",
    feature = "storage-redisjson",
    feature = "storage-zset",
    feature = "storage-stream",
    feature = "storage-hll",
    feature = "storage-bitmap",
    feature = "storage-bloom"
)))]
compile_error!("One storage feature must be enabled");

// Count enabled features using a const fn approach
const FEATURE_COUNT: usize =
    cfg!(feature = "storage-json") as usize +
    cfg!(feature = "storage-hash") as usize +
    cfg!(feature = "storage-list") as usize +
    cfg!(feature = "storage-redisjson") as usize +
    cfg!(feature = "storage-zset") as usize +
    cfg!(feature = "storage-stream") as usize +
    cfg!(feature = "storage-hll") as usize +
    cfg!(feature = "storage-bitmap") as usize +
    cfg!(feature = "storage-bloom") as usize;

// This will fail to compile if more than one feature is enabled
const _: () = assert!(FEATURE_COUNT <= 1, "Only one storage feature can be enabled at a time");

/// Storage backend trait for Redis operations
/// Uses native async fn in traits (Rust 1.75+)
pub trait CacheStorage: Send + Sync {
    /// Get a value by key
    fn get<T>(&self, key: &str, metrics: &AppMetrics) -> impl std::future::Future<Output = Result<Option<T>>> + Send
    where
        T: DeserializeOwned + Send;

    /// Set a value with TTL
    fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> impl std::future::Future<Output = Result<()>> + Send
    where
        T: Serialize + Send + Sync;

    /// Batch set multiple entries (key, json_string, ttl)
    fn set_batch_json(
        &self,
        entries: Vec<(String, String, u64)>,
        metrics: &AppMetrics,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Increment a counter atomically
    fn incr(&self, key: &str, metrics: &AppMetrics) -> impl std::future::Future<Output = Result<i64>> + Send;

    /// Batch increment multiple counters
    fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Delete a key
    fn del(&self, key: &str, metrics: &AppMetrics) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Batch delete multiple keys
    fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Get storage type name for logging
    fn storage_type(&self) -> &'static str;
}

/// Base Redis connection pool shared by all backends
pub struct RedisConnectionPool {
    connections: Vec<MultiplexedConnection>,
    conn_count: usize,
}

impl RedisConnectionPool {
    pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
        let client = Client::open(redis_url)?;
        let conn_count = pool_size as usize;

        let mut connections = Vec::with_capacity(conn_count);
        for _ in 0..conn_count {
            let conn = client.get_multiplexed_async_connection().await?;
            connections.push(conn);
        }

        // Test first connection
        let mut test_conn = connections[0].clone();
        let _: String = redis::cmd("PING").query_async(&mut test_conn).await?;

        Ok(Self { connections, conn_count })
    }

    pub fn get_conn(&self) -> MultiplexedConnection {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % self.conn_count;
        self.connections[idx].clone()
    }
}

// ============================================================================
// JSON Storage (default) - Uses SET/GET with JSON serialization
// ============================================================================

#[cfg(feature = "storage-json")]
pub mod json_storage {
    use super::*;

    pub struct JsonStorage {
        pool: RedisConnectionPool,
    }

    impl JsonStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis JSON storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }
    }

    impl CacheStorage for JsonStorage {
        fn storage_type(&self) -> &'static str {
            "json"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.get::<_, Option<String>>(key).await {
                Ok(value) => {
                    let duration = start.elapsed().as_secs_f64();
                    let result = if value.is_some() { "hit" } else { "miss" };
                    metrics.record_cache_operation("get", result, duration);

                    match value {
                        Some(json_str) => match serde_json::from_str(&json_str) {
                            Ok(v) => Ok(Some(v)),
                            Err(e) => {
                                error!("JSON parse error for key {}: {}", key, e);
                                Err(e.into())
                            }
                        },
                        None => Ok(None),
                    }
                }
                Err(e) => {
                    error!("Redis GET error for key {}: {}", key, e);
                    metrics.record_cache_operation("get", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;

            match conn.set_ex::<_, _, ()>(key, json_str, ttl_seconds).await {
                Ok(_) => {
                    metrics.record_cache_operation("set", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis SET error for key {}: {}", key, e);
                    metrics.record_cache_operation("set", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.set_ex(key.clone(), json_str.clone(), *ttl).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_set", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch SET error: {}", e);
                    metrics.record_cache_operation("batch_set", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.incr::<_, _, i64>(key, 1).await {
                Ok(val) => {
                    metrics.record_cache_operation("incr", "success", start.elapsed().as_secs_f64());
                    Ok(val)
                }
                Err(e) => {
                    error!("Redis INCR error for key {}: {}", key, e);
                    metrics.record_cache_operation("incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.incr(key.clone(), 1i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_incr", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch INCR error: {}", e);
                    metrics.record_cache_operation("batch_incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for key {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Hash Storage - Uses HSET/HGET for structured data
// ============================================================================

#[cfg(feature = "storage-hash")]
pub mod hash_storage {
    use super::*;

    pub struct HashStorage {
        pool: RedisConnectionPool,
    }

    impl HashStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis HASH storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }

        /// Extract field name from key for hash storage
        /// e.g., "analytics:org123:overview:24h" -> ("analytics:org123:overview", "24h")
        fn split_key(key: &str) -> (&str, &str) {
            match key.rsplit_once(':') {
                Some((base, field)) => (base, field),
                None => (key, "default"),
            }
        }
    }

    impl CacheStorage for HashStorage {
        fn storage_type(&self) -> &'static str {
            "hash"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (hash_key, field) = Self::split_key(key);

            match conn.hget::<_, _, Option<String>>(hash_key, field).await {
                Ok(value) => {
                    let duration = start.elapsed().as_secs_f64();
                    let result = if value.is_some() { "hit" } else { "miss" };
                    metrics.record_cache_operation("hget", result, duration);

                    match value {
                        Some(json_str) => match serde_json::from_str(&json_str) {
                            Ok(v) => Ok(Some(v)),
                            Err(e) => {
                                error!("JSON parse error for hash key {}:{}: {}", hash_key, field, e);
                                Err(e.into())
                            }
                        },
                        None => Ok(None),
                    }
                }
                Err(e) => {
                    error!("Redis HGET error for {}:{}: {}", hash_key, field, e);
                    metrics.record_cache_operation("hget", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;
            let (hash_key, field) = Self::split_key(key);

            // Use pipeline: HSET + EXPIRE
            let mut pipe = redis::pipe();
            pipe.hset(hash_key, field, json_str).ignore();
            pipe.expire(hash_key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("hset", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis HSET error for {}:{}: {}", hash_key, field, e);
                    metrics.record_cache_operation("hset", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                let (hash_key, field) = Self::split_key(key);
                pipe.hset(hash_key, field, json_str.clone()).ignore();
                pipe.expire(hash_key, *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_hset", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch HSET error: {}", e);
                    metrics.record_cache_operation("batch_hset", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (hash_key, field) = Self::split_key(key);

            match conn.hincr::<_, _, _, i64>(hash_key, field, 1).await {
                Ok(val) => {
                    metrics.record_cache_operation("hincr", "success", start.elapsed().as_secs_f64());
                    Ok(val)
                }
                Err(e) => {
                    error!("Redis HINCRBY error for {}:{}: {}", hash_key, field, e);
                    metrics.record_cache_operation("hincr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (hash_key, field) = Self::split_key(key);
                pipe.hincr(hash_key, field, 1i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_hincr", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch HINCRBY error: {}", e);
                    metrics.record_cache_operation("batch_hincr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (hash_key, field) = Self::split_key(key);

            match conn.hdel::<_, _, i32>(hash_key, field).await {
                Ok(_) => {
                    metrics.record_cache_operation("hdel", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis HDEL error for {}:{}: {}", hash_key, field, e);
                    metrics.record_cache_operation("hdel", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (hash_key, field) = Self::split_key(key);
                pipe.hdel(hash_key, field).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_hdel", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch HDEL error: {}", e);
                    metrics.record_cache_operation("batch_hdel", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// List Storage - Uses LPUSH/LRANGE for time-series data
// ============================================================================

#[cfg(feature = "storage-list")]
pub mod list_storage {
    use super::*;

    pub struct ListStorage {
        pool: RedisConnectionPool,
        max_list_size: usize,
    }

    impl ListStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis LIST storage initialized with {} connections", pool_size);
            Ok(Self {
                pool,
                max_list_size: 1000, // Default max list size
            })
        }
    }

    impl CacheStorage for ListStorage {
        fn storage_type(&self) -> &'static str {
            "list"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // Get the most recent item from the list (index 0)
            match conn.lindex::<_, Option<String>>(key, 0).await {
                Ok(value) => {
                    let duration = start.elapsed().as_secs_f64();
                    let result = if value.is_some() { "hit" } else { "miss" };
                    metrics.record_cache_operation("lindex", result, duration);

                    match value {
                        Some(json_str) => match serde_json::from_str(&json_str) {
                            Ok(v) => Ok(Some(v)),
                            Err(e) => {
                                error!("JSON parse error for list key {}: {}", key, e);
                                Err(e.into())
                            }
                        },
                        None => Ok(None),
                    }
                }
                Err(e) => {
                    error!("Redis LINDEX error for key {}: {}", key, e);
                    metrics.record_cache_operation("lindex", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;

            // Use pipeline: LPUSH + LTRIM + EXPIRE
            let mut pipe = redis::pipe();
            pipe.lpush(key, json_str).ignore();
            pipe.ltrim(key, 0, (self.max_list_size - 1) as isize).ignore();
            pipe.expire(key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("lpush", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis LPUSH error for key {}: {}", key, e);
                    metrics.record_cache_operation("lpush", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.lpush(key.clone(), json_str.clone()).ignore();
                pipe.ltrim(key.clone(), 0, (self.max_list_size - 1) as isize).ignore();
                pipe.expire(key.clone(), *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_lpush", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch LPUSH error: {}", e);
                    metrics.record_cache_operation("batch_lpush", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // Lists don't support INCR directly, so we use regular INCR on a separate key
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let counter_key = format!("{}:counter", key);

            match conn.incr::<_, _, i64>(&counter_key, 1).await {
                Ok(val) => {
                    metrics.record_cache_operation("incr", "success", start.elapsed().as_secs_f64());
                    Ok(val)
                }
                Err(e) => {
                    error!("Redis INCR error for key {}: {}", counter_key, e);
                    metrics.record_cache_operation("incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let counter_key = format!("{}:counter", key);
                pipe.incr(counter_key, 1i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_incr", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch INCR error: {}", e);
                    metrics.record_cache_operation("batch_incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for key {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// RedisJSON Storage - Uses JSON.SET/JSON.GET from RedisJSON module
// ============================================================================

#[cfg(feature = "storage-redisjson")]
pub mod redisjson_storage {
    use super::*;

    pub struct RedisJsonStorage {
        pool: RedisConnectionPool,
    }

    impl RedisJsonStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;

            // Verify RedisJSON module is available
            let mut conn = pool.get_conn();
            match redis::cmd("MODULE").arg("LIST").query_async::<Vec<Vec<String>>>(&mut conn).await {
                Ok(modules) => {
                    let has_json = modules.iter().any(|m| {
                        m.iter().any(|s| s.to_lowercase().contains("rejson") || s.to_lowercase().contains("redisjson"))
                    });
                    if !has_json {
                        debug!("RedisJSON module not detected, JSON commands may fail");
                    }
                }
                Err(e) => {
                    debug!("Could not check for RedisJSON module: {}", e);
                }
            }

            info!("Redis JSON (RedisJSON) storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }
    }

    impl CacheStorage for RedisJsonStorage {
        fn storage_type(&self) -> &'static str {
            "redisjson"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // JSON.GET key $ returns array with single element
            match redis::cmd("JSON.GET")
                .arg(key)
                .arg("$")
                .query_async::<Option<String>>(&mut conn)
                .await
            {
                Ok(value) => {
                    let duration = start.elapsed().as_secs_f64();
                    let result = if value.is_some() { "hit" } else { "miss" };
                    metrics.record_cache_operation("json.get", result, duration);

                    match value {
                        Some(json_str) => {
                            // JSON.GET returns an array, extract the first element
                            let parsed: Result<Vec<T>, _> = serde_json::from_str(&json_str);
                            match parsed {
                                Ok(mut arr) if !arr.is_empty() => Ok(Some(arr.remove(0))),
                                Ok(_) => Ok(None),
                                Err(e) => {
                                    error!("JSON parse error for key {}: {}", key, e);
                                    Err(e.into())
                                }
                            }
                        }
                        None => Ok(None),
                    }
                }
                Err(e) => {
                    // Check if it's a "key doesn't exist" error
                    let err_str = e.to_string();
                    if err_str.contains("not exist") || err_str.contains("nil") {
                        metrics.record_cache_operation("json.get", "miss", start.elapsed().as_secs_f64());
                        return Ok(None);
                    }
                    error!("Redis JSON.GET error for key {}: {}", key, e);
                    metrics.record_cache_operation("json.get", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;

            // Use pipeline: JSON.SET + EXPIRE
            let mut pipe = redis::pipe();
            pipe.cmd("JSON.SET").arg(key).arg("$").arg(&json_str).ignore();
            pipe.expire(key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("json.set", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis JSON.SET error for key {}: {}", key, e);
                    metrics.record_cache_operation("json.set", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.cmd("JSON.SET").arg(key.clone()).arg("$").arg(json_str.clone()).ignore();
                pipe.expire(key.clone(), *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_json.set", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch JSON.SET error: {}", e);
                    metrics.record_cache_operation("batch_json.set", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // RedisJSON has JSON.NUMINCRBY but we use regular INCR for counters
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.incr::<_, _, i64>(key, 1).await {
                Ok(val) => {
                    metrics.record_cache_operation("incr", "success", start.elapsed().as_secs_f64());
                    Ok(val)
                }
                Err(e) => {
                    error!("Redis INCR error for key {}: {}", key, e);
                    metrics.record_cache_operation("incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.incr(key.clone(), 1i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_incr", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch INCR error: {}", e);
                    metrics.record_cache_operation("batch_incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for key {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Sorted Set Storage - Uses ZADD/ZRANGE for rankings and leaderboards
// ============================================================================

#[cfg(feature = "storage-zset")]
pub mod zset_storage {
    use super::*;

    pub struct ZSetStorage {
        pool: RedisConnectionPool,
    }

    impl ZSetStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis Sorted Set (ZSET) storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }

        /// Extract member and score from key
        /// e.g., "leaderboard:org123:user456" -> ("leaderboard:org123", "user456")
        fn split_key(key: &str) -> (&str, &str) {
            match key.rsplit_once(':') {
                Some((base, member)) => (base, member),
                None => (key, "default"),
            }
        }
    }

    impl CacheStorage for ZSetStorage {
        fn storage_type(&self) -> &'static str {
            "zset"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (zset_key, member) = Self::split_key(key);

            // Get the JSON stored as member's associated data via a companion hash
            let data_key = format!("{}:data", zset_key);
            match conn.hget::<_, _, Option<String>>(&data_key, member).await {
                Ok(value) => {
                    let duration = start.elapsed().as_secs_f64();
                    let result = if value.is_some() { "hit" } else { "miss" };
                    metrics.record_cache_operation("zset_get", result, duration);

                    match value {
                        Some(json_str) => match serde_json::from_str(&json_str) {
                            Ok(v) => Ok(Some(v)),
                            Err(e) => {
                                error!("JSON parse error for zset {}:{}: {}", zset_key, member, e);
                                Err(e.into())
                            }
                        },
                        None => Ok(None),
                    }
                }
                Err(e) => {
                    error!("Redis ZSET GET error for {}:{}: {}", zset_key, member, e);
                    metrics.record_cache_operation("zset_get", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;
            let (zset_key, member) = Self::split_key(key);
            let score = chrono::Utc::now().timestamp_millis() as f64;

            // Store in sorted set (for ordering) and hash (for data)
            let data_key = format!("{}:data", zset_key);
            let mut pipe = redis::pipe();
            pipe.zadd(zset_key, member, score).ignore();
            pipe.hset(&data_key, member, json_str).ignore();
            pipe.expire(zset_key, ttl_seconds as i64).ignore();
            pipe.expire(&data_key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("zadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis ZADD error for {}:{}: {}", zset_key, member, e);
                    metrics.record_cache_operation("zadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let base_score = chrono::Utc::now().timestamp_millis() as f64;

            let mut pipe = redis::pipe();
            for (i, (key, json_str, ttl)) in entries.iter().enumerate() {
                let (zset_key, member) = Self::split_key(key);
                let data_key = format!("{}:data", zset_key);
                let score = base_score + i as f64;

                pipe.zadd(zset_key, member, score).ignore();
                pipe.hset(&data_key, member, json_str.clone()).ignore();
                pipe.expire(zset_key, *ttl as i64).ignore();
                pipe.expire(&data_key, *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_zadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch ZADD error: {}", e);
                    metrics.record_cache_operation("batch_zadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (zset_key, member) = Self::split_key(key);

            // ZINCRBY increments the score
            match redis::cmd("ZINCRBY")
                .arg(zset_key)
                .arg(1.0f64)
                .arg(member)
                .query_async::<f64>(&mut conn)
                .await
            {
                Ok(val) => {
                    metrics.record_cache_operation("zincrby", "success", start.elapsed().as_secs_f64());
                    Ok(val as i64)
                }
                Err(e) => {
                    error!("Redis ZINCRBY error for {}:{}: {}", zset_key, member, e);
                    metrics.record_cache_operation("zincrby", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (zset_key, member) = Self::split_key(key);
                pipe.cmd("ZINCRBY").arg(zset_key).arg(1.0f64).arg(member).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_zincrby", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch ZINCRBY error: {}", e);
                    metrics.record_cache_operation("batch_zincrby", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (zset_key, member) = Self::split_key(key);
            let data_key = format!("{}:data", zset_key);

            let mut pipe = redis::pipe();
            pipe.zrem(zset_key, member).ignore();
            pipe.hdel(&data_key, member).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("zrem", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis ZREM error for {}:{}: {}", zset_key, member, e);
                    metrics.record_cache_operation("zrem", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (zset_key, member) = Self::split_key(key);
                let data_key = format!("{}:data", zset_key);
                pipe.zrem(zset_key, member).ignore();
                pipe.hdel(&data_key, member).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_zrem", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch ZREM error: {}", e);
                    metrics.record_cache_operation("batch_zrem", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Stream Storage - Uses XADD/XREAD for event sourcing
// ============================================================================

#[cfg(feature = "storage-stream")]
pub mod stream_storage {
    use super::*;

    pub struct StreamStorage {
        pool: RedisConnectionPool,
        max_stream_len: usize,
    }

    impl StreamStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis Stream storage initialized with {} connections", pool_size);
            Ok(Self {
                pool,
                max_stream_len: 10000, // Default max stream length
            })
        }
    }

    impl CacheStorage for StreamStorage {
        fn storage_type(&self) -> &'static str {
            "stream"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // XREVRANGE to get most recent entry
            match redis::cmd("XREVRANGE")
                .arg(key)
                .arg("+")
                .arg("-")
                .arg("COUNT")
                .arg(1)
                .query_async::<Vec<(String, Vec<(String, String)>)>>(&mut conn)
                .await
            {
                Ok(entries) => {
                    let duration = start.elapsed().as_secs_f64();
                    if entries.is_empty() {
                        metrics.record_cache_operation("xrevrange", "miss", duration);
                        return Ok(None);
                    }

                    metrics.record_cache_operation("xrevrange", "hit", duration);

                    // Extract the "data" field from the entry
                    let (_id, fields) = &entries[0];
                    for (field_name, field_value) in fields {
                        if field_name == "data" {
                            match serde_json::from_str(field_value) {
                                Ok(v) => return Ok(Some(v)),
                                Err(e) => {
                                    error!("JSON parse error for stream {}: {}", key, e);
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    Ok(None)
                }
                Err(e) => {
                    error!("Redis XREVRANGE error for {}: {}", key, e);
                    metrics.record_cache_operation("xrevrange", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;

            // XADD with MAXLEN for bounded streams
            let mut pipe = redis::pipe();
            pipe.cmd("XADD")
                .arg(key)
                .arg("MAXLEN")
                .arg("~")
                .arg(self.max_stream_len)
                .arg("*")
                .arg("data")
                .arg(&json_str)
                .ignore();
            pipe.expire(key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("xadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis XADD error for {}: {}", key, e);
                    metrics.record_cache_operation("xadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.cmd("XADD")
                    .arg(key.clone())
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(self.max_stream_len)
                    .arg("*")
                    .arg("data")
                    .arg(json_str.clone())
                    .ignore();
                pipe.expire(key.clone(), *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_xadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch XADD error: {}", e);
                    metrics.record_cache_operation("batch_xadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // Streams don't have native INCR, use separate counter
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let counter_key = format!("{}:counter", key);

            match conn.incr::<_, _, i64>(&counter_key, 1).await {
                Ok(val) => {
                    metrics.record_cache_operation("incr", "success", start.elapsed().as_secs_f64());
                    Ok(val)
                }
                Err(e) => {
                    error!("Redis INCR error for {}: {}", counter_key, e);
                    metrics.record_cache_operation("incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let counter_key = format!("{}:counter", key);
                pipe.incr(counter_key, 1i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_incr", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch INCR error: {}", e);
                    metrics.record_cache_operation("batch_incr", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// HyperLogLog Storage - Uses PFADD/PFCOUNT for cardinality estimation
// ============================================================================

#[cfg(feature = "storage-hll")]
pub mod hll_storage {
    use super::*;

    pub struct HllStorage {
        pool: RedisConnectionPool,
    }

    impl HllStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis HyperLogLog storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }
    }

    impl CacheStorage for HllStorage {
        fn storage_type(&self) -> &'static str {
            "hll"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // For HLL, "get" returns the cardinality count as JSON
            match redis::cmd("PFCOUNT")
                .arg(key)
                .query_async::<i64>(&mut conn)
                .await
            {
                Ok(count) => {
                    let duration = start.elapsed().as_secs_f64();
                    if count == 0 {
                        metrics.record_cache_operation("pfcount", "miss", duration);
                        return Ok(None);
                    }
                    metrics.record_cache_operation("pfcount", "hit", duration);

                    // Return count wrapped in JSON
                    let json_str = format!(r#"{{"count":{}}}"#, count);
                    match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("JSON parse error for HLL {}: {}", key, e);
                            Err(e.into())
                        }
                    }
                }
                Err(e) => {
                    error!("Redis PFCOUNT error for {}: {}", key, e);
                    metrics.record_cache_operation("pfcount", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // For HLL, we add the serialized value as an element to count
            let json_str = serde_json::to_string(value)?;

            let mut pipe = redis::pipe();
            pipe.cmd("PFADD").arg(key).arg(&json_str).ignore();
            pipe.expire(key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("pfadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis PFADD error for {}: {}", key, e);
                    metrics.record_cache_operation("pfadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.cmd("PFADD").arg(key.clone()).arg(json_str.clone()).ignore();
                pipe.expire(key.clone(), *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_pfadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch PFADD error: {}", e);
                    metrics.record_cache_operation("batch_pfadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // HLL doesn't have INCR, return current count
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match redis::cmd("PFCOUNT")
                .arg(key)
                .query_async::<i64>(&mut conn)
                .await
            {
                Ok(count) => {
                    metrics.record_cache_operation("pfcount", "success", start.elapsed().as_secs_f64());
                    Ok(count)
                }
                Err(e) => {
                    error!("Redis PFCOUNT error for {}: {}", key, e);
                    metrics.record_cache_operation("pfcount", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            // For HLL batch "incr", we add a unique element to each
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let unique_element = uuid::Uuid::new_v4().to_string();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.cmd("PFADD").arg(key.clone()).arg(&unique_element).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_pfadd", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch PFADD error: {}", e);
                    metrics.record_cache_operation("batch_pfadd", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Bitmap Storage - Uses SETBIT/GETBIT for compact boolean tracking
// ============================================================================

#[cfg(feature = "storage-bitmap")]
pub mod bitmap_storage {
    use super::*;

    pub struct BitmapStorage {
        pool: RedisConnectionPool,
    }

    impl BitmapStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;
            info!("Redis Bitmap storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }

        /// Extract bitmap key and offset from key
        /// e.g., "user:active:2024-01-15:12345" -> ("user:active:2024-01-15", 12345)
        fn split_key(key: &str) -> (&str, u32) {
            match key.rsplit_once(':') {
                Some((base, offset_str)) => {
                    let offset = offset_str.parse::<u32>().unwrap_or(0);
                    (base, offset)
                }
                None => (key, 0),
            }
        }
    }

    impl CacheStorage for BitmapStorage {
        fn storage_type(&self) -> &'static str {
            "bitmap"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (bitmap_key, offset) = Self::split_key(key);

            match redis::cmd("GETBIT")
                .arg(bitmap_key)
                .arg(offset)
                .query_async::<i32>(&mut conn)
                .await
            {
                Ok(bit) => {
                    let duration = start.elapsed().as_secs_f64();
                    metrics.record_cache_operation("getbit", "hit", duration);

                    // Return bit value as JSON
                    let json_str = format!(r#"{{"value":{}}}"#, bit);
                    match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("JSON parse error for bitmap {}:{}: {}", bitmap_key, offset, e);
                            Err(e.into())
                        }
                    }
                }
                Err(e) => {
                    error!("Redis GETBIT error for {}:{}: {}", bitmap_key, offset, e);
                    metrics.record_cache_operation("getbit", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, _value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (bitmap_key, offset) = Self::split_key(key);

            // Set bit to 1
            let mut pipe = redis::pipe();
            pipe.cmd("SETBIT").arg(bitmap_key).arg(offset).arg(1).ignore();
            pipe.expire(bitmap_key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("setbit", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis SETBIT error for {}:{}: {}", bitmap_key, offset, e);
                    metrics.record_cache_operation("setbit", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, _json_str, ttl) in &entries {
                let (bitmap_key, offset) = Self::split_key(key);
                pipe.cmd("SETBIT").arg(bitmap_key).arg(offset).arg(1).ignore();
                pipe.expire(bitmap_key, *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_setbit", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch SETBIT error: {}", e);
                    metrics.record_cache_operation("batch_setbit", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // BITCOUNT returns number of set bits
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (bitmap_key, _) = Self::split_key(key);

            match redis::cmd("BITCOUNT")
                .arg(bitmap_key)
                .query_async::<i64>(&mut conn)
                .await
            {
                Ok(count) => {
                    metrics.record_cache_operation("bitcount", "success", start.elapsed().as_secs_f64());
                    Ok(count)
                }
                Err(e) => {
                    error!("Redis BITCOUNT error for {}: {}", bitmap_key, e);
                    metrics.record_cache_operation("bitcount", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            // Set bits for all keys
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (bitmap_key, offset) = Self::split_key(key);
                pipe.cmd("SETBIT").arg(bitmap_key).arg(offset).arg(1).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_setbit", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch SETBIT error: {}", e);
                    metrics.record_cache_operation("batch_setbit", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let (bitmap_key, offset) = Self::split_key(key);

            // Set bit to 0
            match redis::cmd("SETBIT")
                .arg(bitmap_key)
                .arg(offset)
                .arg(0)
                .query_async::<i32>(&mut conn)
                .await
            {
                Ok(_) => {
                    metrics.record_cache_operation("setbit_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis SETBIT (del) error for {}:{}: {}", bitmap_key, offset, e);
                    metrics.record_cache_operation("setbit_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                let (bitmap_key, offset) = Self::split_key(key);
                pipe.cmd("SETBIT").arg(bitmap_key).arg(offset).arg(0).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_setbit_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch SETBIT (del) error: {}", e);
                    metrics.record_cache_operation("batch_setbit_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Bloom Filter Storage - Uses BF.ADD/BF.EXISTS from RedisBloom module
// ============================================================================

#[cfg(feature = "storage-bloom")]
pub mod bloom_storage {
    use super::*;

    pub struct BloomStorage {
        pool: RedisConnectionPool,
    }

    impl BloomStorage {
        pub async fn new(redis_url: &str, pool_size: u32) -> Result<Self> {
            let pool = RedisConnectionPool::new(redis_url, pool_size).await?;

            // Check if RedisBloom module is available
            let mut conn = pool.get_conn();
            match redis::cmd("MODULE").arg("LIST").query_async::<Vec<Vec<String>>>(&mut conn).await {
                Ok(modules) => {
                    let has_bloom = modules.iter().any(|m| {
                        m.iter().any(|s| s.to_lowercase().contains("bloom") || s.to_lowercase().contains("bf"))
                    });
                    if !has_bloom {
                        info!("RedisBloom module not detected, BF commands may fail");
                    }
                }
                Err(e) => {
                    info!("Could not check for RedisBloom module: {}", e);
                }
            }

            info!("Redis Bloom Filter storage initialized with {} connections", pool_size);
            Ok(Self { pool })
        }
    }

    impl CacheStorage for BloomStorage {
        fn storage_type(&self) -> &'static str {
            "bloom"
        }

        async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
        where
            T: DeserializeOwned + Send,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            // For bloom filters, we check if any element exists
            // Use BF.INFO to get filter stats
            match redis::cmd("BF.INFO")
                .arg(key)
                .query_async::<Vec<(String, i64)>>(&mut conn)
                .await
            {
                Ok(info) => {
                    let duration = start.elapsed().as_secs_f64();
                    metrics.record_cache_operation("bf.info", "hit", duration);

                    // Build JSON from info
                    let mut items_inserted = 0i64;
                    let mut capacity = 0i64;
                    for (field, value) in &info {
                        match field.as_str() {
                            "Number of items inserted" => items_inserted = *value,
                            "Capacity" => capacity = *value,
                            _ => {}
                        }
                    }

                    let json_str = format!(r#"{{"items_inserted":{},"capacity":{}}}"#, items_inserted, capacity);
                    match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("JSON parse error for bloom {}: {}", key, e);
                            Err(e.into())
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("not exist") || err_str.contains("ERR not found") {
                        metrics.record_cache_operation("bf.info", "miss", start.elapsed().as_secs_f64());
                        return Ok(None);
                    }
                    error!("Redis BF.INFO error for {}: {}", key, e);
                    metrics.record_cache_operation("bf.info", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set<T>(&self, key: &str, value: &T, ttl_seconds: u64, metrics: &AppMetrics) -> Result<()>
        where
            T: Serialize + Send + Sync,
        {
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let json_str = serde_json::to_string(value)?;

            // BF.ADD adds element to bloom filter (creates if doesn't exist)
            let mut pipe = redis::pipe();
            pipe.cmd("BF.ADD").arg(key).arg(&json_str).ignore();
            pipe.expire(key, ttl_seconds as i64).ignore();

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("bf.add", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis BF.ADD error for {}: {}", key, e);
                    metrics.record_cache_operation("bf.add", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn set_batch_json(
            &self,
            entries: Vec<(String, String, u64)>,
            metrics: &AppMetrics,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for (key, json_str, ttl) in &entries {
                pipe.cmd("BF.ADD").arg(key.clone()).arg(json_str.clone()).ignore();
                pipe.expire(key.clone(), *ttl as i64).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_bf.add", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch BF.ADD error: {}", e);
                    metrics.record_cache_operation("batch_bf.add", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
            // Bloom filters don't have INCR, add a unique element
            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let unique_element = uuid::Uuid::new_v4().to_string();

            match redis::cmd("BF.ADD")
                .arg(key)
                .arg(&unique_element)
                .query_async::<i32>(&mut conn)
                .await
            {
                Ok(added) => {
                    metrics.record_cache_operation("bf.add", "success", start.elapsed().as_secs_f64());
                    Ok(added as i64)
                }
                Err(e) => {
                    error!("Redis BF.ADD error for {}: {}", key, e);
                    metrics.record_cache_operation("bf.add", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();
            let unique_element = uuid::Uuid::new_v4().to_string();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.cmd("BF.ADD").arg(key.clone()).arg(&unique_element).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_bf.add", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch BF.ADD error: {}", e);
                    metrics.record_cache_operation("batch_bf.add", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
            // Bloom filters can't remove elements, delete entire filter
            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            match conn.del::<_, i32>(key).await {
                Ok(_) => {
                    metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis DEL error for bloom {}: {}", key, e);
                    metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }

        async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }

            let start = Instant::now();
            let mut conn = self.pool.get_conn();

            let mut pipe = redis::pipe();
            for key in keys {
                pipe.del(key.clone()).ignore();
            }

            match pipe.query_async::<()>(&mut conn).await {
                Ok(_) => {
                    metrics.record_cache_operation("batch_del", "success", start.elapsed().as_secs_f64());
                    Ok(())
                }
                Err(e) => {
                    error!("Redis batch DEL error: {}", e);
                    metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                    Err(e.into())
                }
            }
        }
    }
}

// ============================================================================
// Storage Type Alias - Compile-time selected storage backend
// ============================================================================

#[cfg(feature = "storage-json")]
pub type Storage = json_storage::JsonStorage;

#[cfg(feature = "storage-hash")]
pub type Storage = hash_storage::HashStorage;

#[cfg(feature = "storage-list")]
pub type Storage = list_storage::ListStorage;

#[cfg(feature = "storage-redisjson")]
pub type Storage = redisjson_storage::RedisJsonStorage;

#[cfg(feature = "storage-zset")]
pub type Storage = zset_storage::ZSetStorage;

#[cfg(feature = "storage-stream")]
pub type Storage = stream_storage::StreamStorage;

#[cfg(feature = "storage-hll")]
pub type Storage = hll_storage::HllStorage;

#[cfg(feature = "storage-bitmap")]
pub type Storage = bitmap_storage::BitmapStorage;

#[cfg(feature = "storage-bloom")]
pub type Storage = bloom_storage::BloomStorage;

/// Create storage backend based on enabled feature
pub async fn create_storage(redis_url: &str, pool_size: u32) -> Result<Storage> {
    Storage::new(redis_url, pool_size).await
}

/// Get the name of the active storage type
pub fn active_storage_type() -> &'static str {
    #[cfg(feature = "storage-json")]
    return "json";

    #[cfg(feature = "storage-hash")]
    return "hash";

    #[cfg(feature = "storage-list")]
    return "list";

    #[cfg(feature = "storage-redisjson")]
    return "redisjson";

    #[cfg(feature = "storage-zset")]
    return "zset";

    #[cfg(feature = "storage-stream")]
    return "stream";

    #[cfg(feature = "storage-hll")]
    return "hll";

    #[cfg(feature = "storage-bitmap")]
    return "bitmap";

    #[cfg(feature = "storage-bloom")]
    return "bloom";
}
