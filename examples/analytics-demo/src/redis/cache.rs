// Redis Cache Layer
//
// RedisCache using multiple MultiplexedConnection instances.
// Each MultiplexedConnection handles pipelining internally, but having multiple
// connections allows better parallelism across workers.
// Supports multiple Redis command types via feature flags.

use anyhow::Result;
#[cfg(feature = "redis-sorted-set")]
use chrono::Utc;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use std::time::Instant;
use tracing::{error, info, warn};

use crate::metrics::AppMetrics;

/// RedisCache using multiple MultiplexedConnection instances
/// Each MultiplexedConnection handles pipelining internally, but having multiple
/// connections allows better parallelism across workers
pub struct RedisCache {
    connections: Vec<MultiplexedConnection>,
    conn_count: usize,
}

impl RedisCache {
    /// Create multiple Redis connections for parallel access
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

        info!(
            "Redis established with {} multiplexed connections",
            conn_count
        );
        Ok(Self {
            connections,
            conn_count,
        })
    }

    /// Get a connection using simple round-robin based on current thread/task
    fn get_conn(&self) -> MultiplexedConnection {
        // Use thread-local counter for distribution
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % self.conn_count;
        self.connections[idx].clone()
    }

    // ============================================================
    // String Commands (default) - Uses GET/SET with JSON serialization
    // ============================================================
    #[cfg(feature = "redis-string")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn.get::<_, Option<String>>(key).await {
            Ok(value) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if value.is_some() { "hit" } else { "miss" };
                metrics.record_cache_operation("string_get", result, duration);

                match value {
                    Some(json_str) => match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            let preview: String = json_str.chars().take(200).collect();
                            error!(
                                "JSON parse error for key {}: {} (raw value [{}B]: {:?})",
                                key,
                                e,
                                json_str.len(),
                                preview
                            );
                            Err(e.into())
                        }
                    },
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Redis GET error for key {}: {}", key, e);
                metrics.record_cache_operation(
                    "string_get",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-string")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        match conn.set_ex::<_, _, ()>(key, json_str, ttl_seconds).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "string_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis SET error for key {}: {}", key, e);
                metrics.record_cache_operation(
                    "string_set",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    // ============================================================
    // JSON Commands - Uses RedisJSON module (JSON.GET/JSON.SET)
    // Requires Redis Stack or RedisJSON module installed
    // ============================================================
    #[cfg(feature = "redis-json")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        // JSON.GET key $ returns the value at the root path
        match redis::cmd("JSON.GET")
            .arg(key)
            .arg("$")
            .query_async::<Option<String>>(&mut conn)
            .await
        {
            Ok(value) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if value.is_some() { "hit" } else { "miss" };
                metrics.record_cache_operation("json_get", result, duration);

                match value {
                    Some(json_str) => {
                        // JSON.GET with $ path returns an array, extract first element
                        let parsed: Vec<T> = serde_json::from_str(&json_str)?;
                        Ok(parsed.into_iter().next())
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Redis JSON.GET error for key {}: {}", key, e);
                metrics.record_cache_operation("json_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-json")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        // JSON.SET key $ value
        match redis::cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(&json_str)
            .query_async::<()>(&mut conn)
            .await
        {
            Ok(_) => {
                // Set TTL separately using EXPIRE
                let _: () = conn.expire(key, ttl_seconds as i64).await?;
                metrics.record_cache_operation(
                    "json_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis JSON.SET error for key {}: {}", key, e);
                metrics.record_cache_operation("json_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    // ============================================================
    // Hash Commands - Uses HSET/HGET/HGETALL for structured data
    // Stores each field of the struct as a hash field
    // ============================================================
    #[cfg(feature = "redis-hash")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        // HGET the serialized JSON from "data" field
        match conn.hget::<_, _, Option<String>>(key, "data").await {
            Ok(value) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if value.is_some() { "hit" } else { "miss" };
                metrics.record_cache_operation("hash_get", result, duration);

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
                error!("Redis HGET error for key {}: {}", key, e);
                metrics.record_cache_operation("hash_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-hash")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        // HSET key "data" json_str
        match conn.hset::<_, _, _, ()>(key, "data", &json_str).await {
            Ok(_) => {
                // Set TTL using EXPIRE
                let _: () = conn.expire(key, ttl_seconds as i64).await?;
                metrics.record_cache_operation(
                    "hash_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis HSET error for key {}: {}", key, e);
                metrics.record_cache_operation("hash_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    // ============================================================
    // List Commands - Uses LPUSH/LRANGE for queue-like caching
    // Useful for time-series data or event logs
    // ============================================================
    #[cfg(feature = "redis-list")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        // LINDEX key 0 - get the most recent item (head of list)
        match conn.lindex::<_, Option<String>>(key, 0).await {
            Ok(value) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if value.is_some() { "hit" } else { "miss" };
                metrics.record_cache_operation("list_get", result, duration);

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
                error!("Redis LINDEX error for key {}: {}", key, e);
                metrics.record_cache_operation("list_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-list")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        // Delete existing list and push new value
        let mut pipe = redis::pipe();
        pipe.del(key).ignore();
        pipe.lpush(key, &json_str).ignore();
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "list_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis LPUSH error for key {}: {}", key, e);
                metrics.record_cache_operation("list_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// List-specific: Push to list without clearing (append to history)
    #[cfg(feature = "redis-list")]
    pub async fn list_push<T>(
        &self,
        key: &str,
        value: &T,
        max_len: i64,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        let mut pipe = redis::pipe();
        pipe.lpush(key, &json_str).ignore();
        pipe.ltrim(key, 0, (max_len - 1) as isize).ignore(); // Keep only max_len items
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "list_push",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis list_push error for key {}: {}", key, e);
                metrics.record_cache_operation("list_push", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// List-specific: Get all items in the list
    #[cfg(feature = "redis-list")]
    pub async fn list_get_all<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn.lrange::<_, Vec<String>>(key, 0, -1).await {
            Ok(values) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if !values.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation("list_get_all", result, duration);

                let mut items = Vec::with_capacity(values.len());
                for json_str in values {
                    match serde_json::from_str(&json_str) {
                        Ok(v) => items.push(v),
                        Err(e) => {
                            error!("JSON parse error in list for key {}: {}", key, e);
                        }
                    }
                }
                Ok(items)
            }
            Err(e) => {
                error!("Redis LRANGE error for key {}: {}", key, e);
                metrics.record_cache_operation(
                    "list_get_all",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    // ============================================================
    // Set Commands - Uses SADD/SMEMBERS for unique collections
    // Useful for tracking unique visitors, tags, etc.
    // ============================================================
    #[cfg(feature = "redis-set")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        // For sets, we store the JSON in a single member for compatibility
        match conn.smembers::<_, Vec<String>>(key).await {
            Ok(members) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if !members.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation("set_get", result, duration);

                if let Some(json_str) = members.into_iter().next() {
                    match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("JSON parse error for key {}: {}", key, e);
                            Err(e.into())
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                error!("Redis SMEMBERS error for key {}: {}", key, e);
                metrics.record_cache_operation("set_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-set")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        // Clear set and add new value
        let mut pipe = redis::pipe();
        pipe.del(key).ignore();
        pipe.sadd(key, &json_str).ignore();
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation("set_set", "success", start.elapsed().as_secs_f64());
                Ok(())
            }
            Err(e) => {
                error!("Redis SADD error for key {}: {}", key, e);
                metrics.record_cache_operation("set_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Set-specific: Add a member to the set
    #[cfg(feature = "redis-set")]
    pub async fn set_add(
        &self,
        key: &str,
        member: &str,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<bool> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        let mut pipe = redis::pipe();
        pipe.sadd(key, member);
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<(i32,)>(&mut conn).await {
            Ok((added,)) => {
                metrics.record_cache_operation("set_add", "success", start.elapsed().as_secs_f64());
                Ok(added > 0)
            }
            Err(e) => {
                error!("Redis SADD error for key {}: {}", key, e);
                metrics.record_cache_operation("set_add", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Set-specific: Get all members of the set
    #[cfg(feature = "redis-set")]
    pub async fn set_members(&self, key: &str, metrics: &AppMetrics) -> Result<Vec<String>> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn.smembers::<_, Vec<String>>(key).await {
            Ok(members) => {
                let result = if !members.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation(
                    "set_members",
                    result,
                    start.elapsed().as_secs_f64(),
                );
                Ok(members)
            }
            Err(e) => {
                error!("Redis SMEMBERS error for key {}: {}", key, e);
                metrics.record_cache_operation(
                    "set_members",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    /// Set-specific: Get cardinality (number of members)
    #[cfg(feature = "redis-set")]
    pub async fn set_card(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn.scard::<_, i64>(key).await {
            Ok(count) => {
                metrics.record_cache_operation(
                    "set_card",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(count)
            }
            Err(e) => {
                error!("Redis SCARD error for key {}: {}", key, e);
                metrics.record_cache_operation("set_card", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    // ============================================================
    // Sorted Set Commands - Uses ZADD/ZRANGE for ranked/scored data
    // Useful for leaderboards, time-series with scores, etc.
    // ============================================================
    #[cfg(feature = "redis-sorted-set")]
    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();

        // Get the highest scored member
        match conn.zrevrange::<_, Vec<String>>(key, 0, 0).await {
            Ok(members) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if !members.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation("zset_get", result, duration);

                if let Some(json_str) = members.into_iter().next() {
                    match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("JSON parse error for key {}: {}", key, e);
                            Err(e.into())
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                error!("Redis ZREVRANGE error for key {}: {}", key, e);
                metrics.record_cache_operation("zset_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[cfg(feature = "redis-sorted-set")]
    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;
        let score = Utc::now().timestamp_millis() as f64;

        // Clear and add with timestamp as score
        let mut pipe = redis::pipe();
        pipe.del(key).ignore();
        pipe.zadd(key, &json_str, score).ignore();
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "zset_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis ZADD error for key {}: {}", key, e);
                metrics.record_cache_operation("zset_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Sorted set-specific: Add member with custom score
    #[cfg(feature = "redis-sorted-set")]
    pub async fn zset_add(
        &self,
        key: &str,
        member: &str,
        score: f64,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<()> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        let mut pipe = redis::pipe();
        pipe.zadd(key, member, score).ignore();
        pipe.expire(key, ttl_seconds as i64).ignore();

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "zset_add",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis ZADD error for key {}: {}", key, e);
                metrics.record_cache_operation("zset_add", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Sorted set-specific: Get top N members by score (descending)
    #[cfg(feature = "redis-sorted-set")]
    pub async fn zset_top(
        &self,
        key: &str,
        count: i64,
        metrics: &AppMetrics,
    ) -> Result<Vec<(String, f64)>> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn
            .zrevrange_withscores::<_, Vec<(String, f64)>>(key, 0, (count - 1) as isize)
            .await
        {
            Ok(members) => {
                let result = if !members.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation("zset_top", result, start.elapsed().as_secs_f64());
                Ok(members)
            }
            Err(e) => {
                error!("Redis ZREVRANGE error for key {}: {}", key, e);
                metrics.record_cache_operation("zset_top", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Sorted set-specific: Get members within score range
    #[cfg(feature = "redis-sorted-set")]
    pub async fn zset_range_by_score(
        &self,
        key: &str,
        min: f64,
        max: f64,
        metrics: &AppMetrics,
    ) -> Result<Vec<(String, f64)>> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn
            .zrangebyscore_withscores::<_, _, _, Vec<(String, f64)>>(key, min, max)
            .await
        {
            Ok(members) => {
                let result = if !members.is_empty() { "hit" } else { "miss" };
                metrics.record_cache_operation("zset_range", result, start.elapsed().as_secs_f64());
                Ok(members)
            }
            Err(e) => {
                error!("Redis ZRANGEBYSCORE error for key {}: {}", key, e);
                metrics.record_cache_operation(
                    "zset_range",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    /// Sorted set-specific: Increment score of a member
    #[cfg(feature = "redis-sorted-set")]
    pub async fn zset_incr(
        &self,
        key: &str,
        member: &str,
        increment: f64,
        ttl_seconds: u64,
        metrics: &AppMetrics,
    ) -> Result<f64> {
        let start = Instant::now();
        let mut conn = self.get_conn();

        match conn.zincr::<_, _, _, f64>(key, member, increment).await {
            Ok(new_score) => {
                let _: () = conn.expire(key, ttl_seconds as i64).await?;
                metrics.record_cache_operation(
                    "zset_incr",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(new_score)
            }
            Err(e) => {
                error!("Redis ZINCRBY error for key {}: {}", key, e);
                metrics.record_cache_operation("zset_incr", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Batch set multiple keys using Redis pipelining
    /// Accepts pre-serialized JSON strings for mixed types
    pub async fn set_batch_json(
        &self,
        entries: Vec<(String, String, u64)>, // (key, json_string, ttl)
        metrics: &AppMetrics,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut conn = self.get_conn();

        // Build pipeline
        let mut pipe = redis::pipe();
        for (key, json_str, ttl) in &entries {
            pipe.set_ex(key.clone(), json_str.clone(), *ttl).ignore();
        }

        // Execute pipeline - MultiplexedConnection implements ConnectionLike
        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "batch_set",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis batch SET error: {}", e);
                metrics.record_cache_operation("batch_set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// Increment a counter atomically
    #[allow(dead_code)]
    pub async fn incr(&self, key: &str, metrics: &AppMetrics) -> Result<i64> {
        let start = Instant::now();
        let mut conn = self.get_conn();

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

    /// Batch increment multiple counters using pipelining
    #[allow(dead_code)]
    pub async fn incr_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut conn = self.get_conn();

        let mut pipe = redis::pipe();
        for key in keys {
            pipe.incr(key.clone(), 1i64).ignore();
        }

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "batch_incr",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis batch INCR error: {}", e);
                metrics.record_cache_operation(
                    "batch_incr",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }

    /// Batch GET multiple keys using pipelining, returns number of cache hits
    pub async fn get_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let start = Instant::now();
        let mut conn = self.get_conn();

        let mut pipe = redis::pipe();
        for key in keys {
            pipe.get(key.clone());
        }

        match pipe.query_async::<Vec<Option<String>>>(&mut conn).await {
            Ok(results) => {
                let hits = results.iter().filter(|r| r.is_some()).count();
                metrics.record_cache_operation(
                    "batch_get",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(hits)
            }
            Err(e) => {
                warn!("Redis batch GET error: {}", e);
                metrics.record_cache_operation("batch_get", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    #[allow(dead_code)]
    pub async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
        let start = Instant::now();
        let mut conn = self.get_conn();

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

    /// Batch delete multiple keys using pipelining
    pub async fn del_batch(&self, keys: &[String], metrics: &AppMetrics) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut conn = self.get_conn();

        let mut pipe = redis::pipe();
        for key in keys {
            pipe.del(key.clone()).ignore();
        }

        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                metrics.record_cache_operation(
                    "batch_del",
                    "success",
                    start.elapsed().as_secs_f64(),
                );
                Ok(())
            }
            Err(e) => {
                error!("Redis batch DEL error: {}", e);
                metrics.record_cache_operation("batch_del", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    /// DEPRECATED: Use del_batch with explicit keys instead
    /// SCAN is better than KEYS but explicit key tracking is best for throughput
    #[allow(dead_code)]
    pub async fn invalidate_pattern(&self, pattern: &str, metrics: &AppMetrics) -> Result<()> {
        warn!("invalidate_pattern is deprecated - use del_batch with explicit keys for better throughput");

        let start = Instant::now();
        let mut conn = self.get_conn();

        // Use SCAN instead of KEYS (non-blocking)
        let mut cursor: u64 = 0;
        let mut all_keys: Vec<String> = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await?;

            all_keys.extend(keys);
            cursor = new_cursor;

            if cursor == 0 {
                break;
            }
        }

        if !all_keys.is_empty() {
            match conn.del::<Vec<String>, i32>(all_keys).await {
                Ok(_) => {
                    metrics.record_cache_operation(
                        "invalidate",
                        "success",
                        start.elapsed().as_secs_f64(),
                    );
                    Ok(())
                }
                Err(e) => {
                    error!("Redis pattern invalidate DEL error: {}", e);
                    metrics.record_cache_operation(
                        "invalidate",
                        "error",
                        start.elapsed().as_secs_f64(),
                    );
                    Err(e.into())
                }
            }
        } else {
            metrics.record_cache_operation("invalidate", "success", start.elapsed().as_secs_f64());
            Ok(())
        }
    }
}
