// Background Workers
//
// This module contains the background workers that generate realistic load patterns.
// These workers run continuously to simulate user activity, execute queries, and
// maintain cache warmth without requiring external API calls.

use anyhow::Result;
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{sync::Arc, time::Instant};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    database::{Database, RedisCache},
    generators::DataGenerator,
    metrics::AppMetrics,
    models::EventType,
};
use crate::config::Config;
use crate::models::Event;

/// EventGeneratorWorker simulates realistic user activity by creating events
/// Generates page views, clicks, conversions, signups, and purchases
/// at the configured rate with proper distribution patterns
pub struct EventGeneratorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
}

impl EventGeneratorWorker {
    /// Create a new event generator worker with shared services
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
            generator,
        }
    }

    /// Generate a batch of events at the specified rate with efficient batching
    /// Called every second by the main event generation loop
    /// Uses batch inserts for better performance at high throughput
    pub async fn run_batch(&self, events_per_second: u64, organizations: u32) -> Result<()> {
        let start = Instant::now();
        let mut success_count = 0u64;
        let mut error_count = 0u64;

        // Get random organization IDs for load distribution
        let org_ids = self.database.get_random_organization_ids(organizations).await?;
        if org_ids.is_empty() {
            return Ok(());
        }

        // Pre-fetch user IDs for all organizations to avoid repeated queries
        let mut org_users: std::collections::HashMap<Uuid, Vec<Uuid>> = std::collections::HashMap::new();
        for &org_id in &org_ids {
            match self.database.get_random_user_ids(org_id, 50).await {
                Ok(users) => { org_users.insert(org_id, users); }
                Err(_) => { org_users.insert(org_id, Vec::new()); }
            }
        }

        // Generate events in batches for better memory management
        const BATCH_SIZE: usize = 500;
        let total_events = events_per_second as usize;
        let num_batches = (total_events + BATCH_SIZE - 1) / BATCH_SIZE;

        for batch_idx in 0..num_batches {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, total_events);
            let batch_size = batch_end - batch_start;

            // Generate events for this batch
            let mut events = Vec::with_capacity(batch_size);
            let mut event_types = Vec::with_capacity(batch_size);

            for _ in 0..batch_size {
                let org_id = org_ids[StdRng::from_entropy().gen_range(0..org_ids.len())];
                let user_ids = org_users.get(&org_id).map(|v| v.as_slice()).unwrap_or(&[]);

                let event = self.generator.generate_event(org_id, user_ids);
                event_types.push(event.event_type.clone());
                events.push(event);
            }

            // Batch insert events and measure performance
            let insert_start = Instant::now();
            match self.database.insert_events_batch(&events).await {
                Ok(rows_affected) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    success_count += rows_affected;

                    // Record metrics for successful batch
                    self.metrics.record_operation_success("event_generation");
                    self.metrics.record_db_operation("batch_insert", "success", actual_insert_duration);

                    // Handle cache invalidation for high-impact events
                    self.handle_cache_invalidation(&events).await?;

                    // Record individual event metrics
                    for event_type in &event_types {
                        self.metrics.record_event_generated(event_type);
                    }
                }
                Err(e) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    error_count += batch_size as u64;

                    self.metrics.record_operation_error("event_generation", "batch_insert_error");
                    self.metrics.record_db_operation("batch_insert", "error", actual_insert_duration);

                    tracing::warn!("Batch insert failed for {} events: {}", batch_size, e);
                    // Continue with next batch instead of failing completely
                }
            }
        }

        // Track overall batch generation performance
        let total_duration = start.elapsed().as_secs_f64();
        self.metrics.event_generation_duration.observe(total_duration);

        debug!(
            "Generated {} events ({} success, {} errors) in {:.2}s",
            events_per_second, success_count, error_count, total_duration
        );

        Ok(())
    }

    /// Handle cache invalidation for events that should trigger cache clearing
    /// Only invalidate for high-impact events to avoid excessive cache churn
    async fn handle_cache_invalidation(&self, events: &[Event]) -> Result<()> {
        let mut orgs_to_invalidate = std::collections::HashSet::new();

        // Only invalidate cache for conversion and purchase events
        for event in events {
            if matches!(EventType::from_str(&event.event_type), Some(EventType::Purchase)) {
                orgs_to_invalidate.insert(event.organization_id);
            }
        }

        // Batch cache invalidations by organization
        for org_id in orgs_to_invalidate {
            let cache_pattern = format!("analytics:{}:*", org_id);
            let cache_start = Instant::now();

            match self.cache.invalidate_pattern(&cache_pattern, &self.metrics).await {
                Ok(_) => {
                    self.metrics.record_cache_operation("invalidate", "success", cache_start.elapsed().as_secs_f64());
                }
                Err(e) => {
                    self.metrics.record_cache_operation("invalidate", "error", cache_start.elapsed().as_secs_f64());
                    tracing::warn!("Cache invalidation failed for org {}: {}", org_id, e);
                }
            }
        }

        Ok(())
    }
}

/// QuerySimulatorWorker executes realistic analytics queries
/// Simulates dashboard loads, reports, and real-time analytics queries
/// with proper caching patterns and cache hit ratios
pub struct QuerySimulatorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
}

impl QuerySimulatorWorker {
    /// Create a new query simulator worker
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
        }
    }

    /// Execute a batch of queries at the specified rate
    /// Distributes queries across different organizations and query types
    /// Maintains realistic cache hit ratios through weighted selection
    pub async fn run_batch(&self, queries_per_second: u64, organizations: u32) -> Result<()> {
        let org_ids = self.database.get_random_organization_ids(organizations).await?;
        if org_ids.is_empty() {
            return Ok(());
        }

        // Execute the specified number of queries per second
        for _ in 0..queries_per_second {
            let org_id = org_ids[StdRng::from_entropy().gen_range(0..org_ids.len())];
            self.execute_random_query(org_id).await?;
        }

        Ok(())
    }

    /// Execute a random query type based on realistic usage patterns
    /// Query distribution:
    /// - 70% Analytics overview (dashboard loads, high cache hit rate)
    /// - 20% Top pages (content analysis, moderate cache hit rate)
    /// - 10% Real-time stats (never cached, always fresh data)
    async fn execute_random_query(&self, org_id: Uuid) -> Result<()> {
        let start = Instant::now();
        let mut rng = StdRng::from_entropy();
        let query_type = rng.gen_range(0..100);

        let result = match query_type {
            0..=69 => self.get_analytics_overview(org_id).await,
            70..=89 => self.get_top_pages(org_id).await,
            _ => self.get_real_time_stats(org_id).await,
        };

        let duration = start.elapsed().as_secs_f64();

        match result {
            Ok(cache_hit) => {
                self.metrics.record_operation_success("analytics_query");
                self.metrics.record_query_executed(duration, cache_hit);
            }
            Err(e) => {
                self.metrics.record_operation_error("analytics_query", "execution_error");
                tracing::warn!("Query execution error: {}", e);
            }
        }

        Ok(())
    }

    /// Execute analytics overview query with caching
    /// This represents expensive dashboard aggregation queries
    /// Cache TTL: 5 minutes to balance freshness with performance
    async fn get_analytics_overview(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = format!("analytics:{}:overview:24h", org_id);

        let cache_key = format!("analytics:{}:overview:24h", org_id);

        // Measure Redis GET operation
        let cached = self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await;

        match cached {
            Ok(Some(_)) => {
                return Ok(true); // Cache hit
            }
            Ok(None) => {
            }
            Err(_) => {
                return Err(anyhow::anyhow!("Cache error"));
            }
        }

        // Measure database query
        let db_start = Instant::now();
        let overview = self.database.get_analytics_overview(org_id, 24).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        self.cache.set(&cache_key, &overview, 300, &self.metrics).await?;

        Ok(false)
    }

    /// Execute top pages query with caching
    /// Represents content performance analysis queries
    /// Cache TTL: 10 minutes (less frequently updated than overview)
    async fn get_top_pages(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = format!("analytics:{}:top_pages:24h", org_id);

        let cached = self.cache.get::<Vec<serde_json::Value>>(&cache_key, &self.metrics).await;

        match cached {
            Ok(Some(_)) => {
                return Ok(true); // Cache hit
            }
            Ok(None) => {
            }
            Err(_) => {
                return Err(anyhow::anyhow!("Cache get error"));
            }
        }

        // Cache miss - query database
        let db_start = Instant::now();
        let top_pages = self.database.get_top_pages(org_id, 10).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        // Cache result with 10-minute TTL
        let set_result = self.cache.set(&cache_key, &top_pages, 600, &self.metrics).await;

        match set_result {
            Ok(_) => {
            }
            Err(e) => {
                tracing::warn!("Failed to cache top pages for org {}: {}", org_id, e);
            }
        }

        Ok(false) // Cache miss
    }

    /// Execute real-time statistics query (never cached)
    /// Represents live dashboards and real-time monitoring queries
    /// Always hits the database for the freshest data
    async fn get_real_time_stats(&self, org_id: Uuid) -> Result<bool> {
        // Real-time queries are never cached - always fresh data
        let db_start = Instant::now();
        let _overview = self.database.get_analytics_overview(org_id, 1).await?; // Last hour only
        self.metrics.record_db_query(db_start.elapsed().as_secs_f64());

        Ok(false) // Never cached
    }
}

/// CacheWarmupWorker maintains cache hit ratios by pre-loading popular queries
/// Runs periodically to refresh expired cache entries and maintain performance
/// Helps simulate realistic production cache behavior
pub struct CacheWarmupWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
}

impl CacheWarmupWorker {
    /// Create a new cache warmup worker
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
        }
    }

    /// Warm up cache with popular queries for all organizations
    /// Called every minute to maintain target cache hit ratios
    /// Pre-loads expensive queries that are frequently accessed
    pub async fn warmup_popular_queries(&self, organizations: u32) -> Result<()> {
        info!("Starting cache warmup");

        let org_ids = self.database.get_random_organization_ids(organizations).await?;

        for org_id in org_ids {
            // Warmup analytics overview (most popular query)
            let overview_key = format!("analytics:{}:overview:24h", org_id);
            let cached = self.cache.get::<serde_json::Value>(&overview_key, &self.metrics).await;

            match cached {
                Ok(Some(_)) => {
                }
                Ok(None) => {
                    // Cache miss - warmup with fresh data
                    let db_start = Instant::now();
                    let overview = self.database.get_analytics_overview(org_id, 24).await?;
                    let db_duration = db_start.elapsed().as_secs_f64();
                    self.metrics.record_db_operation("select", "success", db_duration);

                    self.cache.set(&overview_key, &overview, 300, &self.metrics).await;
                }
                Err(_) => {
                }
            }

            // Warmup top pages query (second most popular)
            let pages_key = format!("analytics:{}:top_pages:24h", org_id);
            let cached = self.cache.get::<Vec<serde_json::Value>>(&pages_key, &self.metrics).await;

            match cached {
                Ok(Some(_)) => {
                }
                Ok(None) => {
                    // Cache miss - warmup with fresh data
                    let db_start = Instant::now();
                    let top_pages = self.database.get_top_pages(org_id, 10).await?;
                    let db_duration = db_start.elapsed().as_secs_f64();
                    self.metrics.record_db_operation("select", "success", db_duration);

                    self.cache.set(&pages_key, &top_pages, 600, &self.metrics).await;
                }
                Err(_) => {
                }
            }
        }

        info!("Cache warmup completed");
        Ok(())
    }
}

// In workers.rs
pub struct SystemMonitorWorker {
    database: Arc<Database>,
    metrics: Arc<AppMetrics>,
}

impl SystemMonitorWorker {
    pub fn new(database: Arc<Database>, metrics: Arc<AppMetrics>) -> Self {
        Self { database, metrics }
    }

    pub async fn update_system_metrics(&self, config: &Config) -> Result<()> {
        // For SQLx, you'd need to track this differently since it doesn't expose active connections
        // You could estimate based on your query load or use a fixed value for demo purposes
        let estimated_connections = std::cmp::min(
            (config.queries_per_second + config.events_per_second) / 10,
            20 // max connections
        ) as i64;

        self.metrics.db_connections_active.set(estimated_connections);

        // Update organizations count
        let org_count = self.database.get_random_organization_ids(1000).await?.len() as i64;
        self.metrics.active_organizations.set(org_count);

        Ok(())
    }
}