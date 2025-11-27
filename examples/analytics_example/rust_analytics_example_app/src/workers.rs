// Background Workers
//
// Enhanced workers for 10K+ QPS with diverse query types,
// granular caching, and no worker limits.
// FIXED: Proper error logging, cached org_ids for throughput

use anyhow::Result;
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tracing::{debug, info, error, warn};
use uuid::Uuid;

use crate::{
    database::{Database, RedisCache},
    generators::DataGenerator,
    metrics::AppMetrics,
    models::EventType,
};
use crate::config::Config;
use crate::models::Event;

/// Shared cache of organization IDs to avoid repeated DB queries
pub struct OrgIdCache {
    org_ids: RwLock<Vec<Uuid>>,
    user_ids_by_org: RwLock<std::collections::HashMap<Uuid, Vec<Uuid>>>,
}

impl OrgIdCache {
    pub fn new() -> Self {
        Self {
            org_ids: RwLock::new(Vec::new()),
            user_ids_by_org: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub async fn get_random_org_id(&self) -> Option<Uuid> {
        let org_ids = self.org_ids.read().await;
        if org_ids.is_empty() {
            return None;
        }
        let mut rng = StdRng::from_entropy();
        Some(org_ids[rng.gen_range(0..org_ids.len())])
    }

    pub async fn get_org_ids(&self) -> Vec<Uuid> {
        self.org_ids.read().await.clone()
    }

    pub async fn get_user_ids(&self, org_id: Uuid) -> Vec<Uuid> {
        let map = self.user_ids_by_org.read().await;
        map.get(&org_id).cloned().unwrap_or_default()
    }

    pub async fn refresh(&self, database: &Database, organizations: u32) -> Result<()> {
        let start = Instant::now();

        let new_org_ids = database.get_all_organization_ids(organizations).await?;
        if new_org_ids.is_empty() {
            warn!("No organizations found in database during cache refresh");
            return Ok(());
        }

        let mut new_user_map = std::collections::HashMap::new();
        for &org_id in &new_org_ids {
            match database.get_user_ids_for_org(org_id, 100).await {
                Ok(users) => { new_user_map.insert(org_id, users); }
                Err(e) => {
                    error!("Failed to fetch users for org {}: {}", org_id, e);
                    new_user_map.insert(org_id, Vec::new());
                }
            }
        }

        {
            let mut org_ids = self.org_ids.write().await;
            *org_ids = new_org_ids;
        }
        {
            let mut user_ids = self.user_ids_by_org.write().await;
            *user_ids = new_user_map;
        }

        debug!("OrgIdCache refreshed in {:.2}ms", start.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }
}

/// EventGeneratorWorker simulates realistic user activity
pub struct EventGeneratorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl EventGeneratorWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
            generator,
            org_cache,
        }
    }

    pub async fn run_batch(&self, events_per_second: u64, _organizations: u32) -> Result<()> {
        let start = Instant::now();
        let mut success_count = 0u64;

        let org_ids = self.org_cache.get_org_ids().await;
        if org_ids.is_empty() {
            warn!("No organizations available for event generation");
            return Ok(());
        }

        const BATCH_SIZE: usize = 500;
        let total_events = events_per_second as usize;
        let num_batches = (total_events + BATCH_SIZE - 1) / BATCH_SIZE;

        for batch_idx in 0..num_batches {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, total_events);
            let batch_size = batch_end - batch_start;

            let mut events = Vec::with_capacity(batch_size);
            let mut event_types = Vec::with_capacity(batch_size);

            for _ in 0..batch_size {
                let org_id = org_ids[StdRng::from_entropy().gen_range(0..org_ids.len())];
                let user_ids = self.org_cache.get_user_ids(org_id).await;

                let event = self.generator.generate_event(org_id, &user_ids);
                event_types.push(event.event_type.clone());
                events.push(event);
            }

            let insert_start = Instant::now();
            match self.database.insert_events_batch(&events).await {
                Ok(rows_affected) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    success_count += rows_affected;

                    self.metrics.record_operation_success("event_generation");
                    self.metrics.record_db_operation("batch_insert", "success", actual_insert_duration);

                    if let Err(e) = self.handle_cache_updates(&events).await {
                        error!("Cache update failed after event batch: {}", e);
                    }

                    for event_type in &event_types {
                        self.metrics.record_event_generated(event_type);
                    }
                }
                Err(e) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    self.metrics.record_operation_error("event_generation", "batch_insert_error");
                    self.metrics.record_db_operation("batch_insert", "error", actual_insert_duration);
                    error!("Batch insert failed: {}", e);
                }
            }
        }

        let total_duration = start.elapsed().as_secs_f64();
        self.metrics.event_generation_duration.observe(total_duration);

        debug!(
            "Generated {} events ({} success) in {:.2}s",
            events_per_second, success_count, total_duration
        );

        Ok(())
    }

    async fn handle_cache_updates(&self, events: &[Event]) -> Result<()> {
        let mut orgs_to_invalidate = std::collections::HashSet::new();

        // Batch increment realtime counters using pipeline
        let mut counter_keys: Vec<String> = Vec::new();
        for event in events {
            let org_id = event.organization_id;
            orgs_to_invalidate.insert(org_id);
            counter_keys.push(self.generator.cache_key_realtime_counter(org_id, "minute"));
        }

        // Use batch increment instead of individual calls
        if let Err(e) = self.cache.incr_batch(&counter_keys, &self.metrics).await {
            error!("Batch counter increment failed: {}", e);
        }

        // Collect keys to invalidate for important events
        let mut keys_to_invalidate: Vec<String> = Vec::new();
        for event in events {
            match EventType::from_str(&event.event_type) {
                Some(EventType::Purchase) | Some(EventType::Conversion) | Some(EventType::SignUp) => {
                    // Add specific keys instead of using pattern scan
                    for hours in [1, 6, 24, 168] {
                        keys_to_invalidate.push(self.generator.cache_key_overview(event.organization_id, hours));
                    }
                }
                _ => {}
            }
        }

        // Batch delete instead of pattern-based invalidation
        if !keys_to_invalidate.is_empty() {
            if let Err(e) = self.cache.del_batch(&keys_to_invalidate, &self.metrics).await {
                error!("Batch cache invalidation failed: {}", e);
            }
        }

        Ok(())
    }
}

/// QuerySimulatorWorker with support for 10K+ QPS and diverse query types
pub struct QuerySimulatorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl QuerySimulatorWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
            generator,
            org_cache,
        }
    }

    /// Start worker pool - all workers run at maximum speed
    pub async fn start_worker_pool(&self, _queries_per_second: u64, _organizations: u32, max_workers: usize) {
        let num_workers = std::cmp::max(max_workers, 10);

        info!("Starting {} query workers at maximum speed", num_workers);

        for worker_id in 0..num_workers {
            let database = self.database.clone();
            let cache = self.cache.clone();
            let metrics = self.metrics.clone();
            let generator = self.generator.clone();
            let org_cache = self.org_cache.clone();

            tokio::spawn(async move {
                let worker = QuerySimulatorWorker { database, cache, metrics, generator, org_cache };
                worker.run_worker(worker_id).await;
            });
        }
    }

    async fn run_worker(&self, worker_id: usize) {
        info!("Query worker {} started at maximum speed", worker_id);

        loop {
            // Use cached org_ids instead of DB query each iteration
            match self.org_cache.get_random_org_id().await {
                Some(org_id) => {
                    if let Err(e) = self.execute_diverse_query(org_id).await {
                        error!("Worker {} query error: {}", worker_id, e);
                    }
                }
                None => {
                    // Small sleep only when cache is empty
                    debug!("Worker {} waiting for org cache to populate", worker_id);
                    sleep(Duration::from_millis(100)).await;
                }
            }
            // No delay - workers run at maximum speed
        }
    }

    /// Execute diverse query types with weighted distribution
    async fn execute_diverse_query(&self, org_id: Uuid) -> Result<()> {
        let mut rng = StdRng::from_entropy();
        let query_type = rng.gen_range(0..100);

        // Weighted distribution across many query types
        let result = match query_type {
            // High frequency: overview queries (40%)
            0..=39 => self.get_analytics_overview(org_id, 24).await,

            // Hourly time-series (20%)
            40..=59 => {
                let hour_offset = rng.gen_range(0..24);
                self.get_hourly_metrics(org_id, hour_offset).await
            }

            // Top pages (10%)
            60..=69 => self.get_top_pages(org_id).await,

            // Event distribution (10%)
            70..=79 => self.get_event_distribution(org_id).await,

            // User activity queries (5%)
            80..=84 => self.get_random_user_activity(org_id).await,

            // Page performance (5%)
            85..=89 => self.get_random_page_performance(org_id).await,

            // Real-time counters (5%)
            90..=94 => self.get_realtime_stats(org_id).await,

            // Short time ranges (5%)
            _ => self.get_analytics_overview(org_id, 1).await,
        };

        match result {
            Ok(cache_hit) => {
                self.metrics.record_operation_success("analytics_query");
                self.metrics.queries_executed_total.inc();
                if cache_hit {
                    self.metrics.cache_hits_total.inc();
                } else {
                    self.metrics.cache_misses_total.inc();
                }
            }
            Err(e) => {
                self.metrics.record_operation_error("analytics_query", "execution_error");
                error!("Query execution error for org {}: {}", org_id, e);
            }
        }

        Ok(())
    }

    async fn get_analytics_overview(&self, org_id: Uuid, hours: i32) -> Result<bool> {
        let cache_key = self.generator.cache_key_overview(org_id, hours as u32);

        // Try cache first
        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {} // Cache miss, continue to DB
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        // Cache miss - query database
        let db_start = Instant::now();
        let overview = self.database.get_analytics_overview(org_id, hours).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        // Populate cache with 15 minute TTL
        if let Err(e) = self.cache.set(&cache_key, &overview, 900, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_hourly_metrics(&self, org_id: Uuid, hour_offset: i32) -> Result<bool> {
        let hour = chrono::Utc::now() - chrono::Duration::hours(hour_offset as i64);
        let cache_key = self.generator.cache_key_hourly(org_id, hour);

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        let db_start = Instant::now();
        let metrics = self.database.get_hourly_metrics(org_id, hour_offset).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &metrics, 3600, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_top_pages(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_top_pages(org_id, 24);

        match self.cache.get::<Vec<serde_json::Value>>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        let db_start = Instant::now();
        let top_pages = self.database.get_top_pages(org_id, 10).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &top_pages, 1200, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_event_distribution(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_event_distribution(org_id, "24h");

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        let db_start = Instant::now();
        let dist = self.database.get_event_distribution(org_id).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &dist, 900, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_random_user_activity(&self, org_id: Uuid) -> Result<bool> {
        let user_ids = self.org_cache.get_user_ids(org_id).await;
        if user_ids.is_empty() {
            return Ok(false);
        }

        let user_id = user_ids[StdRng::from_entropy().gen_range(0..user_ids.len())];
        let cache_key = self.generator.cache_key_user_activity(user_id);

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        let db_start = Instant::now();
        let activity = self.database.get_user_activity(user_id).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &activity, 1800, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_random_page_performance(&self, org_id: Uuid) -> Result<bool> {
        let pages = self.generator.get_popular_pages();
        let page = pages[StdRng::from_entropy().gen_range(0..pages.len())];
        let page_url = format!("https://app.example.com{}", page);

        let cache_key = self.generator.cache_key_page(org_id, &page_url);

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        let db_start = Instant::now();
        let perf = self.database.get_page_performance(org_id, &page_url).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &perf, 1800, &self.metrics).await {
            error!("Cache set failed for key {}: {}", cache_key, e);
        }

        Ok(false)
    }

    async fn get_realtime_stats(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_realtime(org_id);

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_data)) => return Ok(true),
            Ok(None) => {}
            Err(e) => {
                error!("Cache get failed for key {}: {}", cache_key, e);
            }
        }

        // Realtime stats cache miss (will be populated by event generator)
        Ok(false)
    }
}

/// CacheWarmupWorker proactively populates cache with diverse data
pub struct CacheWarmupWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl CacheWarmupWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
            generator,
            org_cache,
        }
    }

    /// Initial bulk population of cache with thousands of keys
    pub async fn bulk_populate(&self) -> Result<()> {
        info!("Starting bulk cache population...");
        let start = Instant::now();
        let mut total_keys = 0u64;

        let org_ids = self.org_cache.get_org_ids().await;
        if org_ids.is_empty() {
            warn!("No organizations available for bulk population");
            return Ok(());
        }

        // Process orgs in chunks to avoid overwhelming the system
        for (chunk_idx, org_chunk) in org_ids.chunks(10).enumerate() {
            let mut batch_entries: Vec<(String, String, u64)> = Vec::new();

            for &org_id in org_chunk {
                // 1. Overview for multiple time ranges (4 keys per org)
                for hours in [1, 6, 24, 168] {
                    match self.database.get_analytics_overview(org_id, hours).await {
                        Ok(data) => {
                            let key = self.generator.cache_key_overview(org_id, hours as u32);
                            if let Ok(json) = serde_json::to_string(&data) {
                                batch_entries.push((key, json, 900));
                            }
                        }
                        Err(e) => error!("Failed to fetch overview for org {}: {}", org_id, e),
                    }
                }

                // 2. Hourly metrics for last 24 hours (24 keys per org)
                for hour_offset in 0..24 {
                    match self.database.get_hourly_metrics(org_id, hour_offset).await {
                        Ok(metrics) => {
                            let hour = chrono::Utc::now() - chrono::Duration::hours(hour_offset as i64);
                            let key = self.generator.cache_key_hourly(org_id, hour);
                            if let Ok(json) = serde_json::to_string(&metrics) {
                                batch_entries.push((key, json, 3600));
                            }
                        }
                        Err(e) => error!("Failed to fetch hourly metrics for org {}: {}", org_id, e),
                    }
                }

                // 3. Top pages (1 key per org)
                match self.database.get_top_pages(org_id, 10).await {
                    Ok(pages) => {
                        let key = self.generator.cache_key_top_pages(org_id, 24);
                        if let Ok(json) = serde_json::to_string(&pages) {
                            batch_entries.push((key, json, 1200));
                        }
                    }
                    Err(e) => error!("Failed to fetch top pages for org {}: {}", org_id, e),
                }

                // 4. Event distribution (1 key per org)
                match self.database.get_event_distribution(org_id).await {
                    Ok(dist) => {
                        let key = self.generator.cache_key_event_distribution(org_id, "24h");
                        if let Ok(json) = serde_json::to_string(&dist) {
                            batch_entries.push((key, json, 900));
                        }
                    }
                    Err(e) => error!("Failed to fetch event distribution for org {}: {}", org_id, e),
                }

                // 5. Page performance for ALL popular pages (12 keys per org)
                for page in self.generator.get_popular_pages() {
                    let page_url = format!("https://app.example.com{}", page);
                    match self.database.get_page_performance(org_id, &page_url).await {
                        Ok(perf) => {
                            let key = self.generator.cache_key_page(org_id, &page_url);
                            if let Ok(json) = serde_json::to_string(&perf) {
                                batch_entries.push((key, json, 1800));
                            }
                        }
                        Err(e) => error!("Failed to fetch page performance for org {}: {}", org_id, e),
                    }
                }

                // 6. User activity for sampled users (up to 20 keys per org)
                let user_ids = self.org_cache.get_user_ids(org_id).await;
                for user_id in user_ids.iter().take(20) {
                    match self.database.get_user_activity(*user_id).await {
                        Ok(activity) => {
                            let key = self.generator.cache_key_user_activity(*user_id);
                            if let Ok(json) = serde_json::to_string(&activity) {
                                batch_entries.push((key, json, 1800));
                            }
                        }
                        Err(_) => {} // User may have no events, skip silently
                    }
                }

                // 7. Realtime counters (initialize)
                let realtime_key = self.generator.cache_key_realtime(org_id);
                let realtime_data = serde_json::json!({
                    "organization_id": org_id,
                    "current_active_users": 0,
                    "events_last_minute": 0,
                    "events_last_hour": 0
                });
                if let Ok(json) = serde_json::to_string(&realtime_data) {
                    batch_entries.push((realtime_key, json, 60));
                }

                // 8. Rolling window metrics (multiple time windows)
                for minutes in [5, 15, 30, 60] {
                    let key = self.generator.cache_key_rolling_window(org_id, "events", minutes);
                    let data = serde_json::json!({"count": 0, "window_minutes": minutes});
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, (minutes * 60) as u64));
                    }
                }
            }

            // Batch write this chunk
            let chunk_size = batch_entries.len();
            if !batch_entries.is_empty() {
                if let Err(e) = self.cache.set_batch_json(batch_entries, &self.metrics).await {
                    error!("Batch cache write failed for chunk {}: {}", chunk_idx, e);
                } else {
                    total_keys += chunk_size as u64;
                    info!("Populated chunk {} with {} keys (total: {})", chunk_idx, chunk_size, total_keys);
                }
            }
        }

        let duration = start.elapsed().as_secs_f64();
        info!(
            "Bulk cache population completed: {} keys in {:.2}s ({:.0} keys/sec)",
            total_keys,
            duration,
            total_keys as f64 / duration
        );

        Ok(())
    }

    /// Continuous warmup that refreshes cache and adds new keys
    pub async fn warmup_popular_queries(&self, _organizations: u32) -> Result<()> {
        info!("Starting cache warmup refresh cycle");
        let start = Instant::now();
        let mut refreshed_count = 0u64;

        let org_ids = self.org_cache.get_org_ids().await;
        let mut batch_entries: Vec<(String, String, u64)> = Vec::new();

        for org_id in org_ids {
            // Refresh overview for multiple time ranges
            for hours in [1, 6, 24, 168] {
                match self.database.get_analytics_overview(org_id, hours).await {
                    Ok(data) => {
                        let key = self.generator.cache_key_overview(org_id, hours as u32);
                        if let Ok(json) = serde_json::to_string(&data) {
                            batch_entries.push((key, json, 900));
                            refreshed_count += 1;
                        }
                    }
                    Err(e) => error!("Failed to fetch overview for org {}: {}", org_id, e),
                }
            }

            // Refresh recent hourly metrics (last 6 hours only for refresh)
            for hour_offset in 0..6 {
                match self.database.get_hourly_metrics(org_id, hour_offset).await {
                    Ok(metrics) => {
                        let hour = chrono::Utc::now() - chrono::Duration::hours(hour_offset as i64);
                        let key = self.generator.cache_key_hourly(org_id, hour);
                        if let Ok(json) = serde_json::to_string(&metrics) {
                            batch_entries.push((key, json, 3600));
                            refreshed_count += 1;
                        }
                    }
                    Err(e) => error!("Failed to fetch hourly metrics for org {}: {}", org_id, e),
                }
            }

            // Refresh top pages
            match self.database.get_top_pages(org_id, 10).await {
                Ok(pages) => {
                    let key = self.generator.cache_key_top_pages(org_id, 24);
                    if let Ok(json) = serde_json::to_string(&pages) {
                        batch_entries.push((key, json, 1200));
                        refreshed_count += 1;
                    }
                }
                Err(e) => error!("Failed to fetch top pages for org {}: {}", org_id, e),
            }

            // Refresh event distribution
            match self.database.get_event_distribution(org_id).await {
                Ok(dist) => {
                    let key = self.generator.cache_key_event_distribution(org_id, "24h");
                    if let Ok(json) = serde_json::to_string(&dist) {
                        batch_entries.push((key, json, 900));
                        refreshed_count += 1;
                    }
                }
                Err(e) => error!("Failed to fetch event distribution for org {}: {}", org_id, e),
            }

            // Batch write every 100 entries
            if batch_entries.len() >= 100 {
                if let Err(e) = self.cache.set_batch_json(batch_entries.clone(), &self.metrics).await {
                    error!("Batch cache write failed: {}", e);
                }
                batch_entries.clear();
            }
        }

        // Write remaining entries
        if !batch_entries.is_empty() {
            if let Err(e) = self.cache.set_batch_json(batch_entries, &self.metrics).await {
                error!("Final batch cache write failed: {}", e);
            }
        }

        let duration = start.elapsed().as_secs_f64();
        info!(
            "Cache warmup completed: {} entries in {:.2}s ({:.0} keys/sec)",
            refreshed_count,
            duration,
            refreshed_count as f64 / duration
        );

        Ok(())
    }
}

pub struct SystemMonitorWorker {
    database: Arc<Database>,
    metrics: Arc<AppMetrics>,
    org_cache: Arc<OrgIdCache>,
}

impl SystemMonitorWorker {
    pub fn new(database: Arc<Database>, metrics: Arc<AppMetrics>, org_cache: Arc<OrgIdCache>) -> Self {
        Self { database, metrics, org_cache }
    }

    pub async fn update_system_metrics(&self, config: &Config) -> Result<()> {
        let estimated_connections = std::cmp::min(
            (config.queries_per_second + config.events_per_second) / 20,
            config.db_pool_size as u64
        ) as i64;

        self.metrics.db_connections_active.set(estimated_connections);

        let org_count = self.org_cache.get_org_ids().await.len() as i64;
        self.metrics.active_organizations.set(org_count);

        Ok(())
    }

    /// Periodically refresh the org cache
    pub async fn refresh_org_cache(&self, organizations: u32) -> Result<()> {
        if let Err(e) = self.org_cache.refresh(&self.database, organizations).await {
            error!("Failed to refresh org cache: {}", e);
            return Err(e);
        }
        Ok(())
    }
}