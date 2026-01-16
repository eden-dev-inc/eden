// Database and Cache Layer
//
// Enhanced with Redis connection pooling, pipelining support,
// and additional query methods for granular analytics caching.
// FIXED: Proper error logging, efficient org fetching, SCAN instead of KEYS
// FIXED: Using redis crate with proper async connection handling

use anyhow::Result;
use chrono::{Duration, Utc};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use sqlx::{PgPool, Row};
use std::time::Duration as StdDuration;
use tokio::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::metrics::AppMetrics;
use crate::validation::DataValidator;
use crate::{
    config::Config,
    generators::DataGenerator,
    models::{
        AnalyticsOverview, Event, EventTypeDistribution, HourlyMetrics,
        PagePerformance, TopPage, UserActivity
    },
};

/// Database provides all PostgreSQL operations with connection pooling
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection with optimized pool settings for 10K+ QPS
    pub async fn new(database_url: &str, pool_size: u32) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
            .min_connections(pool_size / 2)
            .acquire_timeout(StdDuration::from_secs(5))
            .idle_timeout(StdDuration::from_secs(600))
            .max_lifetime(StdDuration::from_secs(1800))
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Setup database schema with proper indexing for analytics workloads
    pub async fn setup_schema(&self) -> Result<()> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            name VARCHAR NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            email VARCHAR UNIQUE NOT NULL,
            name VARCHAR NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS events (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            organization_id UUID NOT NULL REFERENCES organizations(id),
            user_id UUID REFERENCES users(id),
            event_type VARCHAR NOT NULL,
            page_url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            ip_address VARCHAR,
            properties JSONB DEFAULT '{}',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        "#,
        )
            .execute(&self.pool)
            .await?;

        // Enhanced indexes for high-performance queries
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_org_created ON events(organization_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at DESC);")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_page_url ON events(page_url) WHERE page_url IS NOT NULL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);")
            .execute(&self.pool)
            .await?;

        info!("Database schema setup complete");
        Ok(())
    }

    /// Seed the database with initial organizations and users
    pub async fn seed_initial_data(
        &self,
        generator: &DataGenerator,
        config: &Config,
    ) -> Result<()> {
        let existing_orgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM organizations")
            .fetch_one(&self.pool)
            .await?;

        if existing_orgs > 0 {
            info!(
                "Database already contains {} organizations, skipping seeding",
                existing_orgs
            );
            return Ok(());
        }

        info!("Seeding initial data...");

        for org_index in 0..config.organizations {
            let org = generator.generate_organization();
            sqlx::query("INSERT INTO organizations (id, name, created_at) VALUES ($1, $2, $3)")
                .bind(&org.id)
                .bind(&org.name)
                .bind(&org.created_at)
                .execute(&self.pool)
                .await?;

            let users = generator.generate_users(org.id, config.users_per_org as usize);

            info!(
                "Inserting {} users for organization {} ({})",
                users.len(),
                org_index + 1,
                org.name
            );

            for user in users.iter() {
                if let Err(e) = sqlx::query("INSERT INTO users (id, organization_id, email, name, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (email) DO NOTHING")
                    .bind(&user.id)
                    .bind(&user.organization_id)
                    .bind(&user.email)
                    .bind(&user.name)
                    .bind(&user.created_at)
                    .execute(&self.pool)
                    .await
                {
                    error!("Failed to insert user {}: {}", user.email, e);
                }
            }
        }

        info!("Initial data seeding complete");
        Ok(())
    }

    /// Insert multiple events in a single batch operation
    pub async fn insert_events_batch(&self, events: &[Event]) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO events (id, organization_id, user_id, event_type, page_url, referrer, user_agent, ip_address, properties, created_at) "
        );

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(&event.id)
                .push_bind(&event.organization_id)
                .push_bind(&event.user_id)
                .push_bind(&event.event_type)
                .push_bind(&event.page_url)
                .push_bind(&event.referrer)
                .push_bind(&event.user_agent)
                .push_bind(&event.ip_address)
                .push_bind(&event.properties)
                .push_bind(&event.created_at);
        });

        let result = query_builder.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Get analytics overview with time range
    pub async fn get_analytics_overview(&self, org_id: Uuid, hours: i32) -> Result<AnalyticsOverview> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '1 hour' * $2
            "#,
        )
            .bind(&org_id)
            .bind(hours)
            .fetch_one(&self.pool)
            .await?;

        let total_events: i64 = row.get("total_events");
        let unique_users: i64 = row.get("unique_users");
        let page_views: i64 = row.get("page_views");
        let conversions: i64 = row.get("conversions");

        let conversion_rate = if page_views > 0 {
            (conversions as f64 / page_views as f64) * 100.0
        } else {
            0.0
        };

        Ok(AnalyticsOverview {
            organization_id: org_id,
            total_events,
            unique_users,
            page_views,
            conversions,
            conversion_rate,
            time_period: format!("last {} hours", hours),
        })
    }

    /// Get top pages by view count
    pub async fn get_top_pages(&self, org_id: Uuid, limit: i32) -> Result<Vec<TopPage>> {
        let rows = sqlx::query(
            r#"
            SELECT
                page_url as url,
                COUNT(*) as views,
                COUNT(DISTINCT user_id) as unique_visitors
            FROM events
            WHERE organization_id = $1
            AND event_type = 'page_view'
            AND page_url IS NOT NULL
            AND created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY page_url
            ORDER BY views DESC
            LIMIT $2
            "#,
        )
            .bind(&org_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        let mut top_pages = Vec::new();
        for row in rows {
            top_pages.push(TopPage {
                url: row.get("url"),
                views: row.get("views"),
                unique_visitors: row.get("unique_visitors"),
            });
        }

        Ok(top_pages)
    }

    /// Get hourly metrics for time-series caching
    pub async fn get_hourly_metrics(&self, org_id: Uuid, hour_offset: i32) -> Result<HourlyMetrics> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COUNT(*) FILTER (WHERE event_type = 'sign_up') as signups,
                COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
                COALESCE(SUM(CASE
                    WHEN event_type = 'purchase'
                    THEN (properties->>'total_amount')::float / 100.0
                    ELSE 0
                END), 0) as revenue
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '1 hour' * ($2 + 1)
            AND created_at < NOW() - INTERVAL '1 hour' * $2
            "#,
        )
            .bind(&org_id)
            .bind(hour_offset)
            .fetch_one(&self.pool)
            .await?;

        let hour = Utc::now() - Duration::hours(hour_offset as i64);

        Ok(HourlyMetrics {
            organization_id: org_id,
            hour,
            events: row.get("events"),
            unique_users: row.get("unique_users"),
            page_views: row.get("page_views"),
            clicks: row.get("clicks"),
            conversions: row.get("conversions"),
            signups: row.get("signups"),
            purchases: row.get("purchases"),
            revenue: row.get("revenue"),
        })
    }

    /// Get user activity summary
    pub async fn get_user_activity(&self, user_id: Uuid) -> Result<UserActivity> {
        let row = sqlx::query(
            r#"
            SELECT
                organization_id,
                COUNT(*) as total_events,
                MAX(created_at) as last_seen,
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COALESCE(SUM(CASE
                    WHEN event_type = 'purchase'
                    THEN (properties->>'total_amount')::float / 100.0
                    ELSE 0
                END), 0) as lifetime_value
            FROM events
            WHERE user_id = $1
            GROUP BY organization_id
            "#,
        )
            .bind(&user_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(UserActivity {
            user_id,
            organization_id: row.get("organization_id"),
            total_events: row.get("total_events"),
            last_seen: row.get("last_seen"),
            page_views: row.get("page_views"),
            clicks: row.get("clicks"),
            conversions: row.get("conversions"),
            lifetime_value: row.get("lifetime_value"),
        })
    }

    /// Get page performance metrics
    pub async fn get_page_performance(&self, org_id: Uuid, page_url: &str) -> Result<PagePerformance> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as views,
                COUNT(DISTINCT user_id) as unique_visitors,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions
            FROM events
            WHERE organization_id = $1
            AND page_url = $2
            AND created_at >= NOW() - INTERVAL '24 hours'
            "#,
        )
            .bind(&org_id)
            .bind(page_url)
            .fetch_one(&self.pool)
            .await?;

        Ok(PagePerformance {
            organization_id: org_id,
            page_url: page_url.to_string(),
            views: row.get("views"),
            unique_visitors: row.get("unique_visitors"),
            avg_time_on_page: 45.5, // Placeholder
            bounce_rate: 0.35,       // Placeholder
            conversions: row.get("conversions"),
        })
    }

    /// Get event type distribution
    pub async fn get_event_distribution(&self, org_id: Uuid) -> Result<EventTypeDistribution> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'conversion') as conversions,
                COUNT(*) FILTER (WHERE event_type = 'sign_up') as signups,
                COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
                COUNT(*) as total
            FROM events
            WHERE organization_id = $1
            AND created_at >= NOW() - INTERVAL '24 hours'
            "#,
        )
            .bind(&org_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(EventTypeDistribution {
            organization_id: org_id,
            page_views: row.get("page_views"),
            clicks: row.get("clicks"),
            conversions: row.get("conversions"),
            signups: row.get("signups"),
            purchases: row.get("purchases"),
            total: row.get("total"),
        })
    }

    /// Get ALL organization IDs efficiently (no ORDER BY RANDOM)
    /// This is called once at startup and periodically refreshed
    pub async fn get_all_organization_ids(&self, limit: u32) -> Result<Vec<Uuid>> {
        let rows = sqlx::query("SELECT id FROM organizations LIMIT $1")
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("id")).collect())
    }

    /// Get user IDs for a specific organization (no ORDER BY RANDOM)
    pub async fn get_user_ids_for_org(&self, org_id: Uuid, limit: u32) -> Result<Vec<Uuid>> {
        let rows = sqlx::query(
            "SELECT id FROM users WHERE organization_id = $1 LIMIT $2",
        )
            .bind(&org_id)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("id")).collect())
    }

    /// DEPRECATED: Use get_all_organization_ids + in-memory random selection
    /// Keeping for backward compatibility but logs warning
    pub async fn get_random_organization_ids(&self, limit: u32) -> Result<Vec<Uuid>> {
        warn!("get_random_organization_ids is deprecated - use get_all_organization_ids with OrgIdCache");
        let rows = sqlx::query("SELECT id FROM organizations ORDER BY RANDOM() LIMIT $1")
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("id")).collect())
    }

    /// DEPRECATED: Use get_user_ids_for_org + in-memory random selection
    pub async fn get_random_user_ids(&self, org_id: Uuid, limit: u32) -> Result<Vec<Uuid>> {
        warn!("get_random_user_ids is deprecated - use get_user_ids_for_org with OrgIdCache");
        let rows = sqlx::query(
            "SELECT id FROM users WHERE organization_id = $1 ORDER BY RANDOM() LIMIT $2",
        )
            .bind(&org_id)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("id")).collect())
    }
}

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

        info!("Redis established with {} multiplexed connections", conn_count);
        Ok(Self { connections, conn_count })
    }

    /// Get a connection using simple round-robin based on current thread/task
    fn get_conn(&self) -> MultiplexedConnection {
        // Use thread-local counter for distribution
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % self.conn_count;
        self.connections[idx].clone()
    }

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

    /// Set a value and optionally validate by reading it back.
    /// Validation is performed based on the validator's sample rate.
    pub async fn set_and_validate<T>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: u64,
        metrics: &AppMetrics,
        validator: &DataValidator,
        data_type: &str,
    ) -> Result<()>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let start = Instant::now();
        let mut conn = self.get_conn();
        let json_str = serde_json::to_string(value)?;

        match conn.set_ex::<_, _, ()>(key, json_str.clone(), ttl_seconds).await {
            Ok(_) => {
                metrics.record_cache_operation("set", "success", start.elapsed().as_secs_f64());
            }
            Err(e) => {
                error!("Redis SET error for key {}: {}", key, e);
                metrics.record_cache_operation("set", "error", start.elapsed().as_secs_f64());
                return Err(e.into());
            }
        }

        // Validate by reading back (based on sample rate)
        if validator.should_validate() {
            let mut read_conn = self.get_conn();
            match read_conn.get::<_, Option<String>>(key).await {
                Ok(Some(retrieved_json)) => {
                    let _ = validator.validate_json_str(data_type, &json_str, &retrieved_json);
                }
                Ok(None) => {
                    validator.record_not_found(data_type);
                }
                Err(_) => {
                    validator.record_read_error(data_type);
                }
            }
        }

        Ok(())
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

    /// Increment a counter atomically
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

    /// DEPRECATED: Use del_batch with explicit keys instead
    /// SCAN is better than KEYS but explicit key tracking is best for throughput
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
            metrics.record_cache_operation(
                "invalidate",
                "success",
                start.elapsed().as_secs_f64(),
            );
            Ok(())
        }
    }
}