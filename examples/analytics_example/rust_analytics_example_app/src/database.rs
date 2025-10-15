// Database and Cache Layer
//
// This module provides all database operations and Redis caching functionality.
// It includes connection pooling, schema management, data seeding, and cache
// operations with proper TTL and invalidation strategies.

use anyhow::Result;
use chrono::Utc;
use redis::{AsyncCommands, ProtocolVersion};
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, log};
use uuid::Uuid;

use crate::metrics::AppMetrics;
use crate::{
    config::Config,
    generators::DataGenerator,
    models::{AnalyticsOverview, Event, Organization, TopPage, User},
};

/// Database provides all PostgreSQL operations with connection pooling
/// Handles schema setup, data seeding, event insertion, and analytics queries
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection with optimized pool settings
    /// The connection pool is configured for high concurrency workloads
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(50) // Increase for high throughput
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(600))
            .max_lifetime(Duration::from_secs(1800))
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Setup database schema with proper indexing for analytics workloads
    /// Creates tables, indexes, and extensions needed for the simulation
    pub async fn setup_schema(&self) -> Result<()> {
        // Execute each command separately to avoid prepared statement issues

        // 1. Enable UUID extension
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            .execute(&self.pool)
            .await?;

        // 2. Create organizations table
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

        // 3. Create users table
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

        // 4. Create events table
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

        // 5. Create indexes separately
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_org_created ON events(organization_id, created_at);")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at);",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);")
            .execute(&self.pool)
            .await?;

        info!("Database schema setup complete");
        Ok(())
    }

    /// Seed the database with initial organizations and users
    /// Creates realistic test data for the specified number of orgs and users per org
    pub async fn seed_initial_data(
        &self,
        generator: &DataGenerator,
        config: &Config,
    ) -> Result<()> {
        // Check if we already have data
        let existing_orgs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM organizations")
            .fetch_one(&self.pool)
            .await?;

        if existing_orgs > 0 {
            info!(
                "Database already contains {} organizations, skipping initial data seeding",
                existing_orgs
            );
            return Ok(());
        }

        info!("Seeding initial data...");

        // Create organizations first
        for org_index in 0..config.organizations {
            let org = generator.generate_organization();
            sqlx::query("INSERT INTO organizations (id, name, created_at) VALUES ($1, $2, $3)")
                .bind(&org.id)
                .bind(&org.name)
                .bind(&org.created_at)
                .execute(&self.pool)
                .await?;

            // Generate users for this organization
            let users = generator.generate_users(org.id, config.users_per_org as usize);

            info!(
                "Inserting {} users for organization {} ({})",
                users.len(),
                org_index + 1,
                org.name
            );

            // Insert users with better error handling
            for (user_index, user) in users.iter().enumerate() {
                // Insert users with conflict handling
                match sqlx::query("INSERT INTO users (id, organization_id, email, name, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (email) DO NOTHING")
                    .bind(&user.id)
                    .bind(&user.organization_id)
                    .bind(&user.email)
                    .bind(&user.name)
                    .bind(&user.created_at)
                    .execute(&self.pool)
                    .await
                {
                    Ok(_) => {},
                    Err(e) => {
                        log::warn!("Failed to insert user {} ({}): {}", user_index + 1, user.email, e);
                        // Continue with next user instead of failing completely
                    }
                }
            }
        }

        info!("Initial data seeding complete");
        Ok(())
    }

    /// Insert multiple events in a single batch operation
    /// More efficient for high-volume event generation
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

    /// Insert a new event into the database
    /// This is called frequently by the event generator worker
    pub async fn insert_event(&self, event: &Event) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO events (id, organization_id, user_id, event_type, page_url, referrer, user_agent, ip_address, properties, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
            .bind(&event.id)
            .bind(&event.organization_id)
            .bind(&event.user_id)
            .bind(&event.event_type)
            .bind(&event.page_url)
            .bind(&event.referrer)
            .bind(&event.user_agent)
            .bind(&event.ip_address)
            .bind(&event.properties)
            .bind(&event.created_at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get analytics overview for an organization over a time period
    /// This is an expensive query that's frequently cached
    pub async fn get_analytics_overview(
        &self,
        org_id: Uuid,
        hours: i32,
    ) -> Result<AnalyticsOverview> {
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

    /// Get top pages by view count for an organization
    /// Another expensive query that benefits from caching
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

    /// Get random organization IDs for load distribution
    /// Used by workers to spread load across different tenants
    pub async fn get_random_organization_ids(&self, limit: u32) -> Result<Vec<Uuid>> {
        let rows = sqlx::query("SELECT id FROM organizations ORDER BY RANDOM() LIMIT $1")
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("id")).collect())
    }

    /// Get random user IDs from a specific organization
    /// Used for event generation to simulate realistic user activity
    pub async fn get_random_user_ids(&self, org_id: Uuid, limit: u32) -> Result<Vec<Uuid>> {
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

/// RedisCache provides caching operations with TTL and pattern-based invalidation
/// Implements cache-aside pattern for analytics queries
pub struct RedisCache {
    client: redis::Client,
}

impl RedisCache {
    /// Create a new Redis connection and test connectivity
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        // assert_eq!(
        //     client.get_connection_info().protocol,
        //     ProtocolVersion::RESP3
        // );

        // Test connection on startup - use cmd instead of ping method
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        // .query_async(&mut conn).await?;
        info!("Redis connection established");
        Ok(Self { client })
    }

    pub async fn get<T>(&self, key: &str, metrics: &AppMetrics) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let start = Instant::now();
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        match conn.get::<&str, Option<String>>(key).await {
            Ok(value) => {
                let duration = start.elapsed().as_secs_f64();
                let result = if value.is_some() { "hit" } else { "miss" };
                metrics.record_cache_operation("get", result, duration);

                match value {
                    Some(json_str) => match serde_json::from_str(&json_str) {
                        Ok(v) => Ok(Some(v)),
                        Err(e) => {
                            error!("Can't parse JSON string: {json_str}");
                            Err(e.into())
                        }
                    },
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("RedisError in GET {key}: {e}");
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
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let json_str = serde_json::to_string(value)?;

        match conn
            .set_ex::<&str, String, ()>(key, json_str, ttl_seconds)
            .await
        {
            Ok(_) => {
                log::debug!("Successfully set to {}", key);
                metrics.record_cache_operation("set", "success", start.elapsed().as_secs_f64());
                Ok(())
            }
            Err(e) => {
                log::warn!("Failed to set value: {}", e);
                metrics.record_cache_operation("set", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    pub async fn del(&self, key: &str, metrics: &AppMetrics) -> Result<()> {
        let start = Instant::now();
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        match conn.del::<&str, i32>(key).await {
            Ok(_) => {
                metrics.record_cache_operation("del", "success", start.elapsed().as_secs_f64());
                Ok(())
            }
            Err(e) => {
                metrics.record_cache_operation("del", "error", start.elapsed().as_secs_f64());
                Err(e.into())
            }
        }
    }

    pub async fn invalidate_pattern(&self, pattern: &str, metrics: &AppMetrics) -> Result<()> {
        let start = Instant::now();
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        match conn.keys::<&str, Vec<String>>(pattern).await {
            Ok(keys) => {
                if !keys.is_empty() {
                    match conn.del::<Vec<String>, i32>(keys).await {
                        Ok(_) => {
                            metrics.record_cache_operation(
                                "invalidate",
                                "success",
                                start.elapsed().as_secs_f64(),
                            );
                            Ok(())
                        }
                        Err(e) => {
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
            Err(e) => {
                metrics.record_cache_operation(
                    "invalidate",
                    "error",
                    start.elapsed().as_secs_f64(),
                );
                Err(e.into())
            }
        }
    }
}
