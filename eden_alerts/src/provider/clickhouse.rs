//! ClickHouse analytics provider implementation.

use async_trait::async_trait;
use clickhouse::Client;

use super::{
    AnalyticsProvider, AntiPatternRow, EndpointHealth, ErrorSpikeRow, HotKeyRow, HourlyRollup, ProviderError, SignalRow, TimeWindow,
};

/// ClickHouse provider configuration.
#[derive(Debug, Clone)]
pub struct ClickhouseConfig {
    pub url: String,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".to_string(),
            database: "analytics".to_string(),
            user: None,
            password: None,
        }
    }
}

/// ClickHouse analytics provider.
pub struct ClickhouseProvider {
    client: Client,
    database: String,
}

impl ClickhouseProvider {
    /// Create a new ClickHouse provider with the given configuration.
    pub fn new(config: ClickhouseConfig) -> Result<Self, ProviderError> {
        let mut client = Client::default().with_url(&config.url);

        if let Some(user) = &config.user {
            client = client.with_user(user);
        }
        if let Some(password) = &config.password {
            client = client.with_password(password);
        }

        Ok(Self { client, database: config.database })
    }

    /// Create provider from environment variables.
    pub fn from_env() -> Result<Self, ProviderError> {
        let config = ClickhouseConfig {
            url: std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string()),
            database: std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "analytics".to_string()),
            user: std::env::var("CLICKHOUSE_USER").ok(),
            password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
        };
        Self::new(config)
    }

    fn table(&self, name: &str) -> String {
        format!("{}.{}", self.database, name)
    }
}

#[async_trait]
impl AnalyticsProvider for ClickhouseProvider {
    async fn fetch_endpoint_health(&self, window: &TimeWindow) -> Result<Vec<EndpointHealth>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                organization_uuid,
                endpoint_uuid,
                protocol,
                sum(requests) AS requests,
                sum(errors) AS errors,
                sum(slow_queries) AS slow_queries,
                if(sum(requests) > 0, sum(errors) / sum(requests), 0) AS error_rate,
                if(sum(requests) > 0, sum(slow_queries) / sum(requests), 0) AS slow_rate,
                if(sum(requests) > 0, sum(sum_latency_us) / sum(requests), 0) AS avg_latency_us,
                quantile(0.95)(avg_latency_us) AS p95_latency_us,
                max(max_latency_us) AS max_latency_us
            FROM {}
            WHERE hour >= ? AND hour < ?
            GROUP BY organization_uuid, endpoint_uuid, protocol
            "#,
            self.table("hourly_rollup")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).fetch_all::<EndpointHealth>().await?;

        Ok(rows)
    }

    async fn fetch_anti_patterns(&self, window: &TimeWindow) -> Result<Vec<AntiPatternRow>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                detected_at,
                organization_uuid,
                endpoint_uuid,
                protocol,
                pattern_type,
                sum(occurrence_count) AS occurrence_count,
                any(sample_key) AS sample_key,
                any(sample_details) AS sample_details
            FROM {}
            WHERE detected_at >= ? AND detected_at < ?
            GROUP BY detected_at, organization_uuid, endpoint_uuid, protocol, pattern_type
            ORDER BY occurrence_count DESC
            LIMIT 1000
            "#,
            self.table("anti_patterns")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).fetch_all::<AntiPatternRow>().await?;

        Ok(rows)
    }

    async fn fetch_hourly_rollups(&self, window: &TimeWindow) -> Result<Vec<HourlyRollup>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                hour,
                organization_uuid,
                endpoint_uuid,
                protocol,
                command,
                pattern_hash,
                requests,
                errors,
                slow_queries,
                sum_latency_us,
                max_latency_us,
                if(requests > 0, sum_latency_us / requests, 0) AS avg_latency_us
            FROM {}
            WHERE hour >= ? AND hour < ?
            ORDER BY hour DESC
            LIMIT 10000
            "#,
            self.table("hourly_rollup")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).fetch_all::<HourlyRollup>().await?;

        Ok(rows)
    }

    async fn fetch_signals(&self, window: &TimeWindow) -> Result<Vec<SignalRow>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                event_time,
                organization_uuid,
                endpoint_uuid,
                signal_type,
                severity,
                details,
                latency_us
            FROM {}
            WHERE event_time >= ? AND event_time < ?
            ORDER BY event_time DESC
            LIMIT 10000
            "#,
            self.table("signals")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).fetch_all::<SignalRow>().await?;

        Ok(rows)
    }

    async fn fetch_hot_keys(&self, window: &TimeWindow, min_hits: u64) -> Result<Vec<HotKeyRow>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                organization_uuid,
                endpoint_uuid,
                pattern_template AS key_pattern,
                sum(requests) AS hit_count,
                min(event_time) AS window_start
            FROM {}
            WHERE event_time >= ? AND event_time < ?
            GROUP BY organization_uuid, endpoint_uuid, pattern_template
            HAVING hit_count >= ?
            ORDER BY hit_count DESC
            LIMIT 100
            "#,
            self.table("events_raw")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).bind(min_hits).fetch_all::<HotKeyRow>().await?;

        Ok(rows)
    }

    async fn fetch_error_spikes(&self, window: &TimeWindow, min_errors: u64) -> Result<Vec<ErrorSpikeRow>, ProviderError> {
        let query = format!(
            r#"
            SELECT
                organization_uuid,
                endpoint_uuid,
                countIf(success = 0) AS error_count,
                count(*) AS total_requests,
                if(count(*) > 0, countIf(success = 0) / count(*), 0) AS error_rate,
                min(event_time) AS window_start
            FROM {}
            WHERE event_time >= ? AND event_time < ?
            GROUP BY organization_uuid, endpoint_uuid
            HAVING error_count >= ?
            ORDER BY error_count DESC
            "#,
            self.table("events_raw")
        );

        let rows = self.client.query(&query).bind(window.start).bind(window.end).bind(min_errors).fetch_all::<ErrorSpikeRow>().await?;

        Ok(rows)
    }
}
