use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{RowExt, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL connection pool and usage information
///
/// Simplified struct containing essential metrics about database connections,
/// including connection limits, utilization patterns, and basic breakdowns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionInfo {
    /// Maximum number of concurrent connections allowed
    pub max_connections: u64,
    /// Number of connections reserved for superusers
    pub reserved_connections: u64,
    /// Number of currently active connections
    pub active_connections: u64,
    /// Number of idle connections
    pub idle_connections: u64,
    /// Total number of current connections
    pub total_connections: u64,
    /// Percentage of max_connections currently in use (0.0 to 100.0)
    pub connection_utilization: f64,
    /// Number of connections idle in transaction
    pub idle_in_transaction_connections: u64,
    /// Number of connections idle in transaction (aborted)
    pub idle_in_transaction_aborted: u64,
    /// Duration of the oldest connection in seconds
    pub oldest_connection_duration: f64,
    /// Average duration of all connections in seconds
    pub average_connection_duration: f64,
    /// Percentage of connections that are idle in transaction (0.0 to 100.0)
    pub idle_in_transaction_percentage: f64,
    /// Health score for connection pool (0.0 to 100.0, higher is better)
    pub health_score: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresConnectionDetailedMetrics>,
}

/// Detailed connection metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionDetailedMetrics {
    /// Connection breakdown by database (collected when utilization > 70%)
    pub connections_per_database: Option<Vec<PostgresConnectionPerDatabase>>,
    /// Connection breakdown by user (collected when utilization > 70%)
    pub connections_per_user: Option<Vec<PostgresConnectionPerUser>>,
    /// Connection breakdown by application (collected when utilization > 70%)
    pub connections_per_application: Option<Vec<PostgresConnectionPerApplication>>,
    /// Long-running idle connections (collected when idle_in_transaction_percentage > 10%)
    pub problematic_connections: Option<Vec<PostgresConnectionDetail>>,
}

impl MetadataCollection for PostgresConnectionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([(
            "connection_stats".to_string(),
            QueryInput::new(
                "SELECT
                    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
                    (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections') as reserved_connections,
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) as total_connections,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_in_transaction_aborted,
                    COALESCE(MAX(EXTRACT(EPOCH FROM now() - backend_start)), 0)::double precision as oldest_connection_duration,
                    COALESCE(AVG(EXTRACT(EPOCH FROM now() - backend_start)), 0)::double precision as average_connection_duration
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()"
                    .to_string(),
                Vec::new(),
            ),
        )])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL connection pool information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "connections"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresConnectionInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const UTILIZATION_THRESHOLD: f64 = 70.0;
    const IDLE_IN_TRANSACTION_THRESHOLD: f64 = 10.0;
    const IDLE_CONNECTION_THRESHOLD: f64 = 300.0; // 5 minutes
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut connection_info = PostgresConnectionInfo::default();
        let requests = self.request();

        // Execute core connection stats query
        if let Some(row) = run_single_row(&requests, "connection_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            connection_info.max_connections = row.get_u64("max_connections")?;
            connection_info.reserved_connections = row.get_u64("reserved_connections")?;
            connection_info.active_connections = row.get_u64("active_connections")?;
            connection_info.idle_connections = row.get_u64("idle_connections")?;
            connection_info.total_connections = row.get_u64("total_connections")?;
            connection_info.idle_in_transaction_connections = row.get_u64("idle_in_transaction")?;
            connection_info.idle_in_transaction_aborted = row.get_u64("idle_in_transaction_aborted")?;
            connection_info.oldest_connection_duration = row.get_f64("oldest_connection_duration")?;
            connection_info.average_connection_duration = row.get_f64("average_connection_duration")?;

            // Calculate derived metrics
            connection_info.connection_utilization = connection_info.calculate_utilization_percentage();
            connection_info.idle_in_transaction_percentage = connection_info.calculate_idle_in_transaction_percentage();
            connection_info.health_score = connection_info.calculate_health_score();
        }

        // Conditionally collect detailed metrics only when problems are detected
        connection_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&connection_info, context).await?;

        Ok(connection_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresConnectionInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresConnectionDetailedMetrics>> {
        let needs_breakdown = core_info.connection_utilization > Self::UTILIZATION_THRESHOLD;
        let needs_problematic_connections = core_info.idle_in_transaction_percentage > Self::IDLE_IN_TRANSACTION_THRESHOLD;

        if !needs_breakdown && !needs_problematic_connections {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresConnectionDetailedMetrics {
            connections_per_database: None,
            connections_per_user: None,
            connections_per_application: None,
            problematic_connections: None,
        };

        // Collect connection breakdowns if utilization is high
        if needs_breakdown {
            // Database breakdown
            let per_database_input = QueryInput::new(
                format!(
                    "SELECT
                    COALESCE(datname, 'unknown') as database_name,
                    COUNT(*) as total_connections,
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                GROUP BY datname
                ORDER BY total_connections DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows =
                run_query_with_timeout(&per_database_input, context.clone(), Self::QUERY_TIMEOUT, "connections_per_database").await?;
            detailed_metrics.connections_per_database = Some(Self::parse_connections_per_database(rows)?);

            // User breakdown
            let per_user_input = QueryInput::new(
                format!(
                    "SELECT
                    COALESCE(usename, 'unknown') as username,
                    COUNT(*) as total_connections,
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                GROUP BY usename
                ORDER BY total_connections DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&per_user_input, context.clone(), Self::QUERY_TIMEOUT, "connections_per_user").await?;
            detailed_metrics.connections_per_user = Some(Self::parse_connections_per_user(rows)?);

            // Application breakdown
            let per_application_input = QueryInput::new(
                format!(
                    "SELECT
                    COALESCE(application_name, 'unknown') as application_name,
                    COUNT(*) as total_connections,
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                GROUP BY application_name
                ORDER BY total_connections DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows =
                run_query_with_timeout(&per_application_input, context.clone(), Self::QUERY_TIMEOUT, "connections_per_application").await?;
            detailed_metrics.connections_per_application = Some(Self::parse_connections_per_application(rows)?);
        }

        // Collect problematic connections if needed
        if needs_problematic_connections {
            let problematic_input = QueryInput::new(
                format!(
                    "SELECT
                    pid, COALESCE(datname, 'unknown') as datname,
                    COALESCE(usename, 'unknown') as usename,
                    COALESCE(application_name, 'unknown') as application_name,
                    client_addr::text, state, backend_start,
                    EXTRACT(EPOCH FROM now() - backend_start) as connection_age,
                    EXTRACT(EPOCH FROM now() - state_change) as state_age
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                    AND (state = 'idle in transaction'
                         OR state = 'idle in transaction (aborted)'
                         OR (state = 'idle' AND EXTRACT(EPOCH FROM now() - state_change) > {}))
                ORDER BY state_age DESC
                LIMIT {}",
                    Self::IDLE_CONNECTION_THRESHOLD,
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&problematic_input, context.clone(), Self::QUERY_TIMEOUT, "problematic_connections").await?;
            detailed_metrics.problematic_connections = Some(Self::parse_problematic_connections(rows)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_connections_per_database(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresConnectionPerDatabase>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(PostgresConnectionPerDatabase {
                database_name: row.get_string("database_name")?,
                total_connections: row.get_u64("total_connections")?,
                active_connections: row.get_u64("active_connections")?,
                idle_connections: row.get_u64("idle_connections")?,
                idle_in_transaction: row.get_u64("idle_in_transaction")?,
            });
        }

        Ok(connections)
    }

    fn parse_connections_per_user(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresConnectionPerUser>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(PostgresConnectionPerUser {
                username: row.get_string("username")?,
                total_connections: row.get_u64("total_connections")?,
                active_connections: row.get_u64("active_connections")?,
                idle_connections: row.get_u64("idle_connections")?,
                idle_in_transaction: row.get_u64("idle_in_transaction")?,
            });
        }

        Ok(connections)
    }

    fn parse_connections_per_application(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresConnectionPerApplication>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(PostgresConnectionPerApplication {
                application_name: row.get_string("application_name")?,
                total_connections: row.get_u64("total_connections")?,
                active_connections: row.get_u64("active_connections")?,
                idle_connections: row.get_u64("idle_connections")?,
                idle_in_transaction: row.get_u64("idle_in_transaction")?,
            });
        }

        Ok(connections)
    }

    fn parse_problematic_connections(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresConnectionDetail>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(PostgresConnectionDetail {
                pid: row.get_i32("pid")?,
                database_name: Some(row.get_string("datname")?),
                username: Some(row.get_string("usename")?),
                application_name: Some(row.get_string("application_name")?),
                client_addr: row.get_opt_string("client_addr")?,
                state: PostgresConnectionState::from_string(&row.get_string("state")?),
                backend_start: row.get_datetime("backend_start")?,
                connection_age: row.get_f64("connection_age")?,
                state_age: row.get_f64("state_age")?,
            });
        }

        Ok(connections)
    }
}

impl PostgresConnectionInfo {
    /// Calculates the connection utilization as a percentage
    fn calculate_utilization_percentage(&self) -> f64 {
        if self.max_connections == 0 {
            0.0
        } else {
            (self.total_connections as f64 / self.max_connections as f64) * 100.0
        }
    }

    /// Calculates the percentage of connections idle in transaction
    fn calculate_idle_in_transaction_percentage(&self) -> f64 {
        if self.total_connections == 0 {
            0.0
        } else {
            ((self.idle_in_transaction_connections + self.idle_in_transaction_aborted) as f64 / self.total_connections as f64) * 100.0
        }
    }

    /// Calculates overall connection pool health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for high utilization
        if self.connection_utilization > 80.0 {
            score -= (self.connection_utilization - 80.0).min(20.0);
        }

        // Deduct for excessive idle in transaction
        if self.idle_in_transaction_percentage > 20.0 {
            score -= (self.idle_in_transaction_percentage - 20.0).min(25.0);
        }

        // Deduct for very old connections (potential leaks)
        if self.oldest_connection_duration > 86400.0 {
            // 24 hours
            score -= 15.0;
        } else if self.oldest_connection_duration > 3600.0 {
            // 1 hour
            score -= 5.0;
        }

        // Deduct for high average connection age
        if self.average_connection_duration > 1800.0 {
            // 30 minutes
            score -= 10.0;
        }

        score.max(0.0)
    }

    /// Checks if the connection limit is being approached
    pub fn is_approaching_limit(&self, threshold_percentage: f64) -> bool {
        self.connection_utilization > threshold_percentage
    }

    /// Checks if there are problematic idle-in-transaction connections
    pub fn has_excessive_idle_in_transaction(&self) -> bool {
        self.idle_in_transaction_percentage > Self::IDLE_IN_TRANSACTION_THRESHOLD
    }

    /// Gets available connection capacity
    pub fn available_connections(&self) -> u64 {
        self.max_connections.saturating_sub(self.total_connections)
    }

    /// Gets the percentage of connections that are idle
    pub fn idle_connection_percentage(&self) -> f64 {
        if self.total_connections == 0 {
            0.0
        } else {
            (self.idle_connections as f64 / self.total_connections as f64) * 100.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets connection health summary
    pub fn get_connection_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Connection pool is healthy".to_string(),
            75..=89 => "Good - Connection pool is performing well".to_string(),
            60..=74 => "Fair - Connection pool has some issues that should be monitored".to_string(),
            40..=59 => "Poor - Connection pool has significant problems".to_string(),
            _ => "Critical - Connection pool requires immediate attention".to_string(),
        }
    }
}

/// Connection statistics for a specific database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionPerDatabase {
    /// Database name
    pub database_name: String,
    /// Total connections to this database
    pub total_connections: u64,
    /// Active connections to this database
    pub active_connections: u64,
    /// Idle connections to this database
    pub idle_connections: u64,
    /// Connections idle in transaction
    pub idle_in_transaction: u64,
}

/// Connection statistics for a specific user
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionPerUser {
    /// Username
    pub username: String,
    /// Total connections from this user
    pub total_connections: u64,
    /// Active connections from this user
    pub active_connections: u64,
    /// Idle connections from this user
    pub idle_connections: u64,
    /// Connections idle in transaction
    pub idle_in_transaction: u64,
}

/// Connection statistics for a specific application
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionPerApplication {
    /// Application name from connection string
    pub application_name: String,
    /// Total connections from this application
    pub total_connections: u64,
    /// Active connections from this application
    pub active_connections: u64,
    /// Idle connections from this application
    pub idle_connections: u64,
    /// Connections idle in transaction
    pub idle_in_transaction: u64,
}

/// Simplified connection detail information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionDetail {
    /// Process ID of the backend
    pub pid: i32,
    /// Database name
    pub database_name: Option<String>,
    /// Username
    pub username: Option<String>,
    /// Application name from connection string
    pub application_name: Option<String>,
    /// Client IP address
    pub client_addr: Option<String>,
    /// Current state of the backend
    pub state: PostgresConnectionState,
    /// Time when the backend process started
    pub backend_start: DateTimeWrapper,
    /// Age of the connection (seconds)
    pub connection_age: f64,
    /// Time since last state change (seconds)
    pub state_age: f64,
}

/// PostgreSQL connection states
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq, Hash)]
pub enum PostgresConnectionState {
    /// Connection is executing a query
    Active,
    /// Connection is idle, waiting for a new command
    Idle,
    /// Connection is idle but within a transaction block
    IdleInTransaction,
    /// Connection is idle in a failed transaction block
    IdleInTransactionAborted,
    /// Connection is attempting to connect to the database
    FastpathFunctionCall,
    /// Connection is disabled
    Disabled,
    /// Unknown state (for forward compatibility)
    Unknown(String),
}

impl PostgresConnectionState {
    /// Parses a state string from pg_stat_activity into enum
    pub fn from_string(state_str: &str) -> Self {
        match state_str {
            "active" => PostgresConnectionState::Active,
            "idle" => PostgresConnectionState::Idle,
            "idle in transaction" => PostgresConnectionState::IdleInTransaction,
            "idle in transaction (aborted)" => PostgresConnectionState::IdleInTransactionAborted,
            "fastpath function call" => PostgresConnectionState::FastpathFunctionCall,
            "disabled" => PostgresConnectionState::Disabled,
            _ => PostgresConnectionState::Unknown(state_str.to_string()),
        }
    }

    /// Checks if this state represents an idle connection
    pub fn is_idle(&self) -> bool {
        matches!(
            self,
            PostgresConnectionState::Idle | PostgresConnectionState::IdleInTransaction | PostgresConnectionState::IdleInTransactionAborted
        )
    }

    /// Checks if this state represents a problematic idle state
    pub fn is_problematic(&self) -> bool {
        matches!(self, PostgresConnectionState::IdleInTransaction | PostgresConnectionState::IdleInTransactionAborted)
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_connection_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let connection_info = PostgresConnectionInfo::default();

        let result = connection_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.max_connections > 0);
        assert!(info.connection_utilization >= 0.0);
        assert!(info.connection_utilization <= 100.0);
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
    }
}
