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

/// PostgreSQL database activity information and statistics
///
/// Simplified struct containing essential metrics about current database activity.
/// Focuses on core connection health and performance indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresActivityInfo {
    /// Total number of active client connections
    pub active_connections: u64,
    /// Number of idle connections
    pub idle_connections: u64,
    /// Number of connections idle in transaction
    pub idle_in_transaction: u64,
    /// Total number of connections (including background processes)
    pub total_connections: u64,
    /// Maximum allowed connections from postgresql.conf
    pub max_connections: u64,
    /// Percentage of connection limit being used (0.0 to 100.0)
    pub connection_utilization_pct: f64,
    /// Duration of the longest running query in seconds
    pub longest_query_duration: f64,
    /// Duration of the longest running transaction in seconds
    pub longest_transaction_duration: f64,
    /// Average query duration across all active queries
    pub avg_active_query_duration: f64,
    /// Number of queries currently waiting for locks
    pub waiting_queries_count: u64,
    /// Number of queries that are actively blocking others
    pub blocking_queries_count: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedMetrics {
    /// Active queries (only collected when longest_query_duration > threshold)
    pub long_running_queries: Vec<PostgresActiveQuery>,
    /// Blocking relationships (only collected when blocking_queries_count > 0)
    pub blocked_queries: Vec<PostgresBlockedQuery>,
    /// Connection breakdown by database (collected less frequently)
    pub connections_by_database: Option<Vec<PostgresConnectionsByDatabase>>,
}

impl MetadataCollection for PostgresActivityInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            ("core_stats".to_string(),
             QueryInput::new(
                 "SELECT
                    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                    COUNT(*) as total_connections,
                    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
                    COALESCE(MAX(EXTRACT(EPOCH FROM now() - query_start)), 0)::double precision as longest_query_duration,
                    COALESCE(MAX(EXTRACT(EPOCH FROM now() - xact_start)), 0)::double precision as longest_transaction_duration,
                    COALESCE(AVG(EXTRACT(EPOCH FROM now() - query_start)) FILTER (WHERE state = 'active'), 0)::double precision as avg_active_query_duration,
                    COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state = 'active') as waiting_queries_count
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()".to_string(),
                 Vec::new())
            ),
            ("blocking_count".to_string(),
             QueryInput::new(
                 "SELECT COUNT(DISTINCT blocking_locks.pid) as blocking_count
                 FROM pg_catalog.pg_locks blocked_locks
                 JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                     AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                     AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                     AND blocking_locks.pid != blocked_locks.pid
                 WHERE NOT blocked_locks.granted".to_string(),
                 Vec::new())
            )
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential database activity metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "activity"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresActivityInfo {
    const LONG_QUERY_THRESHOLD: f64 = 30.0; // 30 seconds
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5); // Reduced from 10s
    const MAX_DETAILED_RESULTS: usize = 50; // Reduced from 100

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut activity_info = PostgresActivityInfo::default();
        let requests = self.request();

        // Execute core stats query
        if let Some(row) = run_single_row(&requests, "core_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            activity_info.active_connections = row.get_u64("active_connections")?;
            activity_info.idle_connections = row.get_u64("idle_connections")?;
            activity_info.idle_in_transaction = row.get_u64("idle_in_transaction")?;
            activity_info.total_connections = row.get_u64("total_connections")?;
            activity_info.max_connections = row.get_u64("max_connections")?;
            activity_info.longest_query_duration = row.get_f64("longest_query_duration")?;
            activity_info.longest_transaction_duration = row.get_f64("longest_transaction_duration")?;
            activity_info.avg_active_query_duration = row.get_f64("avg_active_query_duration")?;
            activity_info.waiting_queries_count = row.get_u64("waiting_queries_count")?;

            // Calculate connection utilization percentage
            activity_info.connection_utilization_pct = if activity_info.max_connections > 0 {
                (activity_info.total_connections as f64 / activity_info.max_connections as f64) * 100.0
            } else {
                0.0
            };
        }

        // Get blocking count
        if let Some(row) = run_single_row(&requests, "blocking_count", context.clone(), Self::QUERY_TIMEOUT).await? {
            activity_info.blocking_queries_count = row.get_u64("blocking_count")?;
        }

        // Conditionally collect detailed metrics only when problems are detected
        activity_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&activity_info, context).await?;

        Ok(activity_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresActivityInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedMetrics>> {
        let needs_long_query_details = core_info.longest_query_duration > Self::LONG_QUERY_THRESHOLD;
        let needs_blocking_details = core_info.blocking_queries_count > 0;

        if !needs_long_query_details && !needs_blocking_details {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresDetailedMetrics {
            long_running_queries: Vec::new(),
            blocked_queries: Vec::new(),
            connections_by_database: None,
        };

        // Collect long-running queries if needed
        if needs_long_query_details {
            let long_query_input = QueryInput::new(
                format!(
                    "SELECT
                    pid, COALESCE(datname, 'unknown') as datname,
                    COALESCE(usename, 'unknown') as usename,
                    LEFT(query, 500) as query,  -- Truncate immediately in SQL
                    EXTRACT(EPOCH FROM now() - query_start) as duration,
                    state, application_name, query_start, backend_type
                FROM pg_stat_activity
                WHERE state = 'active'
                    AND pid != pg_backend_pid()
                    AND EXTRACT(EPOCH FROM now() - query_start) > {}
                    AND query NOT LIKE '%pg_stat_activity%'
                ORDER BY query_start ASC
                LIMIT {}",
                    Self::LONG_QUERY_THRESHOLD,
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&long_query_input, context.clone(), Self::QUERY_TIMEOUT, "long_running_queries").await?;
            detailed_metrics.long_running_queries = Self::parse_long_running_queries(rows)?;
        }

        // Collect blocking details if needed
        if needs_blocking_details {
            let blocking_query_input = QueryInput::new(
                format!(
                    "SELECT
                    blocked_locks.pid AS blocked_pid,
                    blocking_locks.pid AS blocking_pid,
                    LEFT(blocked_activity.query, 300) AS blocked_query,
                    LEFT(blocking_activity.query, 300) AS blocking_query,
                    blocked_locks.locktype AS lock_type,
                    blocked_locks.mode AS lock_mode,
                    EXTRACT(EPOCH FROM now() - blocked_activity.query_start) as blocked_duration,
                    COALESCE(blocked_activity.datname, 'unknown') as database_name,
                    COALESCE(blocked_activity.usename, 'unknown') as username,
                    COALESCE(c.relname, 'unknown') as relation_name
                FROM pg_catalog.pg_locks blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.pid != blocked_locks.pid
                JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
                LEFT JOIN pg_catalog.pg_class c ON c.oid = blocked_locks.relation
                WHERE NOT blocked_locks.granted
                ORDER BY blocked_duration DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&blocking_query_input, context.clone(), Self::QUERY_TIMEOUT, "blocked_queries").await?;
            detailed_metrics.blocked_queries = Self::parse_blocked_queries(rows)?;
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_long_running_queries(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresActiveQuery>> {
        let mut queries = Vec::with_capacity(rows.len());

        for row in rows {
            queries.push(PostgresActiveQuery {
                pid: row.get_i32("pid")?,
                database: row.get_string("datname")?,
                username: row.get_string("usename")?,
                query: row.get_string("query")?, // Already truncated in SQL
                duration: row.get_f64("duration")?,
                state: row.get_string("state")?,
                application_name: row.get_opt_string("application_name")?,
                query_start: row.get_datetime("query_start")?,
                backend_type: row.get_string("backend_type")?,
                // Simplified struct - removed fields not essential for monitoring
            });
        }

        Ok(queries)
    }

    fn parse_blocked_queries(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresBlockedQuery>> {
        let mut queries = Vec::with_capacity(rows.len());

        for row in rows {
            queries.push(PostgresBlockedQuery {
                blocked_pid: row.get_i32("blocked_pid")?,
                blocking_pid: row.get_i32("blocking_pid")?,
                blocked_query: row.get_string("blocked_query")?, // Already truncated in SQL
                blocking_query: row.get_string("blocking_query")?,
                lock_type: row.get_string("lock_type")?,
                lock_mode: row.get_string("lock_mode")?,
                blocked_duration: row.get_f64("blocked_duration")?,
                database_name: row.get_string("database_name")?,
                username: row.get_string("username")?,
                relation_name: row.get_string("relation_name")?,
            });
        }

        Ok(queries)
    }
}

/// Simplified active query information
///
/// Contains only essential information about long-running queries.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresActiveQuery {
    /// Process ID of the backend executing this query
    pub pid: i32,
    /// Database name where the query is executing
    pub database: String,
    /// Username executing the query
    pub username: String,
    /// SQL query text (truncated for safety)
    pub query: String,
    /// Duration the query has been running (seconds)
    pub duration: f64,
    /// Current state of the backend
    pub state: String,
    /// Application name from connection string
    pub application_name: Option<String>,
    /// Time when the query started
    pub query_start: DateTimeWrapper,
    /// Type of backend process
    pub backend_type: String,
}

/// Information about blocking query relationships
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresBlockedQuery {
    /// Process ID of the blocked backend
    pub blocked_pid: i32,
    /// Process ID of the backend causing the block
    pub blocking_pid: i32,
    /// SQL query that is being blocked (truncated)
    pub blocked_query: String,
    /// SQL query that is causing the block (truncated)
    pub blocking_query: String,
    /// Type of lock causing the block
    pub lock_type: String,
    /// Lock mode that is conflicting
    pub lock_mode: String,
    /// Duration the query has been blocked (seconds)
    pub blocked_duration: f64,
    /// Database where the blocking is occurring
    pub database_name: String,
    /// Username of the blocked query
    pub username: String,
    /// Table or relation name involved in the lock
    pub relation_name: String,
}

/// Connection statistics grouped by database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresConnectionsByDatabase {
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

impl PostgresActivityInfo {
    /// Calculates the percentage of connections that are currently active
    pub fn active_connection_percentage(&self) -> f64 {
        if self.total_connections == 0 {
            0.0
        } else {
            (self.active_connections as f64 / self.total_connections as f64) * 100.0
        }
    }

    /// Checks if there are long-running queries
    pub fn has_long_running_queries(&self, threshold_seconds: f64) -> bool {
        self.longest_query_duration > threshold_seconds
    }

    /// Checks if there are long-running transactions
    pub fn has_long_running_transactions(&self, threshold_seconds: f64) -> bool {
        self.longest_transaction_duration > threshold_seconds
    }

    /// Checks if there are blocking queries
    pub fn has_blocking_queries(&self) -> bool {
        self.blocking_queries_count > 0
    }

    /// Checks if connection limit is being approached
    pub fn is_approaching_connection_limit(&self, threshold_percentage: f64) -> bool {
        self.connection_utilization_pct > threshold_percentage
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_metadata_activity() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let activity_info = PostgresActivityInfo::default();

        let result = activity_info
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
        assert!(info.connection_utilization_pct >= 0.0);
        assert!(info.connection_utilization_pct <= 100.0);
    }
}
