use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL lock information and blocking query analysis
///
/// Simplified struct containing essential metrics about database locks,
/// blocking relationships, and overall lock health.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLockInfo {
    /// Total number of locks currently held
    pub total_locks: u64,
    /// Number of locks that have been granted
    pub granted_locks: u64,
    /// Number of locks currently waiting to be granted
    pub waiting_locks: u64,
    /// Percentage of locks that are waiting (0.0 to 100.0)
    pub waiting_locks_percentage: f64,
    /// Number of queries currently blocked by locks
    pub blocked_queries_count: u64,
    /// Total number of deadlocks detected (cumulative)
    pub deadlock_count: u64,
    /// Longest current lock wait time (seconds)
    pub max_lock_wait_time: f64,
    /// Number of sessions that are blocking others
    pub blocking_sessions_count: u64,
    /// Overall lock health score (0.0 to 100.0, higher is better)
    pub health_score: f64,
    /// Detailed metrics collected only when lock issues are detected
    pub detailed_metrics: Option<PostgresLockDetailedMetrics>,
}

/// Detailed lock metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLockDetailedMetrics {
    /// Lock breakdown by type (collected when waiting_locks > 5)
    pub locks_by_type: Option<Vec<PostgresLockByType>>,
    /// Active blocking sessions (collected when blocking_sessions_count > 0)
    pub blocking_sessions: Option<Vec<PostgresBlockingSession>>,
    /// Current lock waits (collected when waiting_locks > 0)
    pub lock_waits: Option<Vec<PostgresLockWait>>,
    /// Long-running locks (collected when max_lock_wait_time > 30s)
    pub long_running_locks: Option<Vec<PostgresLongRunningLock>>,
    /// Lock resolution recommendations
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresLockInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "lock_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as total_locks,
                    COUNT(*) FILTER (WHERE granted = true) as granted_locks,
                    COUNT(*) FILTER (WHERE granted = false) as waiting_locks,
                    COALESCE(
                        (SELECT SUM(deadlocks) FROM pg_stat_database),
                        0
                    )::bigint as deadlock_count
                FROM pg_locks"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "blocking_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(DISTINCT blocking_locks.pid) as blocking_sessions_count,
                    COUNT(DISTINCT blocked_locks.pid) as blocked_queries_count,
                    COALESCE(MAX(EXTRACT(EPOCH FROM now() - blocked_activity.query_start)), 0)::double precision as max_wait_time
                FROM pg_catalog.pg_locks blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.pid != blocked_locks.pid
                WHERE NOT blocked_locks.granted AND blocking_locks.granted"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL lock information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "locks"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresLockInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(3); // Shorter timeout for lock queries
    const WAITING_LOCKS_THRESHOLD: u64 = 5;
    const LONG_WAIT_THRESHOLD: f64 = 30.0; // 30 seconds
    const MAX_DETAILED_RESULTS: usize = 20;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut lock_info = PostgresLockInfo::default();
        let requests = self.request();

        // Execute lock summary
        if let Some(row) = run_single_row(&requests, "lock_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.total_locks = Self::safe_i64_to_u64(&row, "total_locks")?;
            lock_info.granted_locks = Self::safe_i64_to_u64(&row, "granted_locks")?;
            lock_info.waiting_locks = Self::safe_i64_to_u64(&row, "waiting_locks")?;
            lock_info.deadlock_count = Self::safe_i64_to_u64(&row, "deadlock_count")?;

            // Calculate waiting percentage
            lock_info.waiting_locks_percentage = if lock_info.total_locks > 0 {
                (lock_info.waiting_locks as f64 / lock_info.total_locks as f64) * 100.0
            } else {
                0.0
            };
        }

        // Parse blocking summary
        if let Some(row) = run_single_row(&requests, "blocking_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.blocking_sessions_count = Self::safe_i64_to_u64(&row, "blocking_sessions_count")?;
            lock_info.blocked_queries_count = Self::safe_i64_to_u64(&row, "blocked_queries_count")?;
            lock_info.max_lock_wait_time = Self::safe_get_f64(&row, "max_wait_time")?;
        }

        // Calculate health score
        lock_info.health_score = lock_info.calculate_health_score();

        // Conditionally collect detailed metrics only when lock issues are detected
        lock_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&lock_info, context).await?;

        Ok(lock_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresLockInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresLockDetailedMetrics>> {
        let needs_lock_breakdown = core_info.waiting_locks > Self::WAITING_LOCKS_THRESHOLD;
        let needs_blocking_details = core_info.blocking_sessions_count > 0;
        let needs_wait_details = core_info.waiting_locks > 0;
        let needs_long_lock_details = core_info.max_lock_wait_time > Self::LONG_WAIT_THRESHOLD;

        if !needs_lock_breakdown && !needs_blocking_details && !needs_wait_details && !needs_long_lock_details {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresLockDetailedMetrics {
            locks_by_type: None,
            blocking_sessions: None,
            lock_waits: None,
            long_running_locks: None,
            recommendations: core_info.generate_recommendations(),
        };

        // Collect lock breakdown by type
        if needs_lock_breakdown {
            let locks_by_type_input = QueryInput::new(
                "SELECT
                    locktype,
                    COUNT(*) as total_count,
                    COUNT(*) FILTER (WHERE granted = true) as granted_count,
                    COUNT(*) FILTER (WHERE granted = false) as waiting_count
                FROM pg_locks
                GROUP BY locktype
                ORDER BY waiting_count DESC, total_count DESC
                LIMIT 10"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&locks_by_type_input, context.clone(), Self::QUERY_TIMEOUT, "locks_by_type").await {
                detailed_metrics.locks_by_type = Some(Self::parse_locks_by_type(rows)?);
            }
        }

        // Collect blocking session details
        if needs_blocking_details {
            let blocking_sessions_input = QueryInput::new(
                format!(
                    "SELECT
                    blocking_locks.pid AS blocking_pid,
                    blocking_activity.query AS blocking_query,
                    blocking_activity.datname AS database_name,
                    blocking_activity.usename AS username,
                    blocking_activity.application_name,
                    COUNT(DISTINCT blocked_locks.pid) AS blocked_count,
                    MAX(EXTRACT(EPOCH FROM now() - blocked_activity.query_start)) AS longest_wait_duration,
                    blocking_locks.locktype AS lock_type,
                    blocking_locks.mode AS lock_mode,
                    EXTRACT(EPOCH FROM now() - blocking_activity.query_start) as blocking_query_duration
                FROM pg_catalog.pg_locks blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.pid != blocked_locks.pid
                JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
                WHERE NOT blocked_locks.granted AND blocking_locks.granted
                GROUP BY blocking_locks.pid, blocking_activity.query, blocking_activity.datname,
                         blocking_activity.usename, blocking_activity.application_name,
                         blocking_locks.locktype, blocking_locks.mode, blocking_activity.query_start
                ORDER BY blocked_count DESC, longest_wait_duration DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) =
                run_query_with_timeout(&blocking_sessions_input, context.clone(), Self::QUERY_TIMEOUT, "blocking_sessions").await
            {
                detailed_metrics.blocking_sessions = Some(Self::parse_blocking_sessions(rows)?);
            }
        }

        // Collect lock wait details
        if needs_wait_details {
            let lock_waits_input = QueryInput::new(
                format!(
                    "SELECT
                    blocked_locks.pid AS waiting_pid,
                    blocking_locks.pid AS blocking_pid,
                    LEFT(blocked_activity.query, 200) AS waiting_query,
                    LEFT(blocking_activity.query, 200) AS blocking_query,
                    EXTRACT(EPOCH FROM now() - blocked_activity.query_start) AS wait_duration,
                    blocked_locks.locktype AS lock_type,
                    blocked_locks.mode AS lock_mode,
                    COALESCE(blocked_activity.datname, 'unknown') as database_name,
                    COALESCE(blocked_activity.usename, 'unknown') as username
                FROM pg_catalog.pg_locks blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.pid != blocked_locks.pid
                JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
                WHERE NOT blocked_locks.granted AND blocking_locks.granted
                ORDER BY wait_duration DESC
                LIMIT {}",
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&lock_waits_input, context.clone(), Self::QUERY_TIMEOUT, "lock_waits").await {
                detailed_metrics.lock_waits = Some(Self::parse_lock_waits(rows)?);
            }
        }

        // Collect long-running lock details
        if needs_long_lock_details {
            let long_locks_input = QueryInput::new(
                format!(
                    "SELECT
                    l.pid,
                    LEFT(a.query, 200) AS query,
                    EXTRACT(EPOCH FROM now() - a.query_start) AS lock_duration,
                    l.locktype,
                    l.mode,
                    COALESCE(a.datname, 'unknown') as database_name,
                    COALESCE(a.usename, 'unknown') as username,
                    a.application_name
                FROM pg_locks l
                JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE l.granted = true
                    AND EXTRACT(EPOCH FROM now() - a.query_start) > {}
                    AND a.state = 'active'
                ORDER BY lock_duration DESC
                LIMIT {}",
                    Self::LONG_WAIT_THRESHOLD,
                    Self::MAX_DETAILED_RESULTS
                ),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&long_locks_input, context.clone(), Self::QUERY_TIMEOUT, "long_running_locks").await {
                detailed_metrics.long_running_locks = Some(Self::parse_long_running_locks(rows)?);
            }
        }

        Ok(Some(detailed_metrics))
    }

    // Helper functions for safe type conversion and extraction
    fn safe_i64_to_u64(row: &PgSimpleRow, column: &str) -> ResultEP<u64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        let value = text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))?;

        if value < 0 {
            return Err(EpError::metadata(format!("Negative value for {}: {}", column, value)));
        }
        Ok(value as u64)
    }

    fn safe_get_f64(row: &PgSimpleRow, column: &str) -> ResultEP<f64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<f64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn parse_locks_by_type(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresLockByType>> {
        let mut locks_by_type = Vec::with_capacity(rows.len());

        for row in rows {
            locks_by_type.push(PostgresLockByType {
                lock_type: Self::safe_get_string(&row, "locktype")?,
                total_count: Self::safe_i64_to_u64(&row, "total_count")?,
                granted_count: Self::safe_i64_to_u64(&row, "granted_count")?,
                waiting_count: Self::safe_i64_to_u64(&row, "waiting_count")?,
            });
        }

        Ok(locks_by_type)
    }

    fn parse_blocking_sessions(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresBlockingSession>> {
        let mut blocking_sessions = Vec::with_capacity(rows.len());

        for row in rows {
            blocking_sessions.push(PostgresBlockingSession {
                blocking_pid: Self::safe_get_i32(&row, "blocking_pid")?,
                blocking_query: Self::safe_get_string(&row, "blocking_query")?,
                database_name: Self::safe_get_optional_string(&row, "database_name")?,
                username: Self::safe_get_optional_string(&row, "username")?,
                application_name: Self::safe_get_optional_string(&row, "application_name")?,
                blocked_count: Self::safe_i64_to_u64(&row, "blocked_count")?,
                longest_wait_duration: Self::safe_get_f64(&row, "longest_wait_duration")?,
                lock_type: Self::safe_get_string(&row, "lock_type")?,
                lock_mode: Self::safe_get_string(&row, "lock_mode")?,
                blocking_query_duration: Self::safe_get_f64(&row, "blocking_query_duration")?,
            });
        }

        Ok(blocking_sessions)
    }

    fn parse_lock_waits(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresLockWait>> {
        let mut lock_waits = Vec::with_capacity(rows.len());

        for row in rows {
            lock_waits.push(PostgresLockWait {
                waiting_pid: Self::safe_get_i32(&row, "waiting_pid")?,
                blocking_pid: Self::safe_get_i32(&row, "blocking_pid")?,
                waiting_query: Self::safe_get_string(&row, "waiting_query")?,
                blocking_query: Self::safe_get_string(&row, "blocking_query")?,
                wait_duration: Self::safe_get_f64(&row, "wait_duration")?,
                lock_type: Self::safe_get_string(&row, "lock_type")?,
                lock_mode: Self::safe_get_string(&row, "lock_mode")?,
                database_name: Self::safe_get_string(&row, "database_name")?,
                username: Self::safe_get_string(&row, "username")?,
            });
        }

        Ok(lock_waits)
    }

    fn parse_long_running_locks(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresLongRunningLock>> {
        let mut long_running_locks = Vec::with_capacity(rows.len());

        for row in rows {
            long_running_locks.push(PostgresLongRunningLock {
                pid: Self::safe_get_i32(&row, "pid")?,
                query: Self::safe_get_string(&row, "query")?,
                lock_duration: Self::safe_get_f64(&row, "lock_duration")?,
                lock_type: Self::safe_get_string(&row, "locktype")?,
                lock_mode: Self::safe_get_string(&row, "mode")?,
                database_name: Self::safe_get_string(&row, "database_name")?,
                username: Self::safe_get_string(&row, "username")?,
                application_name: Self::safe_get_optional_string(&row, "application_name")?,
            });
        }

        Ok(long_running_locks)
    }
}

impl PostgresLockInfo {
    /// Calculates overall lock health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for high waiting locks percentage
        if self.waiting_locks_percentage > 10.0 {
            score -= (self.waiting_locks_percentage - 10.0) * 2.0;
        }

        // Deduct for blocking sessions
        if self.blocking_sessions_count > 0 {
            score -= (self.blocking_sessions_count as f64 * 5.0).min(30.0);
        }

        // Deduct for long wait times
        if self.max_lock_wait_time > 30.0 {
            score -= ((self.max_lock_wait_time - 30.0) / 10.0).min(25.0);
        }

        // Deduct for deadlocks
        if self.deadlock_count > 0 {
            score -= (self.deadlock_count as f64 * 2.0).min(20.0);
        }

        // Deduct for high absolute number of waiting locks
        if self.waiting_locks > 20 {
            score -= ((self.waiting_locks - 20) as f64 * 0.5).min(15.0);
        }

        score.max(0.0)
    }

    /// Generates lock resolution recommendations
    fn generate_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.waiting_locks_percentage > 20.0 {
            recommendations
                .push("High percentage of waiting locks - investigate blocking queries and consider query optimization".to_string());
        }

        if self.blocking_sessions_count > 3 {
            recommendations.push(format!(
                "{} sessions are blocking others - identify and optimize long-running transactions",
                self.blocking_sessions_count
            ));
        }

        if self.max_lock_wait_time > 60.0 {
            recommendations.push(format!(
                "Long lock wait detected ({:.1}s) - investigate blocking query and consider killing if necessary",
                self.max_lock_wait_time
            ));
        }

        if self.deadlock_count > 10 {
            recommendations.push("High deadlock count - review application transaction logic and lock ordering".to_string());
        }

        if self.waiting_locks > 50 {
            recommendations.push("Many concurrent lock waits - consider connection pooling and transaction optimization".to_string());
        }

        if self.total_locks > 1000 && self.waiting_locks_percentage > 5.0 {
            recommendations.push("High lock contention in busy system - consider partitioning or query optimization".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Lock system appears healthy".to_string());
        }

        recommendations
    }

    /// Checks if there are excessive lock waits
    pub fn has_excessive_lock_waits(&self) -> bool {
        self.waiting_locks_percentage > 10.0 || self.waiting_locks > 20
    }

    /// Checks if there are long-running lock waits
    pub fn has_long_lock_waits(&self) -> bool {
        self.max_lock_wait_time > Self::LONG_WAIT_THRESHOLD
    }

    /// Checks if deadlock rate is concerning
    pub fn has_excessive_deadlocks(&self) -> bool {
        self.deadlock_count > 10
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets lock health summary
    pub fn get_lock_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - No significant lock contention".to_string(),
            75..=89 => "Good - Minor lock contention detected".to_string(),
            60..=74 => "Fair - Some lock issues that should be monitored".to_string(),
            40..=59 => "Poor - Significant lock contention detected".to_string(),
            _ => "Critical - Severe lock contention requires immediate attention".to_string(),
        }
    }

    /// Gets lock efficiency (percentage of granted locks)
    pub fn get_lock_efficiency(&self) -> f64 {
        if self.total_locks == 0 {
            100.0
        } else {
            (self.granted_locks as f64 / self.total_locks as f64) * 100.0
        }
    }
}

/// Lock statistics grouped by lock type
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLockByType {
    /// Type of lock (relation, tuple, transactionid, virtualxid, etc.)
    pub lock_type: String,
    /// Total number of locks of this type
    pub total_count: u64,
    /// Number of granted locks of this type
    pub granted_count: u64,
    /// Number of waiting locks of this type
    pub waiting_count: u64,
}

/// Information about a session that is blocking other sessions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresBlockingSession {
    /// Process ID of the blocking session
    pub blocking_pid: i32,
    /// SQL query being executed by the blocking session
    pub blocking_query: String,
    /// Database name where blocking is occurring
    pub database_name: Option<String>,
    /// Username of the blocking session
    pub username: Option<String>,
    /// Application name of the blocking session
    pub application_name: Option<String>,
    /// Number of sessions being blocked
    pub blocked_count: u64,
    /// Duration of the longest wait caused by this session (seconds)
    pub longest_wait_duration: f64,
    /// Type of lock being held
    pub lock_type: String,
    /// Mode of the lock being held
    pub lock_mode: String,
    /// Duration the blocking query has been running (seconds)
    pub blocking_query_duration: f64,
}

/// Detailed information about a specific lock wait
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLockWait {
    /// Process ID of the waiting session
    pub waiting_pid: i32,
    /// Process ID of the session holding the lock
    pub blocking_pid: i32,
    /// SQL query that is waiting (truncated)
    pub waiting_query: String,
    /// SQL query that is holding the lock (truncated)
    pub blocking_query: String,
    /// Duration this query has been waiting (seconds)
    pub wait_duration: f64,
    /// Type of lock being waited for
    pub lock_type: String,
    /// Mode of the lock being waited for
    pub lock_mode: String,
    /// Database where the lock wait is occurring
    pub database_name: String,
    /// Username of the waiting session
    pub username: String,
}

/// Information about long-running locks
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLongRunningLock {
    /// Process ID holding the lock
    pub pid: i32,
    /// SQL query holding the lock (truncated)
    pub query: String,
    /// Duration the lock has been held (seconds)
    pub lock_duration: f64,
    /// Type of lock being held
    pub lock_type: String,
    /// Mode of the lock being held
    pub lock_mode: String,
    /// Database where the lock is held
    pub database_name: String,
    /// Username holding the lock
    pub username: String,
    /// Application name holding the lock
    pub application_name: Option<String>,
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_lock_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let lock_info = PostgresLockInfo::default();

        let result = lock_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
        assert!(info.waiting_locks_percentage >= 0.0);
        assert!(info.waiting_locks_percentage <= 100.0);
    }
}
