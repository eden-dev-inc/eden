use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL transaction information and statistics
///
/// This struct contains comprehensive metrics about database transactions,
/// including commit/rollback rates, deadlocks, long-running transactions,
/// and transaction ID management. Critical for monitoring database health
/// and preventing transaction wraparound issues.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTransactionInfo {
    /// Total number of transactions committed
    pub transactions_committed: u64,
    /// Total number of transactions rolled back
    pub transactions_rolled_back: u64,
    /// Total transactions (committed + rolled back)
    pub total_transactions: u64,
    /// Commit ratio (commits / total transactions)
    pub commit_ratio: f64,
    /// Total number of deadlocks detected
    pub deadlocks_total: u64,
    /// Deadlock rate per 1000 transactions
    pub deadlock_rate_per_transaction: f64,
    /// Current transaction ID
    pub current_xid: Option<u64>,
    /// Distance to transaction wraparound
    pub xid_until_wraparound: Option<i64>,
    /// Whether close to transaction wraparound
    pub is_wraparound_warning: bool,
    /// Number of long-running transactions
    pub long_running_transactions_count: u64,
    /// Number of prepared transactions
    pub prepared_transactions_count: u64,
    /// Oldest transaction age in seconds
    pub oldest_transaction_age: f64,
    /// Transaction health score (0-100)
    pub transaction_health_score: f64,
    /// Whether system needs immediate attention
    pub needs_immediate_attention: bool,
    /// Transaction system status
    pub system_status: String,
    /// Detailed transaction information (collected conditionally)
    pub detailed_transaction_info: Option<PostgresDetailedTransactionInfo>,
}

/// Detailed transaction information collected only when issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedTransactionInfo {
    /// Long-running transactions
    pub long_running_transactions: Vec<PostgresLongTransaction>,
    /// Transaction statistics per database
    pub transactions_by_database: Vec<PostgresTransactionsByDatabase>,
    /// Prepared transactions
    pub prepared_transactions: Vec<PostgresPreparedTransaction>,
    /// Transaction age distribution
    pub transaction_age_stats: PostgresTransactionAgeStats,
    /// Multixact information
    pub multixact_info: PostgresMultixactInfo,
    /// Transaction warnings and recommendations
    pub warnings: Vec<String>,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresTransactionInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "transaction_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COALESCE(SUM(xact_commit)::bigint, 0::bigint) as total_commits,
                    COALESCE(SUM(xact_rollback)::bigint, 0::bigint) as total_rollbacks,
                    COALESCE(SUM(deadlocks)::bigint, 0::bigint) as total_deadlocks,
                    COALESCE((COALESCE(SUM(xact_commit), 0) + COALESCE(SUM(xact_rollback), 0))::bigint, 0::bigint) as total_transactions,
                    CASE WHEN SUM(xact_commit + xact_rollback) > 0 THEN
                        (SUM(xact_commit)::numeric / SUM(xact_commit + xact_rollback)::numeric)::double precision
                    ELSE 1.0 END as commit_ratio
                FROM pg_stat_database
                WHERE datname IS NOT NULL"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "xid_info".to_string(),
                QueryInput::new(
                    "SELECT
                    txid_current() as current_xid,
                    (SELECT setting::bigint FROM pg_settings WHERE name = 'autovacuum_freeze_max_age') as freeze_max_age,
                    COALESCE(MAX(age(datfrozenxid))::bigint, 0::bigint) as max_xid_age
                FROM pg_database
                WHERE datname IS NOT NULL"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "active_transactions".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as long_running_count,
                    COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (now() - xact_start)) > 1800) as very_long_count,
                    MAX(EXTRACT(EPOCH FROM (now() - xact_start))) as oldest_age,
                    AVG(EXTRACT(EPOCH FROM (now() - xact_start))) as avg_age
                FROM pg_stat_activity
                WHERE xact_start IS NOT NULL
                    AND EXTRACT(EPOCH FROM (now() - xact_start)) > 300"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "prepared_count".to_string(),
                QueryInput::new("SELECT COUNT(*) as prepared_count FROM pg_prepared_xacts".to_string(), Vec::new()),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL transaction information including commit/rollback rates, deadlocks, and wraparound status"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "transactions"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresTransactionInfo {
    const LONG_TRANSACTION_THRESHOLD: f64 = 300.0; // 5 minutes
    const VERY_LONG_TRANSACTION_THRESHOLD: f64 = 1800.0; // 30 minutes
    const XID_WRAPAROUND_WARNING_THRESHOLD: i64 = 10_000_000;
    const COMMIT_RATIO_WARNING_THRESHOLD: f64 = 0.95;
    const DEADLOCK_RATE_WARNING_THRESHOLD: f64 = 1.0; // per 1000 transactions
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut transaction_info = PostgresTransactionInfo::default();
        let requests = self.request();

        // Execute core summary queries
        let transaction_summary_row = run_single_row(&requests, "transaction_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        let xid_info_row = run_single_row(&requests, "xid_info", context.clone(), Self::QUERY_TIMEOUT).await?;

        let active_transactions_row = run_single_row(&requests, "active_transactions", context.clone(), Self::QUERY_TIMEOUT).await?;

        let prepared_count_row = run_single_row(&requests, "prepared_count", context.clone(), Self::QUERY_TIMEOUT).await?;

        // Process transaction summary
        if let Some(row) = transaction_summary_row {
            transaction_info.transactions_committed = Self::safe_i64_to_u64(&row, "total_commits")?;
            transaction_info.transactions_rolled_back = Self::safe_i64_to_u64(&row, "total_rollbacks")?;
            transaction_info.total_transactions = Self::safe_i64_to_u64(&row, "total_transactions")?;
            transaction_info.commit_ratio = Self::safe_get_f64(&row, "commit_ratio")?;
            transaction_info.deadlocks_total = Self::safe_i64_to_u64(&row, "total_deadlocks")?;
        }

        // Process XID information
        if let Some(row) = xid_info_row {
            transaction_info.current_xid = Some(Self::safe_i64_to_u64(&row, "current_xid")?);
            let freeze_max_age = Self::safe_get_i64(&row, "freeze_max_age")?;
            let max_xid_age = Self::safe_get_i64(&row, "max_xid_age")?;

            transaction_info.xid_until_wraparound = Some(freeze_max_age - max_xid_age);
            transaction_info.is_wraparound_warning = (freeze_max_age - max_xid_age) < Self::XID_WRAPAROUND_WARNING_THRESHOLD;
        }

        // Process active transactions
        if let Some(row) = active_transactions_row {
            transaction_info.long_running_transactions_count = Self::safe_i64_to_u64(&row, "long_running_count")?;
            transaction_info.oldest_transaction_age = row.get("oldest_age").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        }

        // Process prepared transactions count
        if let Some(row) = prepared_count_row {
            transaction_info.prepared_transactions_count = Self::safe_i64_to_u64(&row, "prepared_count")?;
        }

        // Calculate derived metrics
        transaction_info.deadlock_rate_per_transaction =
            Self::calculate_deadlock_rate(transaction_info.deadlocks_total, transaction_info.total_transactions);

        transaction_info.transaction_health_score = Self::calculate_health_score(&transaction_info);
        transaction_info.needs_immediate_attention = Self::needs_immediate_attention(&transaction_info);
        transaction_info.system_status = Self::get_system_status(&transaction_info);

        // Conditionally collect detailed metrics only when problems are detected
        transaction_info.detailed_transaction_info = Self::collect_detailed_transaction_info_if_needed(&transaction_info, context).await?;

        Ok(transaction_info)
    }

    async fn collect_detailed_transaction_info_if_needed(
        core_info: &PostgresTransactionInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedTransactionInfo>> {
        let needs_detailed_analysis = core_info.transaction_health_score < 80.0
            || core_info.is_wraparound_warning
            || core_info.long_running_transactions_count > 0
            || core_info.prepared_transactions_count > 0
            || core_info.commit_ratio < Self::COMMIT_RATIO_WARNING_THRESHOLD
            || core_info.deadlock_rate_per_transaction > Self::DEADLOCK_RATE_WARNING_THRESHOLD
            || core_info.oldest_transaction_age > Self::VERY_LONG_TRANSACTION_THRESHOLD;

        if !needs_detailed_analysis {
            return Ok(None);
        }

        let mut detailed_info = PostgresDetailedTransactionInfo {
            long_running_transactions: Vec::new(),
            transactions_by_database: Vec::new(),
            prepared_transactions: Vec::new(),
            transaction_age_stats: PostgresTransactionAgeStats::default(),
            multixact_info: PostgresMultixactInfo::default(),
            warnings: Vec::new(),
            recommendations: Vec::new(),
        };

        // Collect long-running transactions
        if core_info.long_running_transactions_count > 0
            && let Ok(long_tx_rows) = Self::query_long_running_transactions(context.clone()).await
        {
            detailed_info.long_running_transactions = Self::parse_long_running_transactions(long_tx_rows)?;
        }

        // Collect database-specific transaction stats
        if let Ok(db_stats_rows) = Self::query_transactions_by_database(context.clone()).await {
            detailed_info.transactions_by_database = Self::parse_transactions_by_database(db_stats_rows)?;
        }

        // Collect prepared transactions
        if core_info.prepared_transactions_count > 0
            && let Ok(prepared_rows) = Self::query_prepared_transactions(context.clone()).await
        {
            detailed_info.prepared_transactions = Self::parse_prepared_transactions(prepared_rows)?;
        }

        // Collect transaction age statistics
        if let Ok(age_stats_rows) = Self::query_transaction_age_stats(context.clone()).await {
            detailed_info.transaction_age_stats = Self::parse_transaction_age_stats(age_stats_rows)?;
        }

        // Collect multixact information
        if let Ok(multixact_rows) = Self::query_multixact_info(context.clone()).await {
            detailed_info.multixact_info = Self::parse_multixact_info(multixact_rows)?;
        }

        // Generate warnings and recommendations
        detailed_info.warnings = Self::generate_warnings(core_info, &detailed_info);
        detailed_info.recommendations = Self::generate_recommendations(core_info, &detailed_info);

        Ok(Some(detailed_info))
    }

    async fn query_long_running_transactions(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                pid, datname, usename, application_name, client_addr::text,
                backend_start, xact_start, query_start, state_change,
                state, LEFT(query, 500) as query, backend_xid, backend_xmin,
                EXTRACT(EPOCH FROM (now() - xact_start)) as xact_duration,
                EXTRACT(EPOCH FROM (now() - query_start)) as query_duration,
                EXTRACT(EPOCH FROM (now() - state_change)) as state_duration
            FROM pg_stat_activity
            WHERE xact_start IS NOT NULL
                AND EXTRACT(EPOCH FROM (now() - xact_start)) > {}
            ORDER BY xact_start ASC
            LIMIT {}",
                Self::LONG_TRANSACTION_THRESHOLD,
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "long_running_transactions").await
    }

    async fn query_transactions_by_database(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                datname, xact_commit, xact_rollback, deadlocks,
                (xact_commit + xact_rollback) as total_transactions,
                CASE WHEN (xact_commit + xact_rollback) > 0 THEN
                    xact_commit::float / (xact_commit + xact_rollback)::float
                ELSE 1.0 END as commit_ratio,
                stats_reset
            FROM pg_stat_database
            WHERE datname IS NOT NULL
                AND (xact_commit + xact_rollback) > 0
            ORDER BY (xact_commit + xact_rollback) DESC"
                .to_string(),
            Vec::new(),
        );
        query_input.run_query_parsed(context).await
    }

    async fn query_prepared_transactions(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                transaction, gid, prepared, owner, database,
                EXTRACT(EPOCH FROM (now() - prepared)) as age_seconds
            FROM pg_prepared_xacts
            ORDER BY prepared ASC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "prepared_transactions").await
    }

    async fn query_transaction_age_stats(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                AVG(EXTRACT(EPOCH FROM (now() - xact_start))) as avg_duration,
                MAX(EXTRACT(EPOCH FROM (now() - xact_start))) as max_duration,
                COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (now() - xact_start)) > 60) as over_1min,
                COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (now() - xact_start)) > 300) as over_5min,
                COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (now() - xact_start)) > 1800) as over_30min
            FROM pg_stat_activity
            WHERE xact_start IS NOT NULL"
                .to_string(),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "transaction_age_stats").await
    }

    async fn query_multixact_info(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                (SELECT setting::bigint FROM pg_settings WHERE name = 'autovacuum_multixact_freeze_max_age') as multixact_freeze_max_age,
                MAX(mxid_age(datminmxid)) as max_multixact_age,
                MIN(datminmxid) as oldest_multixact_id
            FROM pg_database
            WHERE datname IS NOT NULL"
                .to_string(),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "multixact_info").await
    }

    // Helper functions for safe type conversion (same as activity code)
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

    fn safe_get_i64(row: &PgSimpleRow, column: &str) -> ResultEP<i64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<DateTimeWrapper> {
        let text = row
            .get(column)
            .ok_or_else(|| EpError::metadata(format!("Failed to get datetime column {column}: column not found or NULL")))?;
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
    }

    fn safe_get_optional_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        match row.get(column) {
            Some(text) => {
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
            }
            None => Ok(None),
        }
    }

    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_optional_i32(row: &PgSimpleRow, column: &str) -> ResultEP<Option<i32>> {
        Ok(row.get(column).and_then(|s| s.parse::<i32>().ok()))
    }

    fn calculate_deadlock_rate(deadlocks: u64, total_transactions: u64) -> f64 {
        if total_transactions == 0 {
            0.0
        } else {
            (deadlocks as f64 / total_transactions as f64) * 1000.0
        }
    }

    fn calculate_health_score(info: &PostgresTransactionInfo) -> f64 {
        let mut score = 100.0;

        // Deduct for low commit ratio
        if info.commit_ratio < Self::COMMIT_RATIO_WARNING_THRESHOLD {
            score -= (Self::COMMIT_RATIO_WARNING_THRESHOLD - info.commit_ratio) * 100.0;
        }

        // Deduct for high deadlock rate
        if info.deadlock_rate_per_transaction > Self::DEADLOCK_RATE_WARNING_THRESHOLD {
            score -= (info.deadlock_rate_per_transaction - Self::DEADLOCK_RATE_WARNING_THRESHOLD).min(20.0);
        }

        // Deduct for long transactions
        if info.long_running_transactions_count > 0 {
            score -= (info.long_running_transactions_count as f64).min(30.0);
        }

        // Deduct for very old transactions
        if info.oldest_transaction_age > Self::VERY_LONG_TRANSACTION_THRESHOLD {
            score -= 25.0;
        }

        // Deduct for wraparound warning
        if info.is_wraparound_warning {
            score -= 40.0;
        }

        // Deduct for prepared transactions (potential issue)
        if info.prepared_transactions_count > 0 {
            score -= (info.prepared_transactions_count as f64).min(15.0);
        }

        score.clamp(0.0, 100.0)
    }

    fn needs_immediate_attention(info: &PostgresTransactionInfo) -> bool {
        info.is_wraparound_warning
            || info.transaction_health_score < 60.0
            || info.oldest_transaction_age > 7200.0 // 2 hours
            || info.commit_ratio < 0.8
            || info.deadlock_rate_per_transaction > 5.0
    }

    fn get_system_status(info: &PostgresTransactionInfo) -> String {
        if info.needs_immediate_attention {
            "Critical - Immediate Attention Required".to_string()
        } else {
            match info.transaction_health_score {
                score if score >= 90.0 => "Excellent".to_string(),
                score if score >= 75.0 => "Good".to_string(),
                score if score >= 60.0 => "Fair - Some Issues".to_string(),
                _ => "Poor - Multiple Issues".to_string(),
            }
        }
    }

    fn parse_long_running_transactions(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresLongTransaction>> {
        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            transactions.push(PostgresLongTransaction {
                pid: Self::safe_get_i32(&row, "pid")?,
                database: Self::safe_get_string(&row, "datname")?,
                username: Self::safe_get_string(&row, "usename")?,
                application_name: Self::safe_get_optional_string(&row, "application_name")?,
                client_addr: Self::safe_get_optional_string(&row, "client_addr")?,
                backend_start: Self::safe_get_datetime(&row, "backend_start")?,
                xact_start: Self::safe_get_datetime(&row, "xact_start")?,
                query_start: Self::safe_get_optional_datetime(&row, "query_start")?,
                state_change: Self::safe_get_datetime(&row, "state_change")?,
                state: Self::safe_get_string(&row, "state")?,
                query: Self::safe_get_string(&row, "query")?,
                backend_xid: Self::safe_get_optional_i32(&row, "backend_xid")?,
                backend_xmin: Self::safe_get_optional_i32(&row, "backend_xmin")?,
                xact_duration: Self::safe_get_f64(&row, "xact_duration")?,
                query_duration: row.get("query_duration").and_then(|s| s.parse::<f64>().ok()),
                state_duration: Self::safe_get_f64(&row, "state_duration")?,
            });
        }

        Ok(transactions)
    }

    fn parse_transactions_by_database(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTransactionsByDatabase>> {
        let mut databases = Vec::with_capacity(rows.len());

        for row in rows {
            databases.push(PostgresTransactionsByDatabase {
                database_name: Self::safe_get_string(&row, "datname")?,
                transactions_committed: Self::safe_i64_to_u64(&row, "xact_commit")?,
                transactions_rolled_back: Self::safe_i64_to_u64(&row, "xact_rollback")?,
                deadlocks: Self::safe_i64_to_u64(&row, "deadlocks")?,
                total_transactions: Self::safe_i64_to_u64(&row, "total_transactions")?,
                commit_ratio: Self::safe_get_f64(&row, "commit_ratio")?,
                stats_reset: Self::safe_get_optional_datetime(&row, "stats_reset")?,
            });
        }

        Ok(databases)
    }

    fn parse_prepared_transactions(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresPreparedTransaction>> {
        let mut prepared = Vec::with_capacity(rows.len());

        for row in rows {
            prepared.push(PostgresPreparedTransaction {
                transaction_id: Self::safe_get_i32(&row, "transaction")?,
                gid: Self::safe_get_string(&row, "gid")?,
                prepared_time: Self::safe_get_datetime(&row, "prepared")?,
                owner: Self::safe_get_string(&row, "owner")?,
                database: Self::safe_get_string(&row, "database")?,
                age_seconds: Self::safe_get_f64(&row, "age_seconds")?,
            });
        }

        Ok(prepared)
    }

    fn parse_transaction_age_stats(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresTransactionAgeStats> {
        if let Some(row) = rows.first() {
            Ok(PostgresTransactionAgeStats {
                avg_transaction_duration: row.get("avg_duration").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                max_transaction_duration: row.get("max_duration").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                transactions_over_1min: Self::safe_i64_to_u64(row, "over_1min")?,
                transactions_over_5min: Self::safe_i64_to_u64(row, "over_5min")?,
                transactions_over_30min: Self::safe_i64_to_u64(row, "over_30min")?,
                oldest_transaction_age: row.get("max_duration").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
            })
        } else {
            Ok(PostgresTransactionAgeStats::default())
        }
    }

    fn parse_multixact_info(rows: Vec<PgSimpleRow>) -> ResultEP<PostgresMultixactInfo> {
        if let Some(row) = rows.first() {
            let freeze_max_age = Self::safe_get_i64(row, "multixact_freeze_max_age")?;
            let max_multixact_age = Self::safe_get_i64(row, "max_multixact_age")?;
            let multixact_until_wraparound = freeze_max_age - max_multixact_age;

            Ok(PostgresMultixactInfo {
                multixact_freeze_max_age: freeze_max_age,
                oldest_multixact_age: max_multixact_age,
                oldest_multixact_id: Self::safe_get_i32(row, "oldest_multixact_id")?,
                is_multixact_wraparound_warning: multixact_until_wraparound < Self::XID_WRAPAROUND_WARNING_THRESHOLD,
                multixact_until_wraparound,
            })
        } else {
            Ok(PostgresMultixactInfo::default())
        }
    }

    fn generate_warnings(core_info: &PostgresTransactionInfo, detailed_info: &PostgresDetailedTransactionInfo) -> Vec<String> {
        let mut warnings = Vec::new();

        if core_info.is_wraparound_warning {
            warnings.push("Transaction ID wraparound warning - run VACUUM FREEZE immediately".to_string());
        }

        if detailed_info.multixact_info.is_multixact_wraparound_warning {
            warnings.push("Multixact wraparound warning - check multixact usage".to_string());
        }

        if core_info.commit_ratio < 0.9 {
            warnings.push(format!("Low commit ratio ({:.1}%) - high rollback rate detected", core_info.commit_ratio * 100.0));
        }

        if core_info.deadlock_rate_per_transaction > Self::DEADLOCK_RATE_WARNING_THRESHOLD {
            warnings.push(format!("High deadlock rate ({:.2} per 1000 transactions)", core_info.deadlock_rate_per_transaction));
        }

        if detailed_info.transaction_age_stats.transactions_over_30min > 0 {
            warnings.push(format!(
                "{} very long-running transactions detected (>30 min)",
                detailed_info.transaction_age_stats.transactions_over_30min
            ));
        }

        if !detailed_info.prepared_transactions.is_empty() {
            let stale_count = detailed_info.prepared_transactions.iter().filter(|p| p.age_seconds > 3600.0).count();
            if stale_count > 0 {
                warnings.push(format!("{} stale prepared transactions found", stale_count));
            }
        }

        warnings
    }

    fn generate_recommendations(core_info: &PostgresTransactionInfo, detailed_info: &PostgresDetailedTransactionInfo) -> Vec<String> {
        let mut recommendations = Vec::new();

        if core_info.is_wraparound_warning {
            recommendations.push("Run VACUUM FREEZE on all databases immediately".to_string());
            recommendations.push("Consider adjusting autovacuum settings to prevent future wraparound".to_string());
        }

        if core_info.commit_ratio < Self::COMMIT_RATIO_WARNING_THRESHOLD {
            recommendations.push("Review application logic to reduce transaction rollbacks".to_string());
            recommendations.push("Implement proper error handling to improve commit ratio".to_string());
        }

        if core_info.deadlock_rate_per_transaction > Self::DEADLOCK_RATE_WARNING_THRESHOLD {
            recommendations.push("Review application transaction patterns to reduce deadlocks".to_string());
            recommendations.push("Consider using consistent lock ordering in application code".to_string());
        }

        if !detailed_info.long_running_transactions.is_empty() {
            recommendations.push("Review and optimize long-running transactions".to_string());
            recommendations.push("Consider breaking large transactions into smaller chunks".to_string());
        }

        if !detailed_info.prepared_transactions.is_empty() {
            recommendations.push("Clean up prepared transactions that are no longer needed".to_string());
            recommendations.push("Review two-phase commit usage patterns".to_string());
        }

        if detailed_info.multixact_info.is_multixact_wraparound_warning {
            recommendations.push("Review tables with many concurrent updates".to_string());
            recommendations.push("Consider adjusting autovacuum_multixact_freeze_max_age".to_string());
        }

        recommendations
    }
}

/// Information about a long-running transaction
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresLongTransaction {
    /// Process ID of the backend
    pub pid: i32,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Application name
    pub application_name: Option<String>,
    /// Client address
    pub client_addr: Option<String>,
    /// When the backend process started
    pub backend_start: DateTimeWrapper,
    /// When the transaction started
    pub xact_start: DateTimeWrapper,
    /// When the current query started
    pub query_start: Option<DateTimeWrapper>,
    /// When the state last changed
    pub state_change: DateTimeWrapper,
    /// Current state of the backend
    pub state: String,
    /// Currently executing query (truncated)
    pub query: String,
    /// Transaction ID being used
    pub backend_xid: Option<i32>,
    /// Minimum transaction ID
    pub backend_xmin: Option<i32>,
    /// Duration the transaction has been running (seconds)
    pub xact_duration: f64,
    /// Duration the current query has been running (seconds)
    pub query_duration: Option<f64>,
    /// Time since last state change (seconds)
    pub state_duration: f64,
}

/// Transaction statistics for a specific database
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTransactionsByDatabase {
    /// Database name
    pub database_name: String,
    /// Number of committed transactions
    pub transactions_committed: u64,
    /// Number of rolled back transactions
    pub transactions_rolled_back: u64,
    /// Number of deadlocks
    pub deadlocks: u64,
    /// Total transactions
    pub total_transactions: u64,
    /// Commit ratio (0.0 to 1.0)
    pub commit_ratio: f64,
    /// When statistics were last reset
    pub stats_reset: Option<DateTimeWrapper>,
}

/// Information about a prepared transaction
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresPreparedTransaction {
    /// Transaction ID
    pub transaction_id: i32,
    /// Global transaction identifier
    pub gid: String,
    /// When the transaction was prepared
    pub prepared_time: DateTimeWrapper,
    /// Owner of the transaction
    pub owner: String,
    /// Database name
    pub database: String,
    /// Age of the prepared transaction (seconds)
    pub age_seconds: f64,
}

/// Transaction age and timing statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTransactionAgeStats {
    /// Average transaction duration (seconds)
    pub avg_transaction_duration: f64,
    /// Maximum transaction duration (seconds)
    pub max_transaction_duration: f64,
    /// Number of transactions running longer than 1 minute
    pub transactions_over_1min: u64,
    /// Number of transactions running longer than 5 minutes
    pub transactions_over_5min: u64,
    /// Number of transactions running longer than 30 minutes
    pub transactions_over_30min: u64,
    /// Oldest transaction age (seconds)
    pub oldest_transaction_age: f64,
}

/// Multixact (multiple transaction) information
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresMultixactInfo {
    /// Maximum age before multixact freeze
    pub multixact_freeze_max_age: i64,
    /// Age of the oldest multixact
    pub oldest_multixact_age: i64,
    /// Oldest multixact ID
    pub oldest_multixact_id: i32,
    /// Whether approaching multixact wraparound
    pub is_multixact_wraparound_warning: bool,
    /// Multixacts until wraparound warning
    pub multixact_until_wraparound: i64,
}

impl PostgresTransactionInfo {
    /// Checks if commit ratio is healthy
    pub fn has_healthy_commit_ratio(&self) -> bool {
        self.commit_ratio >= Self::COMMIT_RATIO_WARNING_THRESHOLD
    }

    /// Checks if deadlock rate is concerning
    pub fn has_excessive_deadlocks(&self) -> bool {
        self.deadlock_rate_per_transaction > Self::DEADLOCK_RATE_WARNING_THRESHOLD
    }

    /// Checks if approaching transaction ID wraparound
    pub fn is_approaching_xid_wraparound(&self) -> bool {
        self.is_wraparound_warning
    }

    /// Checks if detailed transaction info was collected
    pub fn has_detailed_info(&self) -> bool {
        self.detailed_transaction_info.is_some()
    }

    /// Gets all warnings
    pub fn get_all_warnings(&self) -> Vec<&String> {
        self.detailed_transaction_info.as_ref().map(|info| info.warnings.iter().collect()).unwrap_or_default()
    }

    /// Gets all recommendations
    pub fn get_all_recommendations(&self) -> Vec<&String> {
        self.detailed_transaction_info.as_ref().map(|info| info.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets long-running transactions
    pub fn get_long_running_transactions(&self) -> Vec<&PostgresLongTransaction> {
        self.detailed_transaction_info.as_ref().map(|info| info.long_running_transactions.iter().collect()).unwrap_or_default()
    }

    /// Gets prepared transactions
    pub fn get_prepared_transactions(&self) -> Vec<&PostgresPreparedTransaction> {
        self.detailed_transaction_info.as_ref().map(|info| info.prepared_transactions.iter().collect()).unwrap_or_default()
    }

    /// Gets transactions by database
    pub fn get_transactions_by_database(&self) -> Vec<&PostgresTransactionsByDatabase> {
        self.detailed_transaction_info.as_ref().map(|info| info.transactions_by_database.iter().collect()).unwrap_or_default()
    }

    /// Calculates transaction throughput per second (if time period known)
    pub fn calculate_transaction_throughput(&self, time_period_seconds: f64) -> f64 {
        if time_period_seconds <= 0.0 {
            0.0
        } else {
            self.total_transactions as f64 / time_period_seconds
        }
    }

    /// Gets very long transactions (over 30 minutes)
    pub fn get_very_long_transactions(&self) -> Vec<&PostgresLongTransaction> {
        self.get_long_running_transactions()
            .into_iter()
            .filter(|txn| txn.xact_duration > Self::VERY_LONG_TRANSACTION_THRESHOLD)
            .collect()
    }

    /// Gets stale prepared transactions (over 1 hour)
    pub fn get_stale_prepared_transactions(&self) -> Vec<&PostgresPreparedTransaction> {
        self.get_prepared_transactions().into_iter().filter(|prep| prep.age_seconds > 3600.0).collect()
    }

    /// Gets database with highest deadlock count
    pub fn get_most_deadlock_prone_database(&self) -> Option<&PostgresTransactionsByDatabase> {
        self.get_transactions_by_database().into_iter().max_by_key(|db| db.deadlocks)
    }

    /// Gets database with lowest commit ratio
    pub fn get_lowest_commit_ratio_database(&self) -> Option<&PostgresTransactionsByDatabase> {
        self.get_transactions_by_database()
            .into_iter()
            .filter(|db| db.total_transactions > 0)
            .min_by(|a, b| a.commit_ratio.partial_cmp(&b.commit_ratio).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Gets overall transaction system assessment
    pub fn get_system_assessment(&self) -> String {
        let mut issues = Vec::new();

        if self.is_wraparound_warning {
            issues.push("Transaction wraparound risk");
        }

        if !self.has_healthy_commit_ratio() {
            issues.push("Poor commit ratio");
        }

        if self.has_excessive_deadlocks() {
            issues.push("High deadlock rate");
        }

        if self.long_running_transactions_count > 0 {
            issues.push("Long-running transactions");
        }

        if self.prepared_transactions_count > 0 {
            issues.push("Prepared transactions present");
        }

        if issues.is_empty() {
            format!("System Status: {}", self.system_status)
        } else {
            format!("System Status: {} (Issues: {})", self.system_status, issues.join(", "))
        }
    }

    /// Gets transaction health summary
    pub fn get_health_summary(&self) -> String {
        format!(
            "Transaction Health: {:.1}/100. Commit Ratio: {:.1}%. Deadlock Rate: {:.2}/1000 txns. Long Transactions: {}",
            self.transaction_health_score,
            self.commit_ratio * 100.0,
            self.deadlock_rate_per_transaction,
            self.long_running_transactions_count
        )
    }

    /// Checks if system needs urgent intervention
    pub fn needs_urgent_intervention(&self) -> bool {
        self.is_wraparound_warning || self.transaction_health_score < 50.0 || self.oldest_transaction_age > 7200.0 // 2 hours
    }

    /// Gets priority action items
    pub fn get_priority_actions(&self) -> Vec<String> {
        let mut actions = Vec::new();

        if self.is_wraparound_warning {
            actions.push("URGENT: Run VACUUM FREEZE immediately to prevent wraparound".to_string());
        }

        if self.oldest_transaction_age > 7200.0 {
            actions.push("URGENT: Investigate and terminate long-running transactions".to_string());
        }

        if self.commit_ratio < 0.8 {
            actions.push("HIGH: Review application logic causing high rollback rate".to_string());
        }

        if self.deadlock_rate_per_transaction > 5.0 {
            actions.push("HIGH: Address deadlock issues in application code".to_string());
        }

        if !self.get_stale_prepared_transactions().is_empty() {
            actions.push("MEDIUM: Clean up stale prepared transactions".to_string());
        }

        actions
    }

    /// Gets transaction age distribution summary
    pub fn get_age_distribution_summary(&self) -> String {
        if let Some(detailed) = &self.detailed_transaction_info {
            format!(
                "Transaction Age Distribution: >1min: {}, >5min: {}, >30min: {}",
                detailed.transaction_age_stats.transactions_over_1min,
                detailed.transaction_age_stats.transactions_over_5min,
                detailed.transaction_age_stats.transactions_over_30min
            )
        } else {
            "Transaction age distribution not available".to_string()
        }
    }

    /// Calculates time until wraparound in days (if applicable)
    pub fn get_days_until_wraparound(&self) -> Option<f64> {
        self.xid_until_wraparound.map(|xids| {
            // Very rough estimate: assume 1000 transactions per second
            let estimated_txn_rate = 1000.0;
            let seconds_until_wraparound = xids as f64 / estimated_txn_rate;
            seconds_until_wraparound / 86400.0 // Convert to days
        })
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_metadata_transactions() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let transaction_info = PostgresTransactionInfo::default();

        let result = transaction_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.transaction_health_score >= 0.0);
        assert!(info.transaction_health_score <= 100.0);
        assert!(info.commit_ratio >= 0.0);
        assert!(info.commit_ratio <= 1.0);
        assert!(!info.system_status.is_empty());
    }

    #[tokio::test]
    async fn test_transaction_health_score_calculation() {
        let mut transaction_info = PostgresTransactionInfo {
            // Test healthy system
            commit_ratio: 0.98,
            deadlock_rate_per_transaction: 0.1,
            long_running_transactions_count: 0,
            is_wraparound_warning: false,
            ..Default::default()
        };

        let healthy_score = PostgresTransactionInfo::calculate_health_score(&transaction_info);
        assert!(healthy_score > 90.0);

        // Test problematic system
        transaction_info.commit_ratio = 0.85;
        transaction_info.deadlock_rate_per_transaction = 3.0;
        transaction_info.long_running_transactions_count = 5;
        transaction_info.is_wraparound_warning = true;

        let poor_score = PostgresTransactionInfo::calculate_health_score(&transaction_info);
        assert!(poor_score < 50.0);
    }

    #[tokio::test]
    async fn test_deadlock_rate_calculation() {
        assert_eq!(PostgresTransactionInfo::calculate_deadlock_rate(10, 10000), 1.0);
        assert_eq!(PostgresTransactionInfo::calculate_deadlock_rate(0, 1000), 0.0);
        assert_eq!(PostgresTransactionInfo::calculate_deadlock_rate(5, 0), 0.0);
    }

    #[tokio::test]
    async fn test_transaction_health_checks() {
        let mut transaction_info = PostgresTransactionInfo {
            commit_ratio: 0.96,
            deadlock_rate_per_transaction: 0.5,
            ..Default::default()
        };

        assert!(transaction_info.has_healthy_commit_ratio());
        assert!(!transaction_info.has_excessive_deadlocks());

        transaction_info.commit_ratio = 0.90;
        transaction_info.deadlock_rate_per_transaction = 2.0;

        assert!(!transaction_info.has_healthy_commit_ratio());
        assert!(transaction_info.has_excessive_deadlocks());
    }

    #[tokio::test]
    async fn test_transaction_throughput_calculation() {
        let transaction_info = PostgresTransactionInfo { total_transactions: 1000, ..Default::default() };

        assert_eq!(transaction_info.calculate_transaction_throughput(100.0), 10.0);
        assert_eq!(transaction_info.calculate_transaction_throughput(0.0), 0.0);
    }

    #[tokio::test]
    async fn test_immediate_attention_detection() {
        let mut transaction_info = PostgresTransactionInfo {
            is_wraparound_warning: false,
            transaction_health_score: 75.0,
            oldest_transaction_age: 1800.0,
            commit_ratio: 0.95,
            ..Default::default()
        };

        assert!(!PostgresTransactionInfo::needs_immediate_attention(&transaction_info));

        transaction_info.is_wraparound_warning = true;
        assert!(PostgresTransactionInfo::needs_immediate_attention(&transaction_info));

        transaction_info.is_wraparound_warning = false;
        transaction_info.oldest_transaction_age = 8000.0; // Over 2 hours
        assert!(PostgresTransactionInfo::needs_immediate_attention(&transaction_info));
    }

    #[tokio::test]
    async fn test_system_status_determination() {
        let mut transaction_info = PostgresTransactionInfo {
            transaction_health_score: 95.0,
            needs_immediate_attention: false,
            ..Default::default()
        };

        assert_eq!(PostgresTransactionInfo::get_system_status(&transaction_info), "Excellent");

        transaction_info.transaction_health_score = 80.0;
        assert_eq!(PostgresTransactionInfo::get_system_status(&transaction_info), "Good");

        transaction_info.needs_immediate_attention = true;
        assert_eq!(
            PostgresTransactionInfo::get_system_status(&transaction_info),
            "Critical - Immediate Attention Required"
        );
    }
}
