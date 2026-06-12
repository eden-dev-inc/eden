use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{RowExt, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL database statistics and information
///
/// Simplified struct containing essential metrics about database size,
/// performance, and health indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDatabaseStats {
    /// Database name
    pub database_name: String,
    /// Database OID
    pub database_oid: u32,
    /// Database size in bytes
    pub database_size_bytes: u64,
    /// Human-readable database size
    pub database_size_pretty: String,
    /// Number of connections to this database
    pub connection_count: u64,
    /// Database encoding
    pub encoding: String,
    /// Database collation
    pub collation: String,
    /// Whether the database allows connections
    pub allows_connections: bool,
    /// Connection limit for this database
    pub connection_limit: i32,
    /// Database owner
    pub owner: String,
    /// Database tablespace
    pub tablespace: String,
    /// Cache hit ratio percentage (0.0 to 100.0)
    pub cache_hit_ratio: f64,
    /// Transaction commit ratio percentage (0.0 to 100.0)
    pub commit_ratio: f64,
    /// Number of deadlocks detected
    pub deadlocks: u64,
    /// Number of checksum failures
    pub checksum_failures: u64,
    /// Age of frozen transaction ID
    pub xid_age: i64,
    /// Whether approaching transaction wraparound
    pub approaching_wraparound: bool,
    /// Overall health score (0.0 to 100.0, higher is better)
    pub health_score: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<PostgresDatabaseDetailedMetrics>,
}

/// Detailed database metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDatabaseDetailedMetrics {
    /// Object counts (collected when health_score < 80)
    pub object_counts: Option<PostgresObjectCounts>,
    /// Performance statistics (collected when cache_hit_ratio < 90 or deadlocks > 0)
    pub performance_stats: Option<PostgresDatabasePerformanceStats>,
    /// Bloat analysis (collected when database_size > 1GB)
    pub bloat_analysis: Option<PostgresDatabaseBloatStats>,
    /// Maintenance recommendations based on current state
    pub maintenance_recommendations: Vec<String>,
}

impl MetadataCollection for PostgresDatabaseStats {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "database_info".to_string(),
                QueryInput::new(
                    "SELECT
                        d.datname,
                        d.oid,
                        d.datdba,
                        pg_encoding_to_char(d.encoding) AS encoding,
                        d.datcollate,
                        d.datctype,
                        d.datallowconn,
                        d.datconnlimit,
                        pg_get_userbyid(d.datdba) AS owner,
                        t.spcname AS tablespace,
                        pg_database_size(d.datname) AS size_bytes,
                        pg_size_pretty(pg_database_size(d.datname)) AS size_pretty,
                        (
                            SELECT count(*)
                            FROM pg_stat_activity
                            WHERE datname = d.datname
                              AND pid != pg_backend_pid()
                        ) AS connections
                    FROM pg_database d
                    JOIN pg_tablespace t ON d.dattablespace = t.oid
                    WHERE d.datname = current_database()"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "performance_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    datname, xact_commit, xact_rollback, blks_read, blks_hit,
                    deadlocks, COALESCE(checksum_failures, 0) AS checksum_failures,
                    CASE WHEN (blks_read + blks_hit) > 0 THEN
                        (blks_hit::float / (blks_read + blks_hit)::float) * 100
                    ELSE 100 END as cache_hit_ratio
                FROM pg_stat_database
                WHERE datname = current_database()"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "xid_age".to_string(),
                QueryInput::new(
                    "SELECT
                    datname, age(datfrozenxid)::bigint as xid_age, datfrozenxid
                FROM pg_database
                WHERE datname = current_database()"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL database statistics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "database"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresDatabaseStats {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const HEALTH_THRESHOLD: f64 = 80.0;
    const CACHE_HIT_THRESHOLD: f64 = 90.0;
    const SIZE_THRESHOLD_GB: f64 = 1.0;
    const WRAPAROUND_THRESHOLD: i64 = 1_500_000_000; // 1.5B transactions

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut database_stats = PostgresDatabaseStats::default();
        let requests = self.request();

        // Execute core database info queries concurrently
        let (database_info_row, performance_row, xid_age_row) = tokio::try_join!(
            run_single_row(&requests, "database_info", context.clone(), Self::QUERY_TIMEOUT),
            run_single_row(&requests, "performance_summary", context.clone(), Self::QUERY_TIMEOUT),
            run_single_row(&requests, "xid_age", context.clone(), Self::QUERY_TIMEOUT)
        )?;

        // Parse database info
        if let Some(row) = database_info_row {
            database_stats.database_name = row.get_string("datname")?;
            database_stats.database_oid = row.get_u32("oid")?;
            database_stats.encoding = row.get_string("encoding")?;
            database_stats.collation = row.get_string("datcollate")?;
            database_stats.allows_connections = row.get_bool("datallowconn")?;
            database_stats.connection_limit = row.get_i32("datconnlimit")?;
            database_stats.owner = row.get_string("owner")?;
            database_stats.tablespace = row.get_string("tablespace")?;
            database_stats.database_size_bytes = row.get_u64("size_bytes")?;
            database_stats.database_size_pretty = row.get_string("size_pretty")?;
            database_stats.connection_count = row.get_u64("connections")?;
        }

        // Parse performance stats
        if let Some(row) = performance_row {
            database_stats.cache_hit_ratio = row.get_f64("cache_hit_ratio")?;
            database_stats.deadlocks = row.get_u64("deadlocks")?;
            database_stats.checksum_failures = row.get_opt_u64("checksum_failures")?.unwrap_or_default();

            // Calculate commit ratio
            let commits = row.get_u64("xact_commit")?;
            let rollbacks = row.get_u64("xact_rollback")?;
            database_stats.commit_ratio = if commits + rollbacks > 0 {
                (commits as f64 / (commits + rollbacks) as f64) * 100.0
            } else {
                100.0
            };
        }

        // Parse XID age
        if let Some(row) = xid_age_row {
            database_stats.xid_age = row.get_i64("xid_age")?;
            database_stats.approaching_wraparound = database_stats.xid_age > Self::WRAPAROUND_THRESHOLD;
        }

        // Calculate health score
        database_stats.health_score = database_stats.calculate_health_score();

        // Conditionally collect detailed metrics only when problems are detected
        database_stats.detailed_metrics = Self::collect_detailed_metrics_if_needed(&database_stats, context).await?;

        Ok(database_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresDatabaseStats,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDatabaseDetailedMetrics>> {
        let needs_object_counts = core_info.health_score < Self::HEALTH_THRESHOLD;
        let needs_performance_details =
            core_info.cache_hit_ratio < Self::CACHE_HIT_THRESHOLD || core_info.deadlocks > 0 || core_info.checksum_failures > 0;
        let needs_bloat_analysis = core_info.get_size_gb() > Self::SIZE_THRESHOLD_GB;

        if !needs_object_counts && !needs_performance_details && !needs_bloat_analysis {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresDatabaseDetailedMetrics {
            object_counts: None,
            performance_stats: None,
            bloat_analysis: None,
            maintenance_recommendations: core_info.generate_maintenance_recommendations(),
        };

        // Collect object counts if health is poor
        if needs_object_counts {
            let object_counts_input = QueryInput::new(
                "SELECT
                    (SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as table_count,
                    (SELECT count(*) FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'pg_catalog')) as view_count,
                    (SELECT count(*) FROM pg_indexes WHERE schemaname NOT IN ('information_schema', 'pg_catalog')) as index_count,
                    (SELECT count(*) FROM information_schema.routines WHERE routine_schema NOT IN ('information_schema', 'pg_catalog')) as function_count,
                    (SELECT count(*) FROM information_schema.sequences WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog')) as sequence_count".to_string(),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&object_counts_input, context.clone(), Self::QUERY_TIMEOUT, "object_counts").await?;

            if let Some(row) = rows.first() {
                detailed_metrics.object_counts = Some(PostgresObjectCounts {
                    table_count: row.get_u64("table_count")?,
                    view_count: row.get_u64("view_count")?,
                    index_count: row.get_u64("index_count")?,
                    function_count: row.get_u64("function_count")?,
                    sequence_count: row.get_u64("sequence_count")?,
                });
            }
        }

        // Collect detailed performance stats if needed
        if needs_performance_details {
            let performance_input = QueryInput::new(
                "SELECT
                    datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
                    tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
                    temp_files, temp_bytes, deadlocks, COALESCE(checksum_failures, 0) AS checksum_failures,
                    blk_read_time, blk_write_time, stats_reset
                FROM pg_stat_database
                WHERE datname = current_database()"
                    .to_string(),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&performance_input, context.clone(), Self::QUERY_TIMEOUT, "performance_stats").await?;

            if let Some(row) = rows.first() {
                detailed_metrics.performance_stats = Some(PostgresDatabasePerformanceStats {
                    active_backends: row.get_u64("numbackends")?,
                    transactions_committed: row.get_u64("xact_commit")?,
                    transactions_rolled_back: row.get_u64("xact_rollback")?,
                    blocks_read: row.get_u64("blks_read")?,
                    blocks_hit: row.get_u64("blks_hit")?,
                    tuples_returned: row.get_u64("tup_returned")?,
                    tuples_fetched: row.get_u64("tup_fetched")?,
                    tuples_inserted: row.get_u64("tup_inserted")?,
                    tuples_updated: row.get_u64("tup_updated")?,
                    tuples_deleted: row.get_u64("tup_deleted")?,
                    temp_files: row.get_u64("temp_files")?,
                    temp_bytes: row.get_u64("temp_bytes")?,
                    deadlocks: row.get_u64("deadlocks")?,
                    checksum_failures: row.get_opt_u64("checksum_failures")?.unwrap_or_default(),
                    block_read_time: row.get_f64("blk_read_time")?,
                    block_write_time: row.get_f64("blk_write_time")?,
                    stats_reset: row.get_opt_datetime("stats_reset")?,
                });
            }
        }

        // Collect bloat analysis for large databases
        if needs_bloat_analysis {
            let bloat_analysis_input = QueryInput::new(
                format!(
                    "SELECT
                        schemaname, relname,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size_pretty,
                        pg_total_relation_size(schemaname||'.'||relname) as total_size_bytes,
                        n_live_tup, n_dead_tup,
                        CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                            (n_dead_tup::float / (n_live_tup + n_dead_tup)::float) * 100
                        ELSE 0 END as estimated_bloat_ratio
                    FROM pg_stat_user_tables
                    WHERE n_live_tup + n_dead_tup > 1000
                        AND n_dead_tup > (n_live_tup * 0.1)
                    ORDER BY estimated_bloat_ratio DESC, total_size_bytes DESC
                    LIMIT {}",
                    50
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&bloat_analysis_input, context.clone(), Self::QUERY_TIMEOUT, "bloat_analysis").await?;

            if !rows.is_empty() {
                let mut total_bloat_bytes: u64 = 0;
                let mut max_ratio = 0.0;
                let mut concerning = false;

                for row in rows {
                    let ratio = row.get_f64("estimated_bloat_ratio")?.max(0.0);
                    let size_bytes = row.get_u64("total_size_bytes")?;
                    let bloat_bytes = (size_bytes as f64 * ratio / 100.0) as u64;
                    total_bloat_bytes = total_bloat_bytes.saturating_add(bloat_bytes);
                    if ratio > max_ratio {
                        max_ratio = ratio;
                    }
                    if ratio >= 25.0 {
                        concerning = true;
                    }
                }

                detailed_metrics.bloat_analysis = Some(PostgresDatabaseBloatStats {
                    estimated_bloat_bytes: total_bloat_bytes,
                    bloat_percentage: max_ratio,
                    is_bloat_concerning: concerning,
                });
            }
        }

        Ok(Some(detailed_metrics))
    }
}

impl PostgresDatabaseStats {
    /// Calculates the database size in GB
    pub fn get_size_gb(&self) -> f64 {
        self.database_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Calculates overall database health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for poor cache hit ratio
        if self.cache_hit_ratio < 95.0 {
            score -= (95.0 - self.cache_hit_ratio) * 2.0;
        }

        // Deduct for low commit ratio
        if self.commit_ratio < 95.0 {
            score -= (95.0 - self.commit_ratio) * 1.5;
        }

        // Deduct for deadlocks
        if self.deadlocks > 0 {
            score -= (self.deadlocks as f64).min(20.0);
        }

        // Deduct for checksum failures
        if self.checksum_failures > 0 {
            score -= 30.0;
        }

        // Deduct for approaching wraparound
        if self.approaching_wraparound {
            score -= 50.0;
        }

        score.max(0.0)
    }

    /// Generates maintenance recommendations based on current metrics
    fn generate_maintenance_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.approaching_wraparound {
            recommendations.push("URGENT: Run VACUUM FREEZE to prevent transaction wraparound".to_string());
        }

        if self.cache_hit_ratio < 90.0 {
            recommendations.push("Low cache hit ratio - consider increasing shared_buffers".to_string());
        }

        if self.deadlocks > 10 {
            recommendations.push("High deadlock count - review application transaction logic".to_string());
        }

        if self.checksum_failures > 0 {
            recommendations.push("CRITICAL: Checksum failures detected - check storage integrity".to_string());
        }

        if self.commit_ratio < 90.0 {
            recommendations.push("High rollback ratio - review application error handling".to_string());
        }

        recommendations
    }

    /// Checks if the database is large
    pub fn is_large_database(&self) -> bool {
        self.get_size_gb() > Self::SIZE_THRESHOLD_GB
    }

    /// Checks if approaching transaction wraparound
    pub fn is_approaching_wraparound(&self) -> bool {
        self.approaching_wraparound
    }

    /// Gets database health summary
    pub fn get_database_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Database is healthy".to_string(),
            75..=89 => "Good - Database is performing well".to_string(),
            60..=74 => "Fair - Database has some issues that should be monitored".to_string(),
            40..=59 => "Poor - Database has significant problems".to_string(),
            _ => "Critical - Database requires immediate attention".to_string(),
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets risk assessment
    pub fn get_risk_assessment(&self) -> String {
        if self.checksum_failures > 0 {
            "CRITICAL RISK - Data integrity issues detected".to_string()
        } else if self.approaching_wraparound {
            "HIGH RISK - Transaction wraparound imminent".to_string()
        } else if self.health_score < 50.0 {
            "HIGH RISK - Multiple performance issues".to_string()
        } else if self.health_score < 70.0 {
            "MEDIUM RISK - Some performance concerns".to_string()
        } else if self.health_score < 85.0 {
            "LOW RISK - Minor issues detected".to_string()
        } else {
            "MINIMAL RISK - Database appears healthy".to_string()
        }
    }
}

/// Count of various database objects
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresObjectCounts {
    /// Number of user tables
    pub table_count: u64,
    /// Number of views
    pub view_count: u64,
    /// Number of indexes
    pub index_count: u64,
    /// Number of functions/procedures
    pub function_count: u64,
    /// Number of sequences
    pub sequence_count: u64,
}

/// Performance statistics for a specific database
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDatabasePerformanceStats {
    /// Number of active backends
    pub active_backends: u64,
    /// Transactions committed
    pub transactions_committed: u64,
    /// Transactions rolled back
    pub transactions_rolled_back: u64,
    /// Disk blocks read
    pub blocks_read: u64,
    /// Buffer hits
    pub blocks_hit: u64,
    /// Tuples returned by queries
    pub tuples_returned: u64,
    /// Tuples fetched by queries
    pub tuples_fetched: u64,
    /// Tuples inserted
    pub tuples_inserted: u64,
    /// Tuples updated
    pub tuples_updated: u64,
    /// Tuples deleted
    pub tuples_deleted: u64,
    /// Temporary files created
    pub temp_files: u64,
    /// Temporary file bytes
    pub temp_bytes: u64,
    /// Number of deadlocks
    pub deadlocks: u64,
    /// Checksum failures
    pub checksum_failures: u64,
    /// Time spent reading blocks (ms)
    pub block_read_time: f64,
    /// Time spent writing blocks (ms)
    pub block_write_time: f64,
    /// When statistics were reset
    pub stats_reset: Option<DateTimeWrapper>,
}

/// Simplified bloat statistics for a database
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDatabaseBloatStats {
    /// Estimated total bloat in bytes
    pub estimated_bloat_bytes: u64,
    /// Percentage of database that is bloat
    pub bloat_percentage: f64,
    /// Whether bloat is concerning
    pub is_bloat_concerning: bool,
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_database_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let database_stats = PostgresDatabaseStats::default();

        let result = database_stats
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok(), "sync_metadata failed: {:?}", result.as_ref().err());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
        assert!(info.cache_hit_ratio >= 0.0);
        assert!(info.cache_hit_ratio <= 100.0);
        assert!(!info.database_name.is_empty());
    }
}
