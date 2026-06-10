#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{RowExt, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL index information and statistics
///
/// Simplified struct containing essential metrics about database indexes,
/// focusing on usage patterns, sizes, and maintenance indicators.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresIndexInfo {
    /// Total number of user indexes
    pub total_indexes: u32,
    /// Number of unused indexes (< 10 scans)
    pub unused_indexes: u32,
    /// Number of large indexes (> 100MB)
    pub large_indexes: u32,
    /// Total index size in bytes
    pub total_index_size_bytes: u64,
    /// Human-readable total index size
    pub total_index_size_pretty: String,
    /// Average index cache hit ratio
    pub avg_cache_hit_ratio: f64,
    /// Number of indexes with high bloat (> 25%)
    pub high_bloat_indexes: u32,
    /// Overall index health score (0.0 to 100.0)
    pub health_score: f64,
    /// Detailed metrics collected only when issues are detected
    pub detailed_metrics: Option<PostgresIndexDetailedMetrics>,
}

/// Individual index information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresIndex {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Index name
    pub index_name: String,
    /// Index type (btree, hash, gist, gin, etc.)
    pub index_type: PostgresIndexType,
    /// Index size in bytes
    pub index_size_bytes: u64,
    /// Human-readable index size
    pub index_size_pretty: String,
    /// Whether index is unique
    pub is_unique: bool,
    /// Whether index is primary key
    pub is_primary_key: bool,
    /// Number of index scans
    pub index_scans: u64,
    /// Index cache hit ratio (0.0 to 100.0)
    pub cache_hit_ratio: f64,
    /// Usage frequency classification
    pub usage_frequency: PostgresIndexUsageFrequency,
    /// Estimated bloat percentage
    pub bloat_percentage: f64,
    /// Index efficiency score (0.0 to 100.0)
    pub efficiency_score: f64,
}

/// Detailed index metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresIndexDetailedMetrics {
    /// Unused indexes (collected when unused_indexes > 0)
    pub unused_indexes: Option<Vec<PostgresIndex>>,
    /// Indexes with high bloat (collected when high_bloat_indexes > 0)
    pub bloated_indexes: Option<Vec<PostgresIndex>>,
    /// Large indexes with low usage (collected when large_indexes > 5)
    pub inefficient_large_indexes: Option<Vec<PostgresIndex>>,
    /// Potentially duplicate indexes (collected when total_indexes > 20)
    pub potential_duplicates: Option<Vec<PostgresDuplicateIndexGroup>>,
    /// Maintenance recommendations
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresIndexInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([(
            "index_summary".to_string(),
            QueryInput::new(
                "SELECT
                        COUNT(*) AS total_indexes,
                        COUNT(*) FILTER (WHERE s.idx_scan < 10) AS unused_indexes,
                        COUNT(*) FILTER (WHERE pg_relation_size(s.indexrelid) > 104857600) AS large_indexes,
                        COUNT(*) FILTER (
                            WHERE s.idx_tup_read > 0
                              AND (s.idx_tup_read - s.idx_tup_fetch)::float
                                  / NULLIF(s.idx_tup_read::float, 0) > 0.2
                        ) AS high_bloat_indexes,
                        COALESCE(SUM(pg_relation_size(s.indexrelid))::bigint, 0) AS total_size,
                        pg_size_pretty(COALESCE(SUM(pg_relation_size(s.indexrelid))::bigint, 0)) AS total_size_pretty,
                        COALESCE(
                            AVG(
                                CASE
                                    WHEN (io.idx_blks_read + io.idx_blks_hit) > 0 THEN
                                        (io.idx_blks_hit::float
                                         / (io.idx_blks_read + io.idx_blks_hit)::float) * 100
                                    ELSE NULL
                                END
                            ),
                            100
                        ) AS avg_cache_hit_ratio
                    FROM pg_stat_user_indexes s
                    JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid"
                    .to_string(),
                Vec::new(),
            ),
        )])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL index statistics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "indexes"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresIndexInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const UNUSED_SCAN_THRESHOLD: u64 = 10;
    const LARGE_INDEX_THRESHOLD: u64 = 104857600; // 100MB
    // Threshold reserved for future health-check reporting
    #[allow(dead_code)]
    const BLOAT_THRESHOLD: f64 = 25.0;
    const MANY_INDEXES_THRESHOLD: u32 = 20;
    const LARGE_INDEX_COUNT_THRESHOLD: u32 = 5;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut index_info = PostgresIndexInfo::default();
        let requests = self.request();

        // Execute core index summary query
        let summary_row = run_single_row(&requests, "index_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        if summary_row.is_none() {
            // No user indexes found (empty database); return default metrics that reflect the absence of data.
            index_info.avg_cache_hit_ratio = 100.0;
            index_info.health_score = index_info.calculate_health_score();
            return Ok(index_info);
        }

        // Parse summary metrics
        if let Some(row) = summary_row {
            index_info.total_indexes = row.get_u64("total_indexes")? as u32;
            index_info.unused_indexes = row.get_u64("unused_indexes")? as u32;
            index_info.large_indexes = row.get_u64("large_indexes")? as u32;
            index_info.high_bloat_indexes = row.get_u64("high_bloat_indexes")? as u32;
            index_info.total_index_size_bytes = row.get_u64("total_size")?;
            index_info.total_index_size_pretty = row.get_string("total_size_pretty")?;
            index_info.avg_cache_hit_ratio = row.get_f64("avg_cache_hit_ratio")?;
        }

        // Calculate health score
        index_info.health_score = index_info.calculate_health_score();

        // Conditionally collect detailed metrics only when issues are detected
        index_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&index_info, context).await?;

        Ok(index_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresIndexInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresIndexDetailedMetrics>> {
        let needs_unused_analysis = core_info.unused_indexes > 0;
        let needs_bloat_analysis = core_info.high_bloat_indexes > 0;
        let needs_large_index_analysis = core_info.large_indexes > Self::LARGE_INDEX_COUNT_THRESHOLD;
        let needs_duplicate_analysis = core_info.total_indexes > Self::MANY_INDEXES_THRESHOLD;

        if !needs_unused_analysis && !needs_bloat_analysis && !needs_large_index_analysis && !needs_duplicate_analysis {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresIndexDetailedMetrics {
            unused_indexes: None,
            bloated_indexes: None,
            inefficient_large_indexes: None,
            potential_duplicates: None,
            recommendations: core_info.generate_recommendations(),
        };

        // Collect unused indexes
        if needs_unused_analysis {
            let unused_input = QueryInput::new(
                format!(
                    "SELECT
                    s.schemaname, s.relname, s.indexrelname,
                    am.amname as index_type,
                    pg_relation_size(s.indexrelid) as index_size,
                    pg_size_pretty(pg_relation_size(s.indexrelid)) as size_pretty,
                    i.indisunique, i.indisprimary,
                    s.idx_scan,
                    CASE WHEN (io.idx_blks_read + io.idx_blks_hit) > 0 THEN
                        (io.idx_blks_hit::float / (io.idx_blks_read + io.idx_blks_hit)::float) * 100
                    ELSE 100 END as cache_hit_ratio,
                    CASE
                        WHEN s.idx_tup_read > 0 THEN
                            ((s.idx_tup_read - s.idx_tup_fetch)::float / s.idx_tup_read::float) * 100
                        ELSE 0
                    END as bloat_ratio
                FROM pg_stat_user_indexes s
                JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid
                JOIN pg_index i ON i.indexrelid = s.indexrelid
                JOIN pg_class c ON c.oid = s.indexrelid
                JOIN pg_am am ON am.oid = c.relam
                WHERE s.idx_scan < {}
                ORDER BY s.idx_scan ASC, pg_relation_size(s.indexrelid) DESC
                LIMIT 20",
                    Self::UNUSED_SCAN_THRESHOLD
                ),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&unused_input, context.clone(), Self::QUERY_TIMEOUT, "unused_indexes").await?;
            detailed_metrics.unused_indexes = Some(Self::parse_indexes(rows)?);
        }

        // Collect large indexes with low usage
        if needs_large_index_analysis {
            let large_inefficient_input = QueryInput::new(
                format!(
                    "SELECT
                    s.schemaname, s.relname, s.indexrelname,
                    am.amname as index_type,
                    pg_relation_size(s.indexrelid) as index_size,
                    pg_size_pretty(pg_relation_size(s.indexrelid)) as size_pretty,
                    i.indisunique, i.indisprimary,
                    s.idx_scan,
                    CASE WHEN (io.idx_blks_read + io.idx_blks_hit) > 0 THEN
                        (io.idx_blks_hit::float / (io.idx_blks_read + io.idx_blks_hit)::float) * 100
                    ELSE 100 END as cache_hit_ratio,
                    CASE
                        WHEN s.idx_tup_read > 0 THEN
                            ((s.idx_tup_read - s.idx_tup_fetch)::float / s.idx_tup_read::float) * 100
                        ELSE 0
                    END as bloat_ratio
                FROM pg_stat_user_indexes s
                JOIN pg_statio_user_indexes io ON s.indexrelid = io.indexrelid
                JOIN pg_index i ON i.indexrelid = s.indexrelid
                JOIN pg_class c ON c.oid = s.indexrelid
                JOIN pg_am am ON am.oid = c.relam
                WHERE pg_relation_size(s.indexrelid) > {}
                    AND s.idx_scan < 1000
                ORDER BY pg_relation_size(s.indexrelid) DESC, s.idx_scan ASC
                LIMIT 15",
                    Self::LARGE_INDEX_THRESHOLD
                ),
                Vec::new(),
            );

            let rows =
                run_query_with_timeout(&large_inefficient_input, context.clone(), Self::QUERY_TIMEOUT, "inefficient_large_indexes").await?;
            detailed_metrics.inefficient_large_indexes = Some(Self::parse_indexes(rows)?);
        }

        // Collect potential duplicate indexes
        if needs_duplicate_analysis {
            let duplicates_input = QueryInput::new(
                "WITH index_columns AS (
                    SELECT
                        s.schemaname, s.relname, s.indexrelname,
                        array_to_string(ARRAY(
                            SELECT pg_get_indexdef(i.indexrelid, k + 1, true)
                            FROM generate_subscripts(i.indkey, 1) as k
                            ORDER BY k
                        ), ',') as columns,
                        pg_relation_size(s.indexrelid) as index_size
                    FROM pg_stat_user_indexes s
                    JOIN pg_index i ON i.indexrelid = s.indexrelid
                )
                SELECT
                    schemaname, relname, columns,
                    to_json(array_agg(indexrelname ORDER BY index_size DESC)) as index_names,
                    array_agg(index_size ORDER BY index_size DESC) as index_sizes
                FROM index_columns
                GROUP BY schemaname, relname, columns
                HAVING COUNT(*) > 1
                ORDER BY schemaname, relname
                LIMIT 10"
                    .to_string(),
                Vec::new(),
            );

            let rows = run_query_with_timeout(&duplicates_input, context.clone(), Self::QUERY_TIMEOUT, "potential_duplicates").await?;
            detailed_metrics.potential_duplicates = Some(Self::parse_duplicate_groups(rows)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_indexes(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresIndex>> {
        let mut indexes = Vec::with_capacity(rows.len());

        for row in rows {
            let index_scans = row.get_u64("idx_scan")?;
            let usage_frequency = PostgresIndexUsageFrequency::classify_from_scans(index_scans);
            let cache_hit_ratio = row.get_f64("cache_hit_ratio")?;
            let index_size = row.get_u64("index_size")?;
            let bloat_percentage = row.get_f64("bloat_ratio")?.max(0.0);

            // Calculate efficiency score
            let efficiency_score = Self::calculate_efficiency_score(&usage_frequency, cache_hit_ratio, index_size, bloat_percentage);

            indexes.push(PostgresIndex {
                schema_name: row.get_string("schemaname")?,
                table_name: row.get_string("relname")?,
                index_name: row.get_string("indexrelname")?,
                index_type: PostgresIndexType::from_amname(&row.get_string("index_type")?),
                index_size_bytes: index_size,
                index_size_pretty: row.get_string("size_pretty")?,
                is_unique: row.get_bool("indisunique")?,
                is_primary_key: row.get_bool("indisprimary")?,
                index_scans,
                cache_hit_ratio,
                usage_frequency,
                bloat_percentage,
                efficiency_score,
            });
        }

        Ok(indexes)
    }

    fn parse_duplicate_groups(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresDuplicateIndexGroup>> {
        let mut groups = Vec::with_capacity(rows.len());

        for row in rows {
            let index_names_value = row.get_json("index_names")?;
            let index_names: Vec<String> = serde_json::from_value(index_names_value)
                .map_err(|e| EpError::metadata(format!("Failed to parse duplicate index names: {e}")))?;

            groups.push(PostgresDuplicateIndexGroup {
                schema_name: row.get_string("schemaname")?,
                table_name: row.get_string("relname")?,
                columns: row.get_string("columns")?,
                duplicate_indexes: index_names,
            });
        }

        Ok(groups)
    }

    fn calculate_efficiency_score(
        usage_frequency: &PostgresIndexUsageFrequency,
        cache_hit_ratio: f64,
        index_size_bytes: u64,
        bloat_percentage: f64,
    ) -> f64 {
        let mut score = 0.0;

        // Usage component (40% of score)
        let usage_score = match usage_frequency {
            PostgresIndexUsageFrequency::VeryHigh => 40.0,
            PostgresIndexUsageFrequency::High => 35.0,
            PostgresIndexUsageFrequency::Medium => 25.0,
            PostgresIndexUsageFrequency::Low => 10.0,
            PostgresIndexUsageFrequency::Unused => 0.0,
        };
        score += usage_score;

        // Cache hit ratio component (25% of score)
        score += cache_hit_ratio * 0.25;

        // Size efficiency component (20% of score)
        let size_mb = index_size_bytes as f64 / (1024.0 * 1024.0);
        let size_efficiency = if size_mb < 1.0 {
            20.0
        } else if size_mb < 10.0 {
            15.0
        } else if size_mb < 100.0 {
            10.0
        } else {
            5.0
        };
        score += size_efficiency;

        // Bloat component (15% of score)
        let bloat_penalty = bloat_percentage * 0.15;
        score += 15.0 - bloat_penalty.min(15.0);

        score.clamp(0.0, 100.0)
    }
}

impl PostgresIndexInfo {
    /// Calculates overall index health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for unused indexes
        if self.total_indexes > 0 {
            let unused_ratio = self.unused_indexes as f64 / self.total_indexes as f64;
            score -= unused_ratio * 30.0;
        }

        // Deduct for low cache hit ratio
        if self.avg_cache_hit_ratio < 90.0 {
            score -= (90.0 - self.avg_cache_hit_ratio) * 0.5;
        }

        // Deduct for high bloat
        if self.total_indexes > 0 {
            let bloat_ratio = self.high_bloat_indexes as f64 / self.total_indexes as f64;
            score -= bloat_ratio * 20.0;
        }

        // Deduct for too many large indexes relative to total
        if self.total_indexes > 0 {
            let large_ratio = self.large_indexes as f64 / self.total_indexes as f64;
            if large_ratio > 0.3 {
                score -= (large_ratio - 0.3) * 40.0;
            }
        }

        score.max(0.0)
    }

    /// Generates index management recommendations
    fn generate_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.unused_indexes > 0 {
            recommendations.push(format!(
                "{} unused index(es) detected - consider dropping to save space and maintenance overhead",
                self.unused_indexes
            ));
        }

        if self.avg_cache_hit_ratio < 90.0 {
            recommendations.push(format!(
                "Low average index cache hit ratio ({:.1}%) - consider increasing shared_buffers or reviewing index usage patterns",
                self.avg_cache_hit_ratio
            ));
        }

        if self.high_bloat_indexes > 0 {
            recommendations.push(format!(
                "{} index(es) with high bloat detected - consider REINDEX operations",
                self.high_bloat_indexes
            ));
        }

        if self.large_indexes > 5 {
            recommendations.push(format!(
                "{} large index(es) (>100MB) detected - review if all are necessary and efficiently used",
                self.large_indexes
            ));
        }

        let total_size_gb = self.total_index_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if total_size_gb > 10.0 && self.unused_indexes > 2 {
            recommendations.push(
                "Large total index size with unused indexes - dropping unused indexes could significantly reduce storage".to_string(),
            );
        }

        if self.total_indexes == 0 {
            recommendations.push("No user indexes found - consider adding indexes to improve query performance".to_string());
        }

        recommendations
    }

    /// Gets total index size in GB
    pub fn get_total_size_gb(&self) -> f64 {
        self.total_index_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets index health summary
    pub fn get_index_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Indexes are well-maintained and efficient".to_string(),
            75..=89 => "Good - Most indexes are performing well".to_string(),
            60..=74 => "Fair - Some index maintenance needed".to_string(),
            40..=59 => "Poor - Multiple index issues detected".to_string(),
            _ => "Critical - Index management requires immediate attention".to_string(),
        }
    }

    /// Gets unused index ratio
    pub fn get_unused_index_ratio(&self) -> f64 {
        if self.total_indexes == 0 {
            0.0
        } else {
            self.unused_indexes as f64 / self.total_indexes as f64
        }
    }
}

/// Group of potentially duplicate indexes
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDuplicateIndexGroup {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Indexed columns (comma-separated)
    pub columns: String,
    /// Names of duplicate indexes
    pub duplicate_indexes: Vec<String>,
}

/// PostgreSQL index types
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresIndexType {
    /// B-tree index (default)
    BTree,
    /// Hash index
    Hash,
    /// GiST (Generalized Search Tree)
    GiST,
    /// SP-GiST (Space-Partitioned GiST)
    SPGiST,
    /// GIN (Generalized Inverted Index)
    GIN,
    /// BRIN (Block Range Index)
    BRIN,
    /// Bloom filter index
    Bloom,
    /// Unknown index type
    Unknown(String),
}

impl PostgresIndexType {
    /// Parses index type from access method name
    pub fn from_amname(amname: &str) -> Self {
        match amname.to_lowercase().as_str() {
            "btree" => PostgresIndexType::BTree,
            "hash" => PostgresIndexType::Hash,
            "gist" => PostgresIndexType::GiST,
            "spgist" => PostgresIndexType::SPGiST,
            "gin" => PostgresIndexType::GIN,
            "brin" => PostgresIndexType::BRIN,
            "bloom" => PostgresIndexType::Bloom,
            _ => PostgresIndexType::Unknown(amname.to_string()),
        }
    }
}

/// Index usage frequency classification
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresIndexUsageFrequency {
    /// Very frequently used
    VeryHigh,
    /// Frequently used
    High,
    /// Moderately used
    Medium,
    /// Rarely used
    Low,
    /// Never or almost never used
    Unused,
}

impl PostgresIndexUsageFrequency {
    /// Classifies usage frequency based on scan count
    pub fn classify_from_scans(scans: u64) -> Self {
        if scans > 10000 {
            PostgresIndexUsageFrequency::VeryHigh
        } else if scans > 1000 {
            PostgresIndexUsageFrequency::High
        } else if scans > 100 {
            PostgresIndexUsageFrequency::Medium
        } else if scans > 10 {
            PostgresIndexUsageFrequency::Low
        } else {
            PostgresIndexUsageFrequency::Unused
        }
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_index_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let index_info = PostgresIndexInfo::default();

        let result = index_info
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
        assert!(info.avg_cache_hit_ratio >= 0.0);
        assert!(info.avg_cache_hit_ratio <= 100.0);
    }
}
