use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{get_first_string, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL table information and statistics collection
///
/// This struct contains summary information about all database tables,
/// including sizes, access patterns, and maintenance status.
/// Detailed table information is collected conditionally to reduce overhead.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableInfo {
    /// Total number of user tables
    pub total_tables: u64,
    /// Number of tables needing maintenance
    pub tables_needing_maintenance: u64,
    /// Number of tables with high bloat
    pub tables_with_high_bloat: u64,
    /// Number of tables with excessive sequential scans
    pub tables_with_excessive_seq_scans: u64,
    /// Total size of all tables in bytes
    pub total_database_size: u64,
    /// Size of largest table in bytes
    pub largest_table_size: u64,
    /// Average table size in bytes
    pub average_table_size: u64,
    /// Total index size across all tables
    pub total_index_size: u64,
    /// Average dead tuple percentage across all tables
    pub average_dead_tuple_percentage: f64,
    /// Average sequential scan ratio across all tables
    pub average_seq_scan_ratio: f64,
    /// Overall table health score (0-100)
    pub overall_health_score: f64,
    /// Tables requiring immediate attention
    pub problematic_tables_count: u64,
    /// Detailed table information (collected conditionally)
    pub detailed_table_info: Option<PostgresDetailedTableInfo>,
}

/// Detailed table information collected only when issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresDetailedTableInfo {
    /// Individual table statistics
    pub tables: Vec<PostgresIndividualTableInfo>,
    /// Tables needing maintenance
    pub maintenance_candidates: Vec<PostgresTableMaintenanceInfo>,
    /// Tables with high bloat
    pub bloated_tables: Vec<PostgresTableBloatInfo>,
    /// Tables with poor access patterns
    pub poorly_accessed_tables: Vec<PostgresTableAccessInfo>,
    /// Overall recommendations
    pub recommendations: Vec<String>,
}

/// Individual table information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresIndividualTableInfo {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Table OID
    pub table_oid: u64,
    /// Table type
    pub table_type: PostgresTableType,
    /// Total table size including indexes and TOAST (bytes)
    pub total_size_bytes: u64,
    /// Table size excluding indexes (bytes)
    pub table_size_bytes: u64,
    /// Index size (bytes)
    pub index_size_bytes: u64,
    /// Human-readable total size
    pub total_size_pretty: String,
    /// Estimated number of rows
    pub estimated_row_count: u64,
    /// Average row size in bytes
    pub avg_row_size: u64,
    /// Table health score (0-100)
    pub health_score: f64,
    /// Column information (collected conditionally)
    pub columns: Option<Vec<PostgresColumnInfo>>,
    /// Table constraints summary (collected conditionally)
    pub constraints_summary: Option<PostgresConstraintsSummary>,
}

/// Table maintenance information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableMaintenanceInfo {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Last vacuum time
    pub last_vacuum: Option<DateTimeWrapper>,
    /// Last analyze time
    pub last_analyze: Option<DateTimeWrapper>,
    /// Hours since last vacuum
    pub hours_since_vacuum: Option<f64>,
    /// Hours since last analyze
    pub hours_since_analyze: Option<f64>,
    /// Whether table needs vacuum
    pub needs_vacuum: bool,
    /// Whether table needs analyze
    pub needs_analyze: bool,
    /// Vacuum count
    pub vacuum_count: u64,
    /// Analyze count
    pub analyze_count: u64,
    /// Tuples modified since last analyze
    pub modified_since_analyze: u64,
}

/// Table bloat information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableBloatInfo {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Live tuples
    pub live_tuples: u64,
    /// Dead tuples
    pub dead_tuples: u64,
    /// Dead tuple percentage
    pub dead_tuple_percentage: f64,
    /// Table size in bytes
    pub table_size_bytes: u64,
    /// Estimated bloat size in bytes
    pub estimated_bloat_bytes: u64,
    /// Bloat severity level
    pub bloat_severity: BloatSeverity,
}

/// Table access pattern information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableAccessInfo {
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Sequential scans count
    pub sequential_scans: u64,
    /// Sequential tuples read
    pub seq_tuples_read: u64,
    /// Index scans count
    pub index_scans: u64,
    /// Index tuples fetched
    pub index_tuples_fetched: u64,
    /// Sequential scan ratio percentage
    pub sequential_scan_ratio: f64,
    /// Average tuples per sequential scan
    pub avg_tuples_per_seq_scan: f64,
    /// Access pattern assessment
    pub access_pattern_assessment: String,
}

/// Bloat severity levels
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum BloatSeverity {
    /// Low bloat (< 20%)
    Low,
    /// Moderate bloat (20-40%)
    Moderate,
    /// High bloat (40-60%)
    High,
    /// Critical bloat (> 60%)
    Critical,
}

impl MetadataCollection for PostgresTableInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "table_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as total_tables,
                    COALESCE(SUM(pg_total_relation_size(c.oid))::bigint, 0) as total_size,
                    COALESCE(MAX(pg_total_relation_size(c.oid)), 0) as largest_table_size,
                    COALESCE(AVG(pg_total_relation_size(c.oid))::bigint, 0) as avg_table_size,
                    COALESCE(SUM(pg_indexes_size(c.oid))::bigint, 0) as total_index_size,
                    COALESCE(AVG(CASE WHEN c.reltuples > 0 THEN
                        pg_relation_size(c.oid) / c.reltuples::bigint
                    ELSE 0 END)::bigint, 0) as avg_row_size
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')  -- regular and partitioned tables
                    AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "maintenance_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) FILTER (WHERE
                        last_vacuum IS NULL OR last_vacuum < NOW() - INTERVAL '7 days' OR
                        last_autovacuum IS NULL OR last_autovacuum < NOW() - INTERVAL '7 days'
                    ) as tables_needing_vacuum,
                    COUNT(*) FILTER (WHERE
                        last_analyze IS NULL OR last_analyze < NOW() - INTERVAL '7 days' OR
                        last_autoanalyze IS NULL OR last_autoanalyze < NOW() - INTERVAL '7 days'
                    ) as tables_needing_analyze,
                    COUNT(*) FILTER (WHERE n_mod_since_analyze > 1000) as tables_stale_stats,
                    COALESCE(AVG(CASE WHEN (n_live_tup + n_dead_tup) > 0 THEN
                        (n_dead_tup::float / (n_live_tup + n_dead_tup)::float) * 100
                    ELSE 0 END), 0) as avg_dead_tuple_pct
                FROM pg_stat_user_tables"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "access_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COALESCE(AVG(CASE WHEN (seq_scan + idx_scan) > 0 THEN
                        (seq_scan::float / (seq_scan + idx_scan)::float) * 100
                    ELSE 0 END), 0) as avg_seq_scan_ratio,
                    COUNT(*) FILTER (WHERE
                        CASE WHEN (seq_scan + idx_scan) > 0 THEN
                            (seq_scan::float / (seq_scan + idx_scan)::float) * 100
                        ELSE 0 END > 50.0
                    ) as tables_high_seq_scan
                FROM pg_stat_user_tables
                WHERE seq_scan + idx_scan > 100"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL table information including sizes, statistics, and maintenance status"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "tables"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresTableInfo {
    const BLOAT_THRESHOLD: f64 = 25.0;
    const SEQ_SCAN_THRESHOLD: f64 = 50.0;
    const LARGE_TABLE_THRESHOLD: u64 = 1_073_741_824; // 1GB
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const MAX_DETAILED_RESULTS: usize = 100;

    const COLUMN_COLLECTION_THRESHOLD: usize = 50; // Only collect columns for top 50 tables
    const COLLECT_COLUMN_STATS: bool = true; // Set to false to skip statistics collection

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut table_info = PostgresTableInfo::default();
        let requests = self.request();

        // Execute summary queries
        let table_summary_row = run_single_row(&requests, "table_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        let maintenance_summary_row = run_single_row(&requests, "maintenance_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        let access_summary_row = run_single_row(&requests, "access_summary", context.clone(), Self::QUERY_TIMEOUT).await?;

        // Process table summary
        if table_summary_row.is_none() {
            // Empty database (no user tables detected) – report default metrics with healthy score.
            table_info.overall_health_score = 100.0;
            return Ok(table_info);
        }
        if let Some(row) = table_summary_row {
            table_info.total_tables = Self::safe_i64_to_u64(&row, "total_tables")?;
            table_info.total_database_size = Self::safe_i64_to_u64(&row, "total_size")?;
            table_info.largest_table_size = Self::safe_i64_to_u64(&row, "largest_table_size")?;
            table_info.average_table_size = Self::safe_i64_to_u64(&row, "avg_table_size")?;
            table_info.total_index_size = Self::safe_i64_to_u64(&row, "total_index_size")?;
        }

        // Process maintenance summary
        if let Some(row) = maintenance_summary_row {
            let tables_needing_vacuum = Self::safe_i64_to_u64(&row, "tables_needing_vacuum")?;
            let tables_needing_analyze = Self::safe_i64_to_u64(&row, "tables_needing_analyze")?;
            table_info.tables_needing_maintenance = tables_needing_vacuum.max(tables_needing_analyze);
            table_info.average_dead_tuple_percentage = Self::safe_get_f64(&row, "avg_dead_tuple_pct")?;
        }

        // Process access summary
        if let Some(row) = access_summary_row {
            table_info.average_seq_scan_ratio = Self::safe_get_f64(&row, "avg_seq_scan_ratio")?;
            table_info.tables_with_excessive_seq_scans = Self::safe_i64_to_u64(&row, "tables_high_seq_scan")?;
        }

        // Calculate derived metrics
        table_info.tables_with_high_bloat = Self::estimate_bloated_tables(&table_info);
        table_info.problematic_tables_count =
            table_info.tables_needing_maintenance + table_info.tables_with_high_bloat + table_info.tables_with_excessive_seq_scans;
        table_info.overall_health_score = Self::calculate_overall_health_score(&table_info);

        // Conditionally collect detailed metrics only when problems are detected
        table_info.detailed_table_info = Self::collect_detailed_table_info_if_needed(&table_info, context).await?;

        Ok(table_info)
    }

    async fn collect_detailed_table_info_if_needed(
        core_info: &PostgresTableInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresDetailedTableInfo>> {
        let needs_detailed_analysis = core_info.problematic_tables_count > 0
            || core_info.overall_health_score < 80.0
            || core_info.average_dead_tuple_percentage > Self::BLOAT_THRESHOLD
            || core_info.average_seq_scan_ratio > Self::SEQ_SCAN_THRESHOLD
            || core_info.largest_table_size > Self::LARGE_TABLE_THRESHOLD;

        if !needs_detailed_analysis {
            return Ok(None);
        }

        let mut detailed_info = PostgresDetailedTableInfo {
            tables: Vec::new(),
            maintenance_candidates: Vec::new(),
            bloated_tables: Vec::new(),
            poorly_accessed_tables: Vec::new(),
            recommendations: Vec::new(),
        };

        // NEW: Determine if we should collect column information
        let should_collect_columns = core_info.total_tables <= Self::COLUMN_COLLECTION_THRESHOLD as u64
            || core_info.overall_health_score < 70.0  // Collect columns for very unhealthy databases
            || core_info.problematic_tables_count > (core_info.total_tables / 2); // More than 50% problematic

        // Collect individual table information (now with optional columns)
        if let Ok(table_rows) = Self::query_individual_tables(context.clone()).await {
            detailed_info.tables = Self::parse_individual_tables(table_rows)?;

            // NEW: If we should collect columns, enhance the table info
            if should_collect_columns {
                Self::enhance_tables_with_columns(&mut detailed_info.tables, context.clone()).await?;
            }
        }

        // Collect maintenance candidates
        if let Ok(maintenance_rows) = Self::query_maintenance_candidates(context.clone()).await {
            detailed_info.maintenance_candidates = Self::parse_maintenance_candidates(maintenance_rows)?;
        }

        // Collect bloated tables
        if let Ok(bloat_rows) = Self::query_bloated_tables(context.clone()).await {
            detailed_info.bloated_tables = Self::parse_bloated_tables(bloat_rows)?;
        }

        // Collect poorly accessed tables
        if let Ok(access_rows) = Self::query_poorly_accessed_tables(context.clone()).await {
            detailed_info.poorly_accessed_tables = Self::parse_poorly_accessed_tables(access_rows)?;
        }

        // Generate recommendations (updated to include column-based recommendations)
        detailed_info.recommendations = Self::generate_table_recommendations(core_info, &detailed_info);

        Ok(Some(detailed_info))
    }

    // Enhance tables with column and constraint information
    async fn enhance_tables_with_columns(tables: &mut [PostgresIndividualTableInfo], context: PostgresAsync) -> ResultEP<()> {
        // Create a set of table OIDs we're interested in
        let table_oids: Vec<u64> = tables.iter().map(|t| t.table_oid).collect();

        if table_oids.is_empty() {
            return Ok(());
        }

        // Collect columns for these specific tables
        let column_rows = Self::query_table_columns_for_oids(context.clone(), &table_oids).await?;
        let columns_by_table = Self::parse_table_columns_by_oid(column_rows)?;

        // Collect column statistics if enabled
        let mut stats_by_table = HashMap::new();
        if Self::COLLECT_COLUMN_STATS
            && let Ok(stats_rows) = Self::query_column_statistics_for_oids(context.clone(), &table_oids).await
        {
            stats_by_table = Self::parse_column_statistics_by_oid(stats_rows)?;
        }

        // Collect constraints for these tables
        let constraint_rows = Self::query_table_constraints_for_oids(context.clone(), &table_oids).await?;
        let constraints_by_oid = Self::parse_table_constraints_by_oid(constraint_rows)?;

        // Enhance each table with its column information
        for table in tables.iter_mut() {
            // Add columns
            if let Some(mut table_columns) = columns_by_table.get(&table.table_oid).cloned() {
                // Merge in statistics if available
                for column in &mut table_columns {
                    let stats_key = format!("{}.{}.{}", table.schema_name, table.table_name, column.column_name);
                    if let Some(stats) = stats_by_table.get(&stats_key) {
                        column.column_stats = Some(stats.clone());
                    }
                }
                table.columns = Some(table_columns);
            }

            // Add constraints
            table.constraints_summary = constraints_by_oid.get(&table.table_oid).cloned();
        }

        Ok(())
    }

    // Query columns for specific table OIDs
    async fn query_table_columns_for_oids(context: PostgresAsync, table_oids: &[u64]) -> ResultEP<Vec<PgSimpleRow>> {
        let oids_str = table_oids.iter().map(|oid| oid.to_string()).collect::<Vec<_>>().join(",");

        let query_input = QueryInput::new(
            format!(
                "SELECT
                    n.nspname as schema_name,
                    c.relname as table_name,
                    c.oid as table_oid,
                    a.attname as column_name,
                    a.attnum as ordinal_position,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as full_type_name,
                    t.typname as data_type,
                    NOT a.attnotnull as is_nullable,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) as column_default,
                    CASE
                        WHEN t.typname IN ('varchar', 'char', 'text', 'bpchar') AND a.atttypmod > 0
                        THEN a.atttypmod - 4
                        ELSE NULL
                    END as character_maximum_length,
                    CASE
                        WHEN t.typname IN ('numeric', 'decimal') AND a.atttypmod > 0
                        THEN (a.atttypmod - 4) >> 16
                        ELSE NULL
                    END as numeric_precision,
                    CASE
                        WHEN t.typname IN ('numeric', 'decimal') AND a.atttypmod > 0
                        THEN (a.atttypmod - 4) & 65535
                        ELSE NULL
                    END as numeric_scale,
                    -- Check if column is part of primary key
                    EXISTS (
                        SELECT 1 FROM pg_constraint con
                        WHERE con.conrelid = c.oid
                        AND con.contype = 'p'
                        AND a.attnum = ANY(con.conkey)
                    ) as is_primary_key,
                    -- Check if column has foreign key
                    EXISTS (
                        SELECT 1 FROM pg_constraint con
                        WHERE con.conrelid = c.oid
                        AND con.contype = 'f'
                        AND a.attnum = ANY(con.conkey)
                    ) as is_foreign_key,
                    -- Check if column is indexed
                    EXISTS (
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = c.oid
                        AND a.attnum = ANY(i.indkey)
                    ) as is_indexed
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                WHERE c.oid = ANY(ARRAY[{}])
                    AND a.attnum > 0  -- exclude system columns
                    AND NOT a.attisdropped  -- exclude dropped columns
                ORDER BY c.oid, a.attnum",
                oids_str
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "table_columns_for_oids").await
    }

    // Query column statistics for specific table OIDs
    async fn query_column_statistics_for_oids(context: PostgresAsync, table_oids: &[u64]) -> ResultEP<Vec<PgSimpleRow>> {
        let oids_str = table_oids.iter().map(|oid| oid.to_string()).collect::<Vec<_>>().join(",");

        let query_input = QueryInput::new(
            format!(
                "SELECT
                    n.nspname as schema_name,
                    c.relname as table_name,
                    c.oid as table_oid,
                    a.attname as column_name,
                    s.n_distinct,
                    s.null_frac,
                    s.avg_width,
                    -- Most common values and frequencies (limited to first 3)
                    CASE
                        WHEN s.most_common_vals IS NOT NULL
                        THEN array_to_string(s.most_common_vals[1:3], '|')
                        ELSE NULL
                    END as most_common_values,
                    CASE
                        WHEN s.most_common_freqs IS NOT NULL
                        THEN array_to_string(s.most_common_freqs[1:3], '|')
                        ELSE NULL
                    END as most_common_freqs
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                LEFT JOIN pg_stats s ON s.schemaname = n.nspname
                    AND s.tablename = c.relname
                    AND s.attname = a.attname
                WHERE c.oid = ANY(ARRAY[{}])
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND s.n_distinct IS NOT NULL  -- Only include columns with statistics
                ORDER BY c.oid, a.attnum",
                oids_str
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "column_statistics_for_oids").await
    }

    // Query table constraints for specific table OIDs
    async fn query_table_constraints_for_oids(context: PostgresAsync, table_oids: &[u64]) -> ResultEP<Vec<PgSimpleRow>> {
        let oids_str = table_oids.iter().map(|oid| oid.to_string()).collect::<Vec<_>>().join(",");

        let query_input = QueryInput::new(
            format!(
                "SELECT
                    c.oid as table_oid,
                    COUNT(*) FILTER (WHERE con.contype = 'p') as primary_key_count,
                    COUNT(*) FILTER (WHERE con.contype = 'f') as foreign_key_count,
                    COUNT(*) FILTER (WHERE con.contype = 'u') as unique_constraint_count,
                    COUNT(*) FILTER (WHERE con.contype = 'c') as check_constraint_count,
                    (SELECT COUNT(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count
                FROM pg_class c
                LEFT JOIN pg_constraint con ON con.conrelid = c.oid
                WHERE c.oid = ANY(ARRAY[{}])
                GROUP BY c.oid
                ORDER BY c.oid",
                oids_str
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "table_constraints_for_oids").await
    }

    // Parse columns grouped by table OID
    fn parse_table_columns_by_oid(rows: Vec<PgSimpleRow>) -> ResultEP<HashMap<u64, Vec<PostgresColumnInfo>>> {
        let mut columns_by_oid: HashMap<u64, Vec<PostgresColumnInfo>> = HashMap::new();

        for row in rows {
            let table_oid = Self::safe_i64_to_u64(&row, "table_oid")?;

            let column_info = PostgresColumnInfo {
                column_name: Self::safe_get_string(&row, "column_name")?,
                ordinal_position: {
                    let text = row
                        .get("ordinal_position")
                        .ok_or_else(|| EpError::metadata("Failed to get ordinal_position: column not found or NULL".to_string()))?;
                    text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get ordinal_position: {}", e)))?
                },
                data_type: Self::safe_get_string(&row, "data_type")?,
                full_type_name: Self::safe_get_string(&row, "full_type_name")?,
                is_nullable: Self::safe_get_bool(&row, "is_nullable")?,
                column_default: Self::safe_get_optional_string(&row, "column_default")?,
                character_maximum_length: row.get("character_maximum_length").and_then(|s| s.parse::<i32>().ok()),
                numeric_precision: row.get("numeric_precision").and_then(|s| s.parse::<i32>().ok()),
                numeric_scale: row.get("numeric_scale").and_then(|s| s.parse::<i32>().ok()),
                is_primary_key: Self::safe_get_bool(&row, "is_primary_key")?,
                is_foreign_key: Self::safe_get_bool(&row, "is_foreign_key")?,
                is_indexed: Self::safe_get_bool(&row, "is_indexed")?,
                column_stats: None, // Will be filled in later
            };

            columns_by_oid.entry(table_oid).or_default().push(column_info);
        }

        Ok(columns_by_oid)
    }

    // Parse column statistics by table.column key
    fn parse_column_statistics_by_oid(rows: Vec<PgSimpleRow>) -> ResultEP<HashMap<String, PostgresColumnStats>> {
        let mut stats_map = HashMap::new();

        for row in rows {
            let schema_name = Self::safe_get_string(&row, "schema_name")?;
            let table_name = Self::safe_get_string(&row, "table_name")?;
            let column_name = Self::safe_get_string(&row, "column_name")?;

            let key = format!("{}.{}.{}", schema_name, table_name, column_name);

            let most_common_values = Self::safe_get_optional_string(&row, "most_common_values")?
                .map(|s| s.split('|').map(|v| v.to_string()).collect())
                .unwrap_or_default();

            let most_common_freqs = Self::safe_get_optional_string(&row, "most_common_freqs")?
                .map(|s| s.split('|').filter_map(|v| v.parse::<f64>().ok()).collect())
                .unwrap_or_default();

            let stats = PostgresColumnStats {
                n_distinct: row.get("n_distinct").and_then(|s| s.parse::<f64>().ok()),
                null_frac: Self::safe_get_f64(&row, "null_frac")?,
                avg_width: {
                    let text = row
                        .get("avg_width")
                        .ok_or_else(|| EpError::metadata("Failed to get avg_width: column not found or NULL".to_string()))?;
                    text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get avg_width: {}", e)))?
                },
                most_common_values,
                most_common_freqs,
            };

            stats_map.insert(key, stats);
        }

        Ok(stats_map)
    }

    // Parse table constraints by OID
    fn parse_table_constraints_by_oid(rows: Vec<PgSimpleRow>) -> ResultEP<HashMap<u64, PostgresConstraintsSummary>> {
        let mut constraints_map = HashMap::new();

        for row in rows {
            let table_oid = Self::safe_i64_to_u64(&row, "table_oid")?;

            let constraints = PostgresConstraintsSummary {
                primary_key_count: Self::safe_i64_to_u64(&row, "primary_key_count")? as u32,
                foreign_key_count: Self::safe_i64_to_u64(&row, "foreign_key_count")? as u32,
                unique_constraint_count: Self::safe_i64_to_u64(&row, "unique_constraint_count")? as u32,
                check_constraint_count: Self::safe_i64_to_u64(&row, "check_constraint_count")? as u32,
                index_count: Self::safe_i64_to_u64(&row, "index_count")? as u32,
            };

            constraints_map.insert(table_oid, constraints);
        }

        Ok(constraints_map)
    }

    async fn query_individual_tables(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                n.nspname as schema_name, c.relname as table_name, c.oid as table_oid,
                c.relkind, pg_total_relation_size(c.oid) as total_size,
                pg_relation_size(c.oid) as table_size, pg_indexes_size(c.oid) as index_size,
                pg_size_pretty(pg_total_relation_size(c.oid)) as size_pretty,
                c.reltuples::bigint as estimated_rows,
                CASE WHEN c.reltuples > 0 THEN
                    pg_relation_size(c.oid) / c.reltuples::bigint
                ELSE 0 END as avg_row_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
                AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY pg_total_relation_size(c.oid) DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "individual_tables").await
    }

    async fn query_maintenance_candidates(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                schemaname, relname,
                last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
                COALESCE(EXTRACT(EPOCH FROM (NOW() - GREATEST(last_vacuum, last_autovacuum)))/3600, NULL) as hours_since_vacuum,
                COALESCE(EXTRACT(EPOCH FROM (NOW() - GREATEST(last_analyze, last_autoanalyze)))/3600, NULL) as hours_since_analyze,
                vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
                n_mod_since_analyze,
                CASE WHEN
                    (last_vacuum IS NULL AND last_autovacuum IS NULL) OR
                    GREATEST(last_vacuum, last_autovacuum) < NOW() - INTERVAL '7 days'
                THEN true ELSE false END as needs_vacuum,
                CASE WHEN
                    (last_analyze IS NULL AND last_autoanalyze IS NULL) OR
                    GREATEST(last_analyze, last_autoanalyze) < NOW() - INTERVAL '7 days' OR
                    n_mod_since_analyze > 1000
                THEN true ELSE false END as needs_analyze
            FROM pg_stat_user_tables
            WHERE (last_vacuum IS NULL AND last_autovacuum IS NULL) OR
                  GREATEST(last_vacuum, last_autovacuum) < NOW() - INTERVAL '3 days' OR
                  (last_analyze IS NULL AND last_autoanalyze IS NULL) OR
                  GREATEST(last_analyze, last_autoanalyze) < NOW() - INTERVAL '3 days' OR
                  n_mod_since_analyze > 1000
            ORDER BY COALESCE(EXTRACT(EPOCH FROM (NOW() - GREATEST(last_vacuum, last_autovacuum)))/3600, 999999) DESC
            LIMIT {}",
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "maintenance_candidates").await
    }

    async fn query_bloated_tables(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                s.schemaname, s.tablename, s.n_live_tup, s.n_dead_tup,
                CASE WHEN (s.n_live_tup + s.n_dead_tup) > 0 THEN
                    (s.n_dead_tup::float / (s.n_live_tup + s.n_dead_tup)::float) * 100
                ELSE 0 END as dead_tuple_percentage,
                pg_relation_size(c.oid) as table_size_bytes,
                CASE WHEN (s.n_live_tup + s.n_dead_tup) > 0 THEN
                    (s.n_dead_tup::float / (s.n_live_tup + s.n_dead_tup)::float) * pg_relation_size(c.oid)
                ELSE 0 END as estimated_bloat_bytes
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.relname = s.tablename
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
            WHERE (s.n_live_tup + s.n_dead_tup) > 0
                AND (s.n_dead_tup::float / (s.n_live_tup + s.n_dead_tup)::float) * 100 > {}
            ORDER BY dead_tuple_percentage DESC
            LIMIT {}",
                Self::BLOAT_THRESHOLD,
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "bloated_tables").await
    }

    async fn query_poorly_accessed_tables(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
                CASE WHEN (seq_scan + idx_scan) > 0 THEN
                    (seq_scan::float / (seq_scan + idx_scan)::float) * 100
                ELSE 0 END as seq_scan_ratio,
                CASE WHEN seq_scan > 0 THEN seq_tup_read::float / seq_scan::float
                ELSE 0 END as avg_tuples_per_seq_scan
            FROM pg_stat_user_tables
            WHERE seq_scan + idx_scan > 100  -- Only consider active tables
                AND CASE WHEN (seq_scan + idx_scan) > 0 THEN
                    (seq_scan::float / (seq_scan + idx_scan)::float) * 100
                ELSE 0 END > {}
            ORDER BY seq_scan_ratio DESC
            LIMIT {}",
                Self::SEQ_SCAN_THRESHOLD,
                Self::MAX_DETAILED_RESULTS
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "poorly_accessed_tables").await
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

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
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

    fn safe_get_bool(row: &PgSimpleRow, column: &str) -> ResultEP<bool> {
        row.get(column)
            .map(|s| s == "t" || s == "true" || s == "1")
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_char(row: &PgSimpleRow, column: &str) -> ResultEP<char> {
        let s = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        s.chars().next().ok_or_else(|| EpError::metadata(format!("Empty string for column {}", column)))
    }

    fn estimate_bloated_tables(table_info: &PostgresTableInfo) -> u64 {
        // Estimate based on average dead tuple percentage
        if table_info.average_dead_tuple_percentage > Self::BLOAT_THRESHOLD {
            // Rough estimate: assume 20% of tables have high bloat if average is high
            (table_info.total_tables as f64 * 0.2) as u64
        } else {
            0
        }
    }

    fn calculate_overall_health_score(table_info: &PostgresTableInfo) -> f64 {
        let mut score = 100.0;

        // Deduct for maintenance issues
        if table_info.total_tables > 0 {
            let maintenance_ratio = table_info.tables_needing_maintenance as f64 / table_info.total_tables as f64;
            score -= maintenance_ratio * 30.0;
        }

        // Deduct for high bloat
        if table_info.average_dead_tuple_percentage > Self::BLOAT_THRESHOLD {
            score -= (table_info.average_dead_tuple_percentage - Self::BLOAT_THRESHOLD).min(25.0);
        }

        // Deduct for poor access patterns
        if table_info.average_seq_scan_ratio > Self::SEQ_SCAN_THRESHOLD {
            score -= (table_info.average_seq_scan_ratio - Self::SEQ_SCAN_THRESHOLD).min(20.0);
        }

        score.clamp(0.0, 100.0)
    }

    fn parse_individual_tables(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresIndividualTableInfo>> {
        let mut tables = Vec::with_capacity(rows.len());

        for row in rows {
            let relkind_char = Self::safe_get_char(&row, "relkind")?;
            let table_type = PostgresTableType::from_relkind(relkind_char);

            let total_size = Self::safe_i64_to_u64(&row, "total_size")?;
            let table_size = Self::safe_i64_to_u64(&row, "table_size")?;
            let estimated_rows = Self::safe_i64_to_u64(&row, "estimated_rows")?;

            // Calculate individual health score
            let health_score = Self::calculate_individual_table_health_score(total_size, estimated_rows);

            tables.push(PostgresIndividualTableInfo {
                schema_name: Self::safe_get_string(&row, "schema_name")?,
                table_name: Self::safe_get_string(&row, "table_name")?,
                table_oid: Self::safe_i64_to_u64(&row, "table_oid")?,
                table_type,
                total_size_bytes: total_size,
                table_size_bytes: table_size,
                index_size_bytes: Self::safe_i64_to_u64(&row, "index_size")?,
                total_size_pretty: Self::safe_get_string(&row, "size_pretty")?,
                estimated_row_count: estimated_rows,
                avg_row_size: Self::safe_i64_to_u64(&row, "avg_row_size")?,
                health_score,
                columns: None,             // NEW: Will be populated later if needed
                constraints_summary: None, // NEW: Will be populated later if needed
            });
        }

        Ok(tables)
    }

    fn parse_maintenance_candidates(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableMaintenanceInfo>> {
        let mut candidates = Vec::with_capacity(rows.len());

        for row in rows {
            candidates.push(PostgresTableMaintenanceInfo {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                last_vacuum: Self::safe_get_datetime(&row, "last_vacuum")?,
                last_analyze: Self::safe_get_datetime(&row, "last_analyze")?,
                hours_since_vacuum: row.get("hours_since_vacuum").and_then(|s| s.parse::<f64>().ok()),
                hours_since_analyze: row.get("hours_since_analyze").and_then(|s| s.parse::<f64>().ok()),
                needs_vacuum: Self::safe_get_bool(&row, "needs_vacuum")?,
                needs_analyze: Self::safe_get_bool(&row, "needs_analyze")?,
                vacuum_count: Self::safe_i64_to_u64(&row, "vacuum_count")?,
                analyze_count: Self::safe_i64_to_u64(&row, "analyze_count")?,
                modified_since_analyze: Self::safe_i64_to_u64(&row, "n_mod_since_analyze")?,
            });
        }

        Ok(candidates)
    }

    fn parse_bloated_tables(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableBloatInfo>> {
        let mut bloated = Vec::with_capacity(rows.len());

        for row in rows {
            let dead_tuple_percentage = Self::safe_get_f64(&row, "dead_tuple_percentage")?;
            let bloat_severity = Self::classify_bloat_severity(dead_tuple_percentage);

            bloated.push(PostgresTableBloatInfo {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                live_tuples: Self::safe_i64_to_u64(&row, "n_live_tup")?,
                dead_tuples: Self::safe_i64_to_u64(&row, "n_dead_tup")?,
                dead_tuple_percentage,
                table_size_bytes: Self::safe_i64_to_u64(&row, "table_size_bytes")?,
                estimated_bloat_bytes: Self::safe_get_f64(&row, "estimated_bloat_bytes")? as u64,
                bloat_severity,
            });
        }

        Ok(bloated)
    }

    fn parse_poorly_accessed_tables(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresTableAccessInfo>> {
        let mut poorly_accessed = Vec::with_capacity(rows.len());

        for row in rows {
            let seq_scan_ratio = Self::safe_get_f64(&row, "seq_scan_ratio")?;
            let avg_tuples_per_seq_scan = Self::safe_get_f64(&row, "avg_tuples_per_seq_scan")?;

            let access_pattern_assessment = Self::assess_access_pattern(seq_scan_ratio, avg_tuples_per_seq_scan);

            poorly_accessed.push(PostgresTableAccessInfo {
                schema_name: Self::safe_get_string(&row, "schemaname")?,
                table_name: get_first_string(|column| row.get(column), &["table_name", "tablename", "relname"])?,
                sequential_scans: Self::safe_i64_to_u64(&row, "seq_scan")?,
                seq_tuples_read: Self::safe_i64_to_u64(&row, "seq_tup_read")?,
                index_scans: Self::safe_i64_to_u64(&row, "idx_scan")?,
                index_tuples_fetched: Self::safe_i64_to_u64(&row, "idx_tup_fetch")?,
                sequential_scan_ratio: seq_scan_ratio,
                avg_tuples_per_seq_scan,
                access_pattern_assessment,
            });
        }

        Ok(poorly_accessed)
    }

    fn calculate_individual_table_health_score(total_size: u64, estimated_rows: u64) -> f64 {
        let mut score = 100.0;

        // Large tables get slightly lower scores due to maintenance complexity
        if total_size > Self::LARGE_TABLE_THRESHOLD {
            score -= 10.0;
        }

        // Very small tables might indicate design issues
        if estimated_rows < 100 && total_size > 1_048_576 {
            // Less than 100 rows but > 1MB
            score -= 15.0;
        }

        // score.max(0.0).min(100.0)
        score
    }

    fn classify_bloat_severity(dead_tuple_percentage: f64) -> BloatSeverity {
        match dead_tuple_percentage {
            pct if pct < 20.0 => BloatSeverity::Low,
            pct if pct < 40.0 => BloatSeverity::Moderate,
            pct if pct < 60.0 => BloatSeverity::High,
            _ => BloatSeverity::Critical,
        }
    }

    fn assess_access_pattern(seq_scan_ratio: f64, avg_tuples_per_seq_scan: f64) -> String {
        match seq_scan_ratio {
            ratio if ratio > 80.0 => {
                if avg_tuples_per_seq_scan > 10000.0 {
                    "Critical: Very high sequential scan ratio with large scans - needs immediate indexing".to_string()
                } else {
                    "High sequential scan ratio - consider adding indexes".to_string()
                }
            }
            ratio if ratio > 60.0 => "Moderate sequential scan usage - review query patterns".to_string(),
            ratio if ratio > 40.0 => "Mixed access pattern - monitor for optimization opportunities".to_string(),
            _ => "Primarily indexed access - good performance".to_string(),
        }
    }

    fn generate_table_recommendations(core_info: &PostgresTableInfo, detailed_info: &PostgresDetailedTableInfo) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Maintenance recommendations
        if !detailed_info.maintenance_candidates.is_empty() {
            recommendations.push(format!(
                "Schedule maintenance for {} tables requiring vacuum/analyze",
                detailed_info.maintenance_candidates.len()
            ));
        }

        // Bloat recommendations
        if !detailed_info.bloated_tables.is_empty() {
            let critical_bloat = detailed_info.bloated_tables.iter().filter(|t| t.bloat_severity == BloatSeverity::Critical).count();

            if critical_bloat > 0 {
                recommendations.push(format!("URGENT: {} tables have critical bloat - consider VACUUM FULL", critical_bloat));
            }

            recommendations.push(format!("Address table bloat in {} tables to reclaim space", detailed_info.bloated_tables.len()));
        }

        // Access pattern recommendations
        if !detailed_info.poorly_accessed_tables.is_empty() {
            recommendations.push(format!(
                "Review indexing strategy for {} tables with poor access patterns",
                detailed_info.poorly_accessed_tables.len()
            ));
        }

        // Column-based recommendations
        let tables_with_columns: Vec<_> = detailed_info.tables.iter().filter(|t| t.columns.is_some()).collect();

        if !tables_with_columns.is_empty() {
            // Check for tables with many nullable columns
            let tables_with_many_nulls = tables_with_columns
                .iter()
                .filter(|t| {
                    if let Some(columns) = &t.columns {
                        let nullable_ratio = columns.iter().filter(|c| c.is_nullable).count() as f64 / columns.len() as f64;
                        nullable_ratio > 0.8 // More than 80% nullable
                    } else {
                        false
                    }
                })
                .count();

            if tables_with_many_nulls > 0 {
                recommendations.push(format!("Review schema design: {} tables have >80% nullable columns", tables_with_many_nulls));
            }

            // Check for tables with unlimited text columns
            let tables_with_unlimited_text = tables_with_columns
                .iter()
                .filter(|t| {
                    if let Some(columns) = &t.columns {
                        columns.iter().any(|c| {
                            matches!(c.data_type.as_str(), "text") || (c.data_type == "varchar" && c.character_maximum_length.is_none())
                        })
                    } else {
                        false
                    }
                })
                .count();

            if tables_with_unlimited_text > 0 {
                recommendations.push(format!(
                    "Consider adding length constraints: {} tables have unlimited text columns",
                    tables_with_unlimited_text
                ));
            }

            // Check for tables without primary keys
            let tables_without_pk = tables_with_columns
                .iter()
                .filter(|t| {
                    if let Some(columns) = &t.columns {
                        !columns.iter().any(|c| c.is_primary_key)
                    } else {
                        false
                    }
                })
                .count();

            if tables_without_pk > 0 {
                recommendations.push(format!(
                    "CRITICAL: {} tables lack primary keys - impacts replication and performance",
                    tables_without_pk
                ));
            }
        }

        // Overall health recommendations
        if core_info.overall_health_score < 70.0 {
            recommendations.push("Overall table health is poor - comprehensive review recommended".to_string());
        }

        // Size-based recommendations
        if core_info.total_index_size > 0 && core_info.total_database_size > 0 {
            let index_ratio = (core_info.total_index_size as f64 / core_info.total_database_size as f64) * 100.0;
            if index_ratio > 50.0 {
                recommendations.push("High index-to-data ratio - review index necessity".to_string());
            }
        }

        recommendations
    }
}

// Helper methods for PostgresIndividualTableInfo
impl PostgresIndividualTableInfo {
    /// Check if table has column information
    pub fn has_column_info(&self) -> bool {
        self.columns.is_some()
    }

    /// Get columns by data type
    pub fn get_columns_by_type(&self, data_type: &str) -> Vec<&PostgresColumnInfo> {
        self.columns.as_ref().map(|cols| cols.iter().filter(|col| col.data_type == data_type).collect()).unwrap_or_default()
    }

    /// Get primary key columns
    pub fn get_primary_key_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.as_ref().map(|cols| cols.iter().filter(|col| col.is_primary_key).collect()).unwrap_or_default()
    }

    /// Get foreign key columns
    pub fn get_foreign_key_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.as_ref().map(|cols| cols.iter().filter(|col| col.is_foreign_key).collect()).unwrap_or_default()
    }

    /// Get indexed columns
    pub fn get_indexed_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.as_ref().map(|cols| cols.iter().filter(|col| col.is_indexed).collect()).unwrap_or_default()
    }

    /// Get column count
    pub fn get_column_count(&self) -> usize {
        self.columns.as_ref().map(|cols| cols.len()).unwrap_or(0)
    }

    /// Check if table has primary key
    pub fn has_primary_key(&self) -> bool {
        !self.get_primary_key_columns().is_empty()
    }

    /// Get constraint counts
    pub fn get_constraint_summary(&self) -> String {
        if let Some(constraints) = &self.constraints_summary {
            format!(
                "PK:{} FK:{} UQ:{} CK:{} IDX:{}",
                constraints.primary_key_count,
                constraints.foreign_key_count,
                constraints.unique_constraint_count,
                constraints.check_constraint_count,
                constraints.index_count
            )
        } else {
            "No constraint info".to_string()
        }
    }

    /// Get schema summary including columns
    pub fn get_enhanced_summary(&self) -> String {
        let column_info = if self.has_column_info() {
            format!(" | {} columns", self.get_column_count())
        } else {
            String::new()
        };

        let constraint_info = if self.constraints_summary.is_some() {
            format!(" | {}", self.get_constraint_summary())
        } else {
            String::new()
        };

        format!(
            "Table: {}.{} ({} | {:.1}MB{}{})",
            self.schema_name,
            self.table_name,
            self.table_type.description(),
            self.total_size_bytes as f64 / (1024.0 * 1024.0),
            column_info,
            constraint_info
        )
    }
}

/// PostgreSQL table types
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum PostgresTableType {
    /// Regular heap table
    #[default]
    Regular,
    /// Partitioned table
    Partitioned,
    /// Foreign table
    Foreign,
    /// Materialized view
    MaterializedView,
    /// Temporary table
    Temporary,
    /// Unlogged table
    Unlogged,
    /// Unknown type
    Unknown(String),
}

impl PostgresTableType {
    /// Parses table type from pg_class.relkind
    pub fn from_relkind(relkind: char) -> Self {
        match relkind {
            'r' => PostgresTableType::Regular,
            'p' => PostgresTableType::Partitioned,
            'f' => PostgresTableType::Foreign,
            'm' => PostgresTableType::MaterializedView,
            't' => PostgresTableType::Temporary,
            'u' => PostgresTableType::Unlogged,
            _ => PostgresTableType::Unknown(relkind.to_string()),
        }
    }

    /// Gets human-readable description
    pub fn description(&self) -> &str {
        match self {
            PostgresTableType::Regular => "Regular table",
            PostgresTableType::Partitioned => "Partitioned table",
            PostgresTableType::Foreign => "Foreign table",
            PostgresTableType::MaterializedView => "Materialized view",
            PostgresTableType::Temporary => "Temporary table",
            PostgresTableType::Unlogged => "Unlogged table",
            PostgresTableType::Unknown(_) => "Unknown table type",
        }
    }
}

impl BloatSeverity {
    /// Gets severity description
    pub fn description(&self) -> &str {
        match self {
            BloatSeverity::Low => "Low bloat",
            BloatSeverity::Moderate => "Moderate bloat",
            BloatSeverity::High => "High bloat",
            BloatSeverity::Critical => "Critical bloat",
        }
    }

    /// Gets recommended action
    pub fn recommended_action(&self) -> &str {
        match self {
            BloatSeverity::Low => "Monitor",
            BloatSeverity::Moderate => "Schedule VACUUM",
            BloatSeverity::High => "VACUUM soon",
            BloatSeverity::Critical => "VACUUM FULL immediately",
        }
    }
}

impl PostgresTableInfo {
    /// Gets database size in megabytes
    pub fn get_total_size_mb(&self) -> f64 {
        self.total_database_size as f64 / (1024.0 * 1024.0)
    }

    /// Gets database size in gigabytes
    pub fn get_total_size_gb(&self) -> f64 {
        self.total_database_size as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Gets average table size in megabytes
    pub fn get_average_table_size_mb(&self) -> f64 {
        self.average_table_size as f64 / (1024.0 * 1024.0)
    }

    /// Gets index overhead percentage
    pub fn get_index_overhead_percentage(&self) -> f64 {
        if self.total_database_size == 0 {
            0.0
        } else {
            (self.total_index_size as f64 / self.total_database_size as f64) * 100.0
        }
    }

    /// Checks if database has problematic tables
    pub fn has_problematic_tables(&self) -> bool {
        self.problematic_tables_count > 0
    }

    /// Checks if detailed table info was collected
    pub fn has_detailed_info(&self) -> bool {
        self.detailed_table_info.is_some()
    }

    /// Gets health assessment
    pub fn get_health_assessment(&self) -> String {
        match self.overall_health_score {
            score if score >= 90.0 => "Excellent".to_string(),
            score if score >= 80.0 => "Good".to_string(),
            score if score >= 70.0 => "Fair".to_string(),
            score if score >= 60.0 => "Poor".to_string(),
            _ => "Critical".to_string(),
        }
    }

    /// Gets all recommendations
    pub fn get_all_recommendations(&self) -> Vec<&String> {
        self.detailed_table_info.as_ref().map(|info| info.recommendations.iter().collect()).unwrap_or_default()
    }

    /// Gets tables needing maintenance
    pub fn get_maintenance_candidates(&self) -> Vec<&PostgresTableMaintenanceInfo> {
        self.detailed_table_info.as_ref().map(|info| info.maintenance_candidates.iter().collect()).unwrap_or_default()
    }

    /// Gets bloated tables
    pub fn get_bloated_tables(&self) -> Vec<&PostgresTableBloatInfo> {
        self.detailed_table_info.as_ref().map(|info| info.bloated_tables.iter().collect()).unwrap_or_default()
    }

    /// Gets poorly accessed tables
    pub fn get_poorly_accessed_tables(&self) -> Vec<&PostgresTableAccessInfo> {
        self.detailed_table_info.as_ref().map(|info| info.poorly_accessed_tables.iter().collect()).unwrap_or_default()
    }

    /// Gets individual table information
    pub fn get_individual_tables(&self) -> Vec<&PostgresIndividualTableInfo> {
        self.detailed_table_info.as_ref().map(|info| info.tables.iter().collect()).unwrap_or_default()
    }

    /// Checks if maintenance is urgently needed
    pub fn needs_urgent_maintenance(&self) -> bool {
        self.tables_needing_maintenance > (self.total_tables / 4) // More than 25% need maintenance
            || self.average_dead_tuple_percentage > 40.0
            || self.overall_health_score < 60.0
    }

    /// Gets maintenance priority tables
    pub fn get_priority_maintenance_tables(&self) -> Vec<String> {
        let mut priority_tables = Vec::new();

        if let Some(detailed) = &self.detailed_table_info {
            // Critical bloat tables
            for table in &detailed.bloated_tables {
                if table.bloat_severity == BloatSeverity::Critical {
                    priority_tables.push(format!("{}.{}", table.schema_name, table.table_name));
                }
            }

            // Tables that haven't been vacuumed in a long time
            for table in &detailed.maintenance_candidates {
                if table.needs_vacuum && table.hours_since_vacuum.unwrap_or(0.0) > 168.0 {
                    // 1 week
                    priority_tables.push(format!("{}.{}", table.schema_name, table.table_name));
                }
            }
        }

        priority_tables
    }

    /// Gets storage efficiency summary
    pub fn get_storage_efficiency_summary(&self) -> String {
        let index_overhead = self.get_index_overhead_percentage();
        let bloat_level = if self.average_dead_tuple_percentage > 30.0 {
            "High"
        } else if self.average_dead_tuple_percentage > 15.0 {
            "Moderate"
        } else {
            "Low"
        };

        format!(
            "Storage Efficiency: {} ({:.1}/100). Index Overhead: {:.1}%. Average Bloat: {} ({:.1}%)",
            self.get_health_assessment(),
            self.overall_health_score,
            index_overhead,
            bloat_level,
            self.average_dead_tuple_percentage
        )
    }

    /// Calculates space that could be reclaimed
    pub fn calculate_reclaimable_space(&self) -> u64 {
        if let Some(detailed) = &self.detailed_table_info {
            detailed.bloated_tables.iter().map(|table| table.estimated_bloat_bytes).sum()
        } else {
            // Rough estimate based on average dead tuple percentage
            (self.total_database_size as f64 * (self.average_dead_tuple_percentage / 100.0)) as u64
        }
    }

    /// Gets tables by size category
    pub fn get_tables_by_size_category(&self) -> (usize, usize, usize) {
        if let Some(detailed) = &self.detailed_table_info {
            let large_tables = detailed.tables.iter().filter(|t| t.total_size_bytes > Self::LARGE_TABLE_THRESHOLD).count();
            let medium_tables = detailed
                .tables
                .iter()
                .filter(|t| {
                    t.total_size_bytes > 104_857_600
                        && t.total_size_bytes <= Self::LARGE_TABLE_THRESHOLD
                }) // 100MB - 1GB
                .count();
            let small_tables = detailed.tables.len() - large_tables - medium_tables;

            (large_tables, medium_tables, small_tables)
        } else {
            (0, 0, 0)
        }
    }
}

// Add these new structs to your existing code

/// Column information for a PostgreSQL table
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresColumnInfo {
    /// Column name
    pub column_name: String,
    /// Column position in table (1-based)
    pub ordinal_position: i32,
    /// Data type name
    pub data_type: String,
    /// Full type name with modifiers (e.g., varchar(255))
    pub full_type_name: String,
    /// Whether column allows NULL
    pub is_nullable: bool,
    /// Default value if any
    pub column_default: Option<String>,
    /// Character maximum length (for string types)
    pub character_maximum_length: Option<i32>,
    /// Numeric precision (for numeric types)
    pub numeric_precision: Option<i32>,
    /// Numeric scale (for numeric types)
    pub numeric_scale: Option<i32>,
    /// Whether this is a primary key column
    pub is_primary_key: bool,
    /// Whether this column has a foreign key constraint
    pub is_foreign_key: bool,
    /// Whether this column is indexed
    pub is_indexed: bool,
    /// Column statistics (if available)
    pub column_stats: Option<PostgresColumnStats>,
}

/// Column statistics information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresColumnStats {
    /// Number of distinct values
    pub n_distinct: Option<f64>,
    /// Null fraction (0.0 to 1.0)
    pub null_frac: f64,
    /// Average width in bytes
    pub avg_width: i32,
    /// Most common values (first few)
    pub most_common_values: Vec<String>,
    /// Frequencies of most common values
    pub most_common_freqs: Vec<f64>,
}

/// Extended table information including columns
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableWithColumns {
    /// Basic table information
    pub table_info: PostgresIndividualTableInfo,
    /// Column information
    pub columns: Vec<PostgresColumnInfo>,
    /// Table constraints summary
    pub constraints_summary: PostgresConstraintsSummary,
}

/// Summary of table constraints
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Default)]
pub struct PostgresConstraintsSummary {
    /// Number of primary key constraints
    pub primary_key_count: u32,
    /// Number of foreign key constraints
    pub foreign_key_count: u32,
    /// Number of unique constraints
    pub unique_constraint_count: u32,
    /// Number of check constraints
    pub check_constraint_count: u32,
    /// Number of indexes
    pub index_count: u32,
}

// Add this field to PostgresDetailedTableInfo struct
// pub tables_with_columns: Option<Vec<PostgresTableWithColumns>>,

#[allow(dead_code)]
impl PostgresTableInfo {
    /// Query to get detailed column information for all tables
    async fn query_table_columns(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            format!(
                "SELECT
                    n.nspname as schema_name,
                    c.relname as table_name,
                    a.attname as column_name,
                    a.attnum as ordinal_position,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as full_type_name,
                    t.typname as data_type,
                    NOT a.attnotnull as is_nullable,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) as column_default,
                    CASE
                        WHEN t.typname IN ('varchar', 'char', 'text', 'bpchar')
                        THEN a.atttypmod - 4
                        ELSE NULL
                    END as character_maximum_length,
                    CASE
                        WHEN t.typname IN ('numeric', 'decimal')
                        THEN (a.atttypmod - 4) >> 16
                        ELSE NULL
                    END as numeric_precision,
                    CASE
                        WHEN t.typname IN ('numeric', 'decimal')
                        THEN (a.atttypmod - 4) & 65535
                        ELSE NULL
                    END as numeric_scale,
                    -- Check if column is part of primary key
                    EXISTS (
                        SELECT 1 FROM pg_constraint con
                        WHERE con.conrelid = c.oid
                        AND con.contype = 'p'
                        AND a.attnum = ANY(con.conkey)
                    ) as is_primary_key,
                    -- Check if column has foreign key
                    EXISTS (
                        SELECT 1 FROM pg_constraint con
                        WHERE con.conrelid = c.oid
                        AND con.contype = 'f'
                        AND a.attnum = ANY(con.conkey)
                    ) as is_foreign_key,
                    -- Check if column is indexed
                    EXISTS (
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = c.oid
                        AND a.attnum = ANY(i.indkey)
                    ) as is_indexed
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                WHERE c.relkind IN ('r', 'p')  -- regular and partitioned tables
                    AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    AND a.attnum > 0  -- exclude system columns
                    AND NOT a.attisdropped  -- exclude dropped columns
                ORDER BY n.nspname, c.relname, a.attnum
                LIMIT {}",
                Self::MAX_DETAILED_RESULTS * 50 // Allow more results for columns
            ),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "table_columns").await
    }

    /// Query to get column statistics
    async fn query_column_statistics(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                n.nspname as schema_name,
                c.relname as table_name,
                a.attname as column_name,
                s.n_distinct,
                s.null_frac,
                s.avg_width,
                -- Most common values and frequencies (limited to first 5)
                CASE
                    WHEN s.most_common_vals IS NOT NULL
                    THEN array_to_string(s.most_common_vals[1:5], '|')
                    ELSE NULL
                END as most_common_values,
                CASE
                    WHEN s.most_common_freqs IS NOT NULL
                    THEN array_to_string(s.most_common_freqs[1:5], '|')
                    ELSE NULL
                END as most_common_freqs
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            LEFT JOIN pg_stats s ON s.schemaname = n.nspname
                AND s.tablename = c.relname
                AND s.attname = a.attname
            WHERE c.relkind IN ('r', 'p')
                AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND a.attnum > 0
                AND NOT a.attisdropped
                AND s.n_distinct IS NOT NULL  -- Only include columns with statistics
            ORDER BY n.nspname, c.relname, a.attnum"
                .to_string(),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "column_statistics").await
    }

    /// Query to get table constraints summary
    async fn query_table_constraints(context: PostgresAsync) -> ResultEP<Vec<PgSimpleRow>> {
        let query_input = QueryInput::new(
            "SELECT
                n.nspname as schema_name,
                c.relname as table_name,
                COUNT(*) FILTER (WHERE con.contype = 'p') as primary_key_count,
                COUNT(*) FILTER (WHERE con.contype = 'f') as foreign_key_count,
                COUNT(*) FILTER (WHERE con.contype = 'u') as unique_constraint_count,
                COUNT(*) FILTER (WHERE con.contype = 'c') as check_constraint_count,
                (SELECT COUNT(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_constraint con ON con.conrelid = c.oid
            WHERE c.relkind IN ('r', 'p')
                AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            GROUP BY n.nspname, c.relname, c.oid
            ORDER BY n.nspname, c.relname"
                .to_string(),
            Vec::new(),
        );
        run_query_with_timeout(&query_input, context, Self::QUERY_TIMEOUT, "table_constraints").await
    }

    /// Collect detailed table information including columns
    async fn collect_tables_with_columns(context: PostgresAsync) -> ResultEP<Vec<PostgresTableWithColumns>> {
        // Get basic table info
        let table_rows = Self::query_individual_tables(context.clone()).await?;
        let tables = Self::parse_individual_tables(table_rows)?;

        // Get column information
        let column_rows = Self::query_table_columns(context.clone()).await?;
        let columns = Self::parse_table_columns(column_rows)?;

        // Get column statistics
        let stats_rows = Self::query_column_statistics(context.clone()).await?;
        let column_stats = Self::parse_column_statistics(stats_rows)?;

        // Get constraints
        let constraint_rows = Self::query_table_constraints(context.clone()).await?;
        let constraints = Self::parse_table_constraints(constraint_rows)?;

        // Combine all information
        let mut tables_with_columns = Vec::new();

        for table in tables {
            let table_key = format!("{}.{}", table.schema_name, table.table_name);

            // Get columns for this table
            let table_columns: Vec<PostgresColumnInfo> = columns
                .iter()
                .filter(|col| format!("{}.{}", col.0, col.1) == table_key)
                .map(|col| {
                    let mut column_info = col.2.clone();

                    // Add statistics if available
                    if let Some(stats) = column_stats.get(&format!("{}.{}", table_key, column_info.column_name)) {
                        column_info.column_stats = Some(stats.clone());
                    }

                    column_info
                })
                .collect();

            // Get constraints for this table
            let constraints_summary = constraints.get(&table_key).cloned().unwrap_or_default();

            tables_with_columns.push(PostgresTableWithColumns {
                table_info: table,
                columns: table_columns,
                constraints_summary,
            });
        }

        Ok(tables_with_columns)
    }

    /// Parse column information from query results
    fn parse_table_columns(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<(String, String, PostgresColumnInfo)>> {
        let mut columns = Vec::new();

        for row in rows {
            let schema_name = Self::safe_get_string(&row, "schema_name")?;
            let table_name = Self::safe_get_string(&row, "table_name")?;

            let column_info = PostgresColumnInfo {
                column_name: Self::safe_get_string(&row, "column_name")?,
                ordinal_position: {
                    let text = row
                        .get("ordinal_position")
                        .ok_or_else(|| EpError::metadata("Failed to get ordinal_position: column not found or NULL".to_string()))?;
                    text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get ordinal_position: {}", e)))?
                },
                data_type: Self::safe_get_string(&row, "data_type")?,
                full_type_name: Self::safe_get_string(&row, "full_type_name")?,
                is_nullable: Self::safe_get_bool(&row, "is_nullable")?,
                column_default: Self::safe_get_optional_string(&row, "column_default")?,
                character_maximum_length: row.get("character_maximum_length").and_then(|s| s.parse::<i32>().ok()),
                numeric_precision: row.get("numeric_precision").and_then(|s| s.parse::<i32>().ok()),
                numeric_scale: row.get("numeric_scale").and_then(|s| s.parse::<i32>().ok()),
                is_primary_key: Self::safe_get_bool(&row, "is_primary_key")?,
                is_foreign_key: Self::safe_get_bool(&row, "is_foreign_key")?,
                is_indexed: Self::safe_get_bool(&row, "is_indexed")?,
                column_stats: None, // Will be filled in later
            };

            columns.push((schema_name, table_name, column_info));
        }

        Ok(columns)
    }

    /// Parse column statistics from query results
    fn parse_column_statistics(rows: Vec<PgSimpleRow>) -> ResultEP<std::collections::HashMap<String, PostgresColumnStats>> {
        let mut stats_map = std::collections::HashMap::new();

        for row in rows {
            let schema_name = Self::safe_get_string(&row, "schema_name")?;
            let table_name = Self::safe_get_string(&row, "table_name")?;
            let column_name = Self::safe_get_string(&row, "column_name")?;

            let key = format!("{}.{}.{}", schema_name, table_name, column_name);

            let most_common_values = Self::safe_get_optional_string(&row, "most_common_values")?
                .map(|s| s.split('|').map(|v| v.to_string()).collect())
                .unwrap_or_default();

            let most_common_freqs = Self::safe_get_optional_string(&row, "most_common_freqs")?
                .map(|s| s.split('|').filter_map(|v| v.parse::<f64>().ok()).collect())
                .unwrap_or_default();

            let stats = PostgresColumnStats {
                n_distinct: row.get("n_distinct").and_then(|s| s.parse::<f64>().ok()),
                null_frac: Self::safe_get_f64(&row, "null_frac")?,
                avg_width: {
                    let text = row
                        .get("avg_width")
                        .ok_or_else(|| EpError::metadata("Failed to get avg_width: column not found or NULL".to_string()))?;
                    text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get avg_width: {}", e)))?
                },
                most_common_values,
                most_common_freqs,
            };

            stats_map.insert(key, stats);
        }

        Ok(stats_map)
    }

    /// Parse table constraints from query results
    fn parse_table_constraints(rows: Vec<PgSimpleRow>) -> ResultEP<std::collections::HashMap<String, PostgresConstraintsSummary>> {
        let mut constraints_map = std::collections::HashMap::new();

        for row in rows {
            let schema_name = Self::safe_get_string(&row, "schema_name")?;
            let table_name = Self::safe_get_string(&row, "table_name")?;
            let key = format!("{}.{}", schema_name, table_name);

            let constraints = PostgresConstraintsSummary {
                primary_key_count: Self::safe_i64_to_u64(&row, "primary_key_count")? as u32,
                foreign_key_count: Self::safe_i64_to_u64(&row, "foreign_key_count")? as u32,
                unique_constraint_count: Self::safe_i64_to_u64(&row, "unique_constraint_count")? as u32,
                check_constraint_count: Self::safe_i64_to_u64(&row, "check_constraint_count")? as u32,
                index_count: Self::safe_i64_to_u64(&row, "index_count")? as u32,
            };

            constraints_map.insert(key, constraints);
        }

        Ok(constraints_map)
    }
}

// Utility methods for PostgresTableWithColumns
#[allow(dead_code)]
impl PostgresTableWithColumns {
    /// Get columns by data type
    pub fn get_columns_by_type(&self, data_type: &str) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.data_type == data_type).collect()
    }

    /// Get primary key columns
    pub fn get_primary_key_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.is_primary_key).collect()
    }

    /// Get foreign key columns
    pub fn get_foreign_key_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.is_foreign_key).collect()
    }

    /// Get nullable columns
    pub fn get_nullable_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.is_nullable).collect()
    }

    /// Get columns with defaults
    pub fn get_columns_with_defaults(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.column_default.is_some()).collect()
    }

    /// Get indexed columns
    pub fn get_indexed_columns(&self) -> Vec<&PostgresColumnInfo> {
        self.columns.iter().filter(|col| col.is_indexed).collect()
    }

    /// Get column count by data type
    pub fn get_column_type_distribution(&self) -> std::collections::HashMap<String, usize> {
        let mut distribution = std::collections::HashMap::new();

        for column in &self.columns {
            *distribution.entry(column.data_type.clone()).or_insert(0) += 1;
        }

        distribution
    }

    /// Check if table has any text/varchar columns without length limits
    pub fn has_unlimited_text_columns(&self) -> bool {
        self.columns
            .iter()
            .any(|col| matches!(col.data_type.as_str(), "text" | "varchar") && col.character_maximum_length.is_none())
    }

    /// Get average column count
    pub fn get_column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get table schema summary
    pub fn get_schema_summary(&self) -> String {
        format!(
            "Table: {}.{} ({} columns, {} indexes, {} constraints)",
            self.table_info.schema_name,
            self.table_info.table_name,
            self.columns.len(),
            self.constraints_summary.index_count,
            self.constraints_summary.primary_key_count
                + self.constraints_summary.foreign_key_count
                + self.constraints_summary.unique_constraint_count
                + self.constraints_summary.check_constraint_count
        )
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_metadata_tables() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let table_info = PostgresTableInfo::default();

        let result = table_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.overall_health_score >= 0.0);
        assert!(info.overall_health_score <= 100.0);
        assert!(info.average_dead_tuple_percentage >= 0.0);
        assert!(info.average_seq_scan_ratio >= 0.0);
    }

    #[tokio::test]
    async fn test_postgres_table_type_parsing() {
        assert_eq!(PostgresTableType::from_relkind('r'), PostgresTableType::Regular);
        assert_eq!(PostgresTableType::from_relkind('p'), PostgresTableType::Partitioned);
        assert_eq!(PostgresTableType::from_relkind('f'), PostgresTableType::Foreign);
        assert_eq!(PostgresTableType::from_relkind('m'), PostgresTableType::MaterializedView);

        // Test unknown type
        if let PostgresTableType::Unknown(s) = PostgresTableType::from_relkind('x') {
            assert_eq!(s, "x");
        } else {
            panic!("Expected Unknown variant");
        }
    }

    #[tokio::test]
    async fn test_bloat_severity_classification() {
        assert_eq!(PostgresTableInfo::classify_bloat_severity(15.0), BloatSeverity::Low);
        assert_eq!(PostgresTableInfo::classify_bloat_severity(25.0), BloatSeverity::Moderate);
        assert_eq!(PostgresTableInfo::classify_bloat_severity(45.0), BloatSeverity::High);
        assert_eq!(PostgresTableInfo::classify_bloat_severity(65.0), BloatSeverity::Critical);
    }

    #[tokio::test]
    async fn test_table_health_score_calculation() {
        let mut table_info = PostgresTableInfo {
            total_tables: 10,
            tables_needing_maintenance: 0,
            average_dead_tuple_percentage: 5.0,
            average_seq_scan_ratio: 20.0,
            ..Default::default()
        };

        let health_score = PostgresTableInfo::calculate_overall_health_score(&table_info);
        assert!(health_score > 90.0);

        // Test with problems
        table_info.tables_needing_maintenance = 8; // 80% need maintenance
        table_info.average_dead_tuple_percentage = 35.0;
        table_info.average_seq_scan_ratio = 70.0;

        let poor_health_score = PostgresTableInfo::calculate_overall_health_score(&table_info);
        assert!(poor_health_score < 50.0);
    }

    #[tokio::test]
    async fn test_table_size_calculations() {
        let table_info = PostgresTableInfo {
            total_database_size: 2_147_483_648, // 2GB
            average_table_size: 104_857_600,    // 100MB
            total_index_size: 536_870_912,      // 512MB
            ..Default::default()
        };

        assert_eq!(table_info.get_total_size_gb(), 2.0);
        assert_eq!(table_info.get_average_table_size_mb(), 100.0);
        assert_eq!(table_info.get_index_overhead_percentage(), 25.0);
    }

    #[tokio::test]
    async fn test_maintenance_urgency() {
        let mut table_info = PostgresTableInfo {
            total_tables: 10,
            tables_needing_maintenance: 1,
            average_dead_tuple_percentage: 10.0,
            overall_health_score: 85.0,
            ..Default::default()
        };

        assert!(!table_info.needs_urgent_maintenance());

        // Make it urgent
        table_info.tables_needing_maintenance = 4; // 40% need maintenance
        table_info.overall_health_score = 55.0;

        assert!(table_info.needs_urgent_maintenance());
    }
}
