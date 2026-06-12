use super::*;
use function_name::named;
impl OracleTableInfo {
    pub(crate) const STALE_STATS_THRESHOLD: f64 = 25.0;
    pub(crate) const LARGE_TABLE_THRESHOLD: u64 = 1_073_741_824; // 1GB
    // Threshold reserved for future health-check reporting
    #[allow(dead_code)]
    pub(crate) const HIGH_GROWTH_THRESHOLD: f64 = 0.1; // 10% growth
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 100;
    pub(crate) const USER_SCHEMA_EXCLUSIONS: &str = "NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'CTXSYS', 'XDB', 'APEX_040200')";

    #[inline]
    pub(crate) fn user_schema_filter(owner: &str) -> String {
        format!("{owner} {}", Self::USER_SCHEMA_EXCLUSIONS)
    }

    #[inline]
    pub(crate) fn segment_size_join(alias: &str, segment_type: &str, owner_column: &str, segment_name_column: &str) -> String {
        format!(
            "LEFT JOIN (
                        SELECT owner, segment_name, SUM(bytes) as bytes
                        FROM dba_segments
                        WHERE segment_type = '{segment_type}'
                        GROUP BY owner, segment_name
                    ) {alias} ON {owner_column} = {alias}.owner AND {segment_name_column} = {alias}.segment_name"
        )
    }

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut table_info = OracleTableInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "table_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.total_tables = row.get_u64("total_tables")?;
            table_info.partitioned_tables = row.get_u64("partitioned_tables")?;
            table_info.tables_with_stats = row.get_u64("tables_with_stats")?;
            table_info.tables_stale_stats = row.get_u64("tables_stale_stats")?;
            table_info.tables_no_stats = row.get_u64("tables_no_stats")?;
            table_info.total_table_rows = row.get_u64("total_table_rows")?;
            table_info.total_table_size_bytes = row.get_u64("total_table_size_bytes")?;
            table_info.compressed_tables = row.get_u64("compressed_tables")?;
            table_info.empty_tables = row.get_u64("empty_tables")?;
            table_info.large_tables = row.get_u64("large_tables")?;
            table_info.largest_table_size_bytes = row.get_u64("largest_table_size_bytes")?;
            table_info.avg_rows_per_table = row.get_u64("avg_rows_per_table")?;
            table_info.avg_table_size_bytes = row.get_u64("avg_table_size_bytes")?;
            table_info.tables_analyzed_24h = row.get_u64("tables_analyzed_24h")?;
        }

        if let Some(row) = run_single_row(&requests, "index_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.total_indexes = row.get_u64("total_indexes")?;
            table_info.total_index_size_bytes = row.get_u64("total_index_size_bytes")?;
            table_info.unusable_indexes = row.get_u64("unusable_indexes")?;
            table_info.invisible_indexes = row.get_u64("invisible_indexes")?;
        }

        if let Some(row) = run_single_row(&requests, "lob_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.tables_with_lobs = row.get_u64("tables_with_lobs")?;
            table_info.total_lob_size_bytes = row.get_u64("total_lob_size_bytes")?;
        }

        if let Some(row) = run_single_row(&requests, "partition_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.total_partitions = row.get_u64("total_partitions")?;
            table_info.total_subpartitions = row.get_u64("total_subpartitions")?;
        }

        if let Some(row) = run_single_row(&requests, "constraint_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.tables_with_fks = row.get_u64("tables_with_fks")?;
            table_info.tables_with_checks = row.get_u64("tables_with_checks")?;
        }

        if let Some(row) = run_single_row(&requests, "activity_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            table_info.high_activity_tables = row.get_u64("high_activity_tables")?;
            table_info.high_growth_tables = row.get_u64("high_growth_tables")?;
        }

        table_info.table_health_score = Self::calculate_health_score(&table_info);

        // Conditionally collect detailed metrics only when problems are detected
        table_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&table_info, context).await?;

        Ok(table_info)
    }
}
