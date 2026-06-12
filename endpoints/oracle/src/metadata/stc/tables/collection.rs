use super::*;
impl MetadataCollection for OracleTableInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "table_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(*) as total_tables,
                    COUNT(CASE WHEN partitioned = 'YES' THEN 1 END) as partitioned_tables,
                    COUNT(CASE WHEN last_analyzed IS NOT NULL THEN 1 END) as tables_with_stats,
                    COUNT(CASE WHEN last_analyzed IS NOT NULL
                              AND last_analyzed < SYSDATE - 7 THEN 1 END) as tables_stale_stats,
                    COUNT(CASE WHEN last_analyzed IS NULL THEN 1 END) as tables_no_stats,
                    SUM(num_rows) as total_table_rows,
                    SUM(NVL(bytes, 0)) as total_table_size_bytes,
                    COUNT(CASE WHEN compression = 'ENABLED' THEN 1 END) as compressed_tables,
                    COUNT(CASE WHEN num_rows = 0 OR num_rows IS NULL THEN 1 END) as empty_tables,
                    COUNT(CASE WHEN NVL(bytes, 0) > 1073741824 THEN 1 END) as large_tables,
                    MAX(NVL(bytes, 0)) as largest_table_size_bytes,
                    AVG(num_rows) as avg_rows_per_table,
                    AVG(NVL(bytes, 0)) as avg_table_size_bytes,
                    COUNT(CASE WHEN last_analyzed >= SYSDATE - 1 THEN 1 END) as tables_analyzed_24h
                FROM (
                    SELECT t.table_name, t.partitioned, t.num_rows, t.last_analyzed,
                           t.compression, s.bytes
                    FROM dba_tables t
                    {}
                    WHERE {}
                )",
                        Self::segment_size_join("s", "TABLE", "t.owner", "t.table_name"),
                        Self::user_schema_filter("t.owner")
                    )
                    .to_string(),
                ),
            ),
            (
                "index_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(*) as total_indexes,
                    SUM(NVL(bytes, 0)) as total_index_size_bytes,
                    COUNT(CASE WHEN status = 'UNUSABLE' THEN 1 END) as unusable_indexes,
                    COUNT(CASE WHEN visibility = 'INVISIBLE' THEN 1 END) as invisible_indexes
                FROM dba_indexes i
                {}
                    WHERE {}",
                        Self::segment_size_join("s", "INDEX", "i.owner", "i.index_name"),
                        Self::user_schema_filter("i.owner")
                    )
                    .to_string(),
                ),
            ),
            (
                "lob_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(DISTINCT table_name) as tables_with_lobs,
                    SUM(NVL(bytes, 0)) as total_lob_size_bytes
                FROM dba_lobs l
                {}
                    WHERE {}",
                        Self::segment_size_join("s", "LOBSEGMENT", "l.owner", "l.segment_name"),
                        Self::user_schema_filter("l.owner")
                    )
                    .to_string(),
                ),
            ),
            (
                "partition_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(*) as total_partitions,
                    COUNT(CASE WHEN subpartition_count > 0 THEN 1 END) as total_subpartitions
                FROM dba_tab_partitions
                WHERE {}",
                        Self::user_schema_filter("table_owner")
                    )
                    .to_string(),
                ),
            ),
            (
                "constraint_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(DISTINCT CASE WHEN constraint_type = 'R' THEN table_name END) as tables_with_fks,
                    COUNT(DISTINCT CASE WHEN constraint_type = 'C' THEN table_name END) as tables_with_checks
                FROM dba_constraints
                WHERE {}
                  AND constraint_type IN ('R', 'C')",
                        Self::user_schema_filter("owner")
                    )
                    .to_string(),
                ),
            ),
            (
                "activity_summary".to_string(),
                crate::metadata::stc::utils::query(
                    format!(
                        "SELECT
                    COUNT(CASE WHEN inserts + updates + deletes > 1000 THEN 1 END) as high_activity_tables,
                    COUNT(CASE WHEN (inserts + updates + deletes) > 0
                              AND NVL(num_rows, 0) > 0
                              AND ((inserts + updates + deletes) / NVL(num_rows, 1)) > 0.1 THEN 1 END) as high_growth_tables
                FROM dba_tab_modifications m
                JOIN dba_tables t ON m.table_owner = t.owner AND m.table_name = t.table_name
                WHERE {}",
                        Self::user_schema_filter("m.table_owner")
                    )
                    .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle table and index metrics", "tables", SyncFrequency::Medium);
}
