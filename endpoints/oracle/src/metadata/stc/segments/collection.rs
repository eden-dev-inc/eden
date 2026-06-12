use super::*;
impl MetadataCollection for OracleSegmentInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "segment_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_segments,
                    COUNT(CASE WHEN segment_type LIKE 'TABLE%' THEN 1 END) as table_segments,
                    COUNT(CASE WHEN segment_type LIKE 'INDEX%' THEN 1 END) as index_segments,
                    COUNT(CASE WHEN segment_type LIKE 'LOB%' THEN 1 END) as lob_segments,
                    COUNT(CASE WHEN segment_type = 'TEMPORARY' THEN 1 END) as temp_segments,
                    SUM(bytes) as total_allocated_space,
                    SUM(CASE WHEN segment_type LIKE 'TABLE%' THEN bytes ELSE 0 END) as table_space,
                    SUM(CASE WHEN segment_type LIKE 'INDEX%' THEN bytes ELSE 0 END) as index_space,
                    MAX(bytes) as largest_segment_size,
                    COUNT(CASE WHEN bytes > 1073741824 THEN 1 END) as large_segments_count,
                    SUM(extents) as total_extents,
                    CASE WHEN SUM(extents) > 0 THEN SUM(bytes) / SUM(extents) ELSE 0 END as avg_extent_size
                FROM dba_segments
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS')"
                        .to_string(),
                ),
            ),
            (
                "space_usage".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(bytes) as total_tablespace_size,
                    SUM(bytes - NVL(free_bytes, 0)) as total_used_space,
                    SUM(NVL(free_bytes, 0)) as total_free_space,
                    COUNT(CASE WHEN (bytes - NVL(free_bytes, 0)) / bytes > 0.9 THEN 1 END) as tablespaces_with_issues
                FROM (
                    SELECT
                        ts.tablespace_name,
                        ts.bytes,
                        fs.free_bytes
                    FROM (
                        SELECT tablespace_name, SUM(bytes) as bytes
                        FROM dba_data_files
                        GROUP BY tablespace_name
                    ) ts
                    LEFT JOIN (
                        SELECT tablespace_name, SUM(bytes) as free_bytes
                        FROM dba_free_space
                        GROUP BY tablespace_name
                    ) fs ON ts.tablespace_name = fs.tablespace_name
                )"
                    .to_string(),
                ),
            ),
            (
                "growth_analysis".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as growing_segments_count,
                    NVL(SUM(bytes), 0) as space_allocated_24h
                FROM dba_segments
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS')
                    AND extents > 1"
                    .to_string(),
                ),
            ),
            (
                "fragmentation_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(CASE WHEN extents > 100 THEN 1 END) as fragmented_segments_count,
                    SUM(CASE WHEN extents > 100 THEN (extents - 1) * initial_extent ELSE 0 END) as fragmentation_waste
                FROM dba_segments
                WHERE (segment_type LIKE 'TABLE%' OR segment_type LIKE 'INDEX%')
                    AND owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS')"
                        .to_string(),
                ),
            ),
            (
                "chaining_analysis".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(DISTINCT table_name) as chained_segments_count
                FROM dba_tables
                WHERE (chain_cnt > 0 OR chain_cnt IS NULL)
                    AND owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS')"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle segment space and fragmentation metrics", "segment", SyncFrequency::Low);
}

use crate::api::lib::query::QueryInput;
