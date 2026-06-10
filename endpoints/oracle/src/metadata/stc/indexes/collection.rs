use super::*;
impl MetadataCollection for OracleIndexInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            ("index_basic_info".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    i.owner,
                    i.index_name,
                    i.table_name,
                    i.tablespace_name,
                    i.index_type,
                    i.uniqueness,
                    i.status,
                    i.visibility,
                    i.created,
                    i.last_analyzed,
                    i.compression,
                    i.prefix_length,
                    (SELECT COUNT(*) FROM dba_ind_columns ic WHERE ic.index_owner = i.owner AND ic.index_name = i.index_name) as column_count,
                    (SELECT LISTAGG(ic.column_name, ', ') WITHIN GROUP (ORDER BY ic.column_position)
                     FROM dba_ind_columns ic
                     WHERE ic.index_owner = i.owner AND ic.index_name = i.index_name) as column_names
                FROM dba_indexes i
                WHERE i.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')
                    AND i.index_type != 'LOB'
                ORDER BY i.owner, i.index_name".to_string())
            ),
            ("index_statistics".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    i.owner,
                    i.index_name,
                    i.leaf_blocks,
                    i.distinct_keys,
                    i.avg_leaf_blocks_per_key,
                    i.avg_data_blocks_per_key,
                    i.clustering_factor,
                    i.num_rows,
                    i.sample_size,
                    i.blevel,
                    CASE
                        WHEN i.num_rows > 0 AND i.distinct_keys > 0
                        THEN ROUND(i.distinct_keys / i.num_rows, 6)
                        ELSE 0
                    END as selectivity
                FROM dba_indexes i
                WHERE i.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')
                    AND i.index_type != 'LOB'
                ORDER BY i.owner, i.index_name".to_string())
            ),
            ("index_usage".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    iu.owner,
                    iu.name as index_name,
                    NVL(iu.total_access_count, 0) as total_access_count,
                    iu.last_used,
                    CASE
                        WHEN iu.total_access_count IS NULL THEN 0
                        WHEN iu.total_access_count = 0 THEN 0
                        WHEN iu.total_access_count < 10 THEN 10
                        WHEN iu.total_access_count < 100 THEN 30
                        WHEN iu.total_access_count < 1000 THEN 60
                        WHEN iu.total_access_count < 10000 THEN 80
                        ELSE 100
                    END as usage_score
                FROM dba_index_usage iu
                WHERE iu.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')".to_string())
            ),
            ("index_storage".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    s.owner,
                    s.segment_name as index_name,
                    s.bytes as index_size_bytes,
                    s.extents,
                    s.initial_extent,
                    s.next_extent,
                    s.max_extents,
                    s.pct_increase,
                    NVL(i.pct_free, 10) as pct_free
                FROM dba_segments s
                JOIN dba_indexes i ON s.owner = i.owner AND s.segment_name = i.index_name
                WHERE s.segment_type LIKE 'INDEX%'
                    AND s.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')
                ORDER BY s.owner, s.segment_name".to_string())
            ),
            ("index_health".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    i.owner,
                    i.index_name,
                    CASE
                        WHEN i.blevel > 4 THEN i.blevel * 10
                        WHEN i.clustering_factor > i.num_rows * 2 THEN 40
                        WHEN i.leaf_blocks > 0 AND i.distinct_keys > 0 AND
                             (i.leaf_blocks / i.distinct_keys) > 10 THEN 30
                        ELSE 0
                    END as fragmentation_level,
                    CASE
                        WHEN i.blevel > 4 THEN 1
                        WHEN i.clustering_factor > i.num_rows * 2 THEN 1
                        WHEN i.leaf_blocks > 0 AND i.distinct_keys > 0 AND
                             (i.leaf_blocks / i.distinct_keys) > 10 THEN 1
                        ELSE 0
                    END as needs_rebuild,
                    CASE
                        WHEN i.blevel > 4 THEN 'High B-tree depth (' || i.blevel || ')'
                        WHEN i.clustering_factor > i.num_rows * 2 THEN 'Poor clustering factor'
                        WHEN i.leaf_blocks > 0 AND i.distinct_keys > 0 AND
                             (i.leaf_blocks / i.distinct_keys) > 10 THEN 'High leaf blocks per key'
                        ELSE NULL
                    END as rebuild_reason,
                    CASE
                        WHEN i.last_analyzed IS NULL THEN 1
                        WHEN i.last_analyzed < SYSDATE - 7 THEN 1
                        ELSE 0
                    END as stale_statistics
                FROM dba_indexes i
                WHERE i.owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')
                    AND i.index_type != 'LOB'
                ORDER BY i.owner, i.index_name".to_string())
            ),
            ("index_partitions".to_string(),
             crate::metadata::stc::utils::query("SELECT
                    pi.index_owner as owner,
                    pi.index_name,
                    COUNT(*) as partition_count,
                    pi.partitioning_type,
                    LISTAGG(pi.partition_name, ', ') WITHIN GROUP (ORDER BY pi.partition_position) as partition_names
                FROM dba_part_indexes pi
                WHERE pi.index_owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS')
                GROUP BY pi.index_owner, pi.index_name, pi.partitioning_type
                ORDER BY pi.index_owner, pi.index_name".to_string())
            )
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Oracle index information and statistics", "indexes", SyncFrequency::Medium);
}
