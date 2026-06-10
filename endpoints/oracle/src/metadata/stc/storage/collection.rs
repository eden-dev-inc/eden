use super::*;
impl MetadataCollection for OracleStorageInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "tablespace_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_tablespaces,
                    COUNT(CASE WHEN status = 'ONLINE' THEN 1 END) as online_tablespaces,
                    COUNT(CASE WHEN status = 'OFFLINE' THEN 1 END) as offline_tablespaces,
                    COUNT(CASE WHEN status = 'READ ONLY' THEN 1 END) as readonly_tablespaces,
                    SUM(ts_size.bytes) as total_allocated_storage,
                    SUM(ts_size.bytes - NVL(ts_free.bytes, 0)) as total_used_storage,
                    SUM(NVL(ts_free.bytes, 0)) as total_free_space,
                    MAX(ts_size.bytes) as largest_tablespace_size,
                    COUNT(CASE WHEN (ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes > 0.85 THEN 1 END) as tablespaces_warning,
                    COUNT(CASE WHEN (ts_size.bytes - NVL(ts_free.bytes, 0)) / ts_size.bytes > 0.95 THEN 1 END) as tablespaces_critical
                FROM dba_tablespaces ts
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) as bytes
                    FROM dba_data_files
                    GROUP BY tablespace_name
                ) ts_size ON ts.tablespace_name = ts_size.tablespace_name
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) as bytes
                    FROM dba_free_space
                    GROUP BY tablespace_name
                ) ts_free ON ts.tablespace_name = ts_free.tablespace_name
                WHERE ts.contents != 'TEMPORARY'"
                        .to_string(),
                ),
            ),
            (
                "datafile_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_data_files,
                    COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_data_files,
                    COUNT(CASE WHEN maxbytes > 0 AND bytes / maxbytes > 0.9 THEN 1 END) as files_near_maxsize,
                    SUM(CASE WHEN autoextensible = 'YES' AND increment_by > 0
                        THEN increment_by * 8192 ELSE 0 END) as potential_growth
                FROM dba_data_files"
                        .to_string(),
                ),
            ),
            (
                "tempfile_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_temp_files,
                    SUM(bytes) as total_temp_space,
                    SUM(CASE WHEN status = 'ONLINE' THEN bytes ELSE 0 END) as available_temp_space
                FROM dba_temp_files"
                        .to_string(),
                ),
            ),
            (
                "extent_analysis".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_extents,
                    AVG(bytes) as avg_extent_size,
                    SUM(CASE WHEN bytes < 65536 THEN bytes ELSE 0 END) as small_extent_waste
                FROM dba_extents
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'DBSNMP', 'SYSMAN', 'WMSYS', 'XDB', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS')"
                        .to_string(),
                ),
            ),
            (
                "undo_analysis".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(df.bytes) as total_undo_space,
                    SUM(df.bytes - NVL(fs.free_bytes, 0)) as used_undo_space
                FROM (
                    SELECT tablespace_name, SUM(bytes) as bytes
                    FROM dba_data_files
                    WHERE tablespace_name IN (
                        SELECT value FROM v$parameter WHERE name = 'undo_tablespace'
                        UNION
                        SELECT tablespace_name FROM dba_tablespaces WHERE contents = 'UNDO'
                    )
                    GROUP BY tablespace_name
                ) df
                LEFT JOIN (
                    SELECT tablespace_name, SUM(bytes) as free_bytes
                    FROM dba_free_space
                    GROUP BY tablespace_name
                ) fs ON df.tablespace_name = fs.tablespace_name"
                    .to_string(),
                ),
            ),
            (
                "temp_usage_analysis".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(tf.bytes) as total_temp_space,
                    SUM(tu.blocks * 8192) as used_temp_space
                FROM dba_temp_files tf
                LEFT JOIN (
                    SELECT tablespace, SUM(blocks) as blocks
                    FROM v$tempseg_usage
                    GROUP BY tablespace
                ) tu ON tf.tablespace_name = tu.tablespace"
                        .to_string(),
                ),
            ),
            (
                "growth_tracking".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as autoextend_events_24h,
                    SUM(CASE WHEN maxbytes > bytes THEN maxbytes - bytes ELSE 0 END) as storage_added_24h
                FROM dba_data_files
                WHERE autoextensible = 'YES'"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle storage and tablespace metrics", "storage", SyncFrequency::Medium);
}
