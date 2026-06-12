use super::*;
impl MetadataCollection for OracleTablespaceInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "tablespace_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_tablespaces,
                    COUNT(CASE WHEN contents = 'TEMPORARY' THEN 1 END) as temp_tablespaces,
                    COUNT(CASE WHEN contents = 'UNDO' THEN 1 END) as undo_tablespaces,
                    COUNT(CASE WHEN contents = 'PERMANENT' THEN 1 END) as permanent_tablespaces,
                    SUM(total_bytes) as total_allocated_bytes,
                    SUM(used_bytes) as total_used_bytes,
                    SUM(free_bytes) as total_free_bytes,
                    SUM(max_bytes) as total_max_bytes,
                    AVG(usage_percent) as avg_usage_percent,
                    COUNT(CASE WHEN usage_percent > 80 THEN 1 END) as high_usage_tablespaces,
                    COUNT(CASE WHEN usage_percent > 95 THEN 1 END) as critical_usage_tablespaces,
                    COUNT(CASE WHEN status = 'OFFLINE' THEN 1 END) as offline_tablespaces,
                    COUNT(CASE WHEN status = 'READ ONLY' THEN 1 END) as readonly_tablespaces,
                    MAX(total_bytes) as largest_tablespace_bytes,
                    COUNT(CASE WHEN bigfile = 'YES' THEN 1 END) as bigfile_tablespaces,
                    COUNT(CASE WHEN extent_management = 'LOCAL' THEN 1 END) as locally_managed,
                    COUNT(CASE WHEN extent_management = 'DICTIONARY' THEN 1 END) as dictionary_managed,
                    COUNT(CASE WHEN allocation_type = 'UNIFORM' THEN 1 END) as uniform_extents
                FROM (
                    SELECT
                        ts.tablespace_name,
                        ts.contents,
                        ts.status,
                        ts.logging,
                        ts.force_logging,
                        ts.extent_management,
                        ts.allocation_type,
                        ts.bigfile,
                        NVL(df.total_bytes, 0) as total_bytes,
                        NVL(df.total_bytes - fs.free_bytes, 0) as used_bytes,
                        NVL(fs.free_bytes, 0) as free_bytes,
                        NVL(df.max_bytes, 0) as max_bytes,
                        CASE
                            WHEN NVL(df.total_bytes, 0) > 0 THEN
                                ROUND(((NVL(df.total_bytes, 0) - NVL(fs.free_bytes, 0)) / NVL(df.total_bytes, 1)) * 100, 2)
                            ELSE 0
                        END as usage_percent
                    FROM dba_tablespaces ts
                    LEFT JOIN (
                        SELECT tablespace_name,
                               SUM(bytes) as total_bytes,
                               SUM(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END) as max_bytes
                        FROM dba_data_files
                        GROUP BY tablespace_name
                        UNION ALL
                        SELECT tablespace_name,
                               SUM(bytes) as total_bytes,
                               SUM(CASE WHEN autoextensible = 'YES' THEN maxbytes ELSE bytes END) as max_bytes
                        FROM dba_temp_files
                        GROUP BY tablespace_name
                    ) df ON ts.tablespace_name = df.tablespace_name
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
                "datafile_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_datafiles,
                    COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_datafiles,
                    COUNT(CASE WHEN autoextensible = 'YES' THEN 1 END) as autoextend_enabled,
                    COUNT(CASE WHEN usage_percent > 80 THEN 1 END) as high_usage_datafiles
                FROM (
                    SELECT autoextensible,
                           CASE
                               WHEN bytes > 0 THEN ROUND((bytes / GREATEST(bytes, 1)) * 100, 2)
                               ELSE 0
                           END as usage_percent
                    FROM dba_data_files
                    UNION ALL
                    SELECT autoextensible,
                           CASE
                               WHEN bytes > 0 THEN ROUND((bytes / GREATEST(bytes, 1)) * 100, 2)
                               ELSE 0
                           END as usage_percent
                    FROM dba_temp_files
                )"
                    .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle tablespace and datafile metrics", "tablespaces", SyncFrequency::Medium);
}
