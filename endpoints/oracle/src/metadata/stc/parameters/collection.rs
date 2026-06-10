use super::*;
impl MetadataCollection for OracleParametersCollection {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "parameters".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    p.name,
                    p.value,
                    p.default_value,
                    p.ismodified,
                    p.isadjusted,
                    p.isdeprecated,
                    p.isbasic,
                    p.description,
                    p.type,
                    p.display_value,
                    CASE
                        WHEN p.name LIKE '%memory%' OR p.name LIKE '%sga%' OR p.name LIKE '%pga%'
                             OR p.name LIKE '%pool%' OR p.name LIKE '%cache%' OR p.name LIKE '%buffer%' THEN 'MEMORY'
                        WHEN p.name LIKE '%parallel%' OR p.name LIKE '%optimizer%' OR p.name LIKE '%cpu%'
                             OR p.name LIKE '%sort%' OR p.name LIKE '%hash%' THEN 'PERFORMANCE'
                        WHEN p.name LIKE '%audit%' OR p.name LIKE '%security%' OR p.name LIKE '%password%'
                             OR p.name LIKE '%encrypt%' OR p.name LIKE '%ssl%' THEN 'SECURITY'
                        WHEN p.name LIKE '%log%' OR p.name LIKE '%archive%' OR p.name LIKE '%redo%' THEN 'LOGGING'
                        WHEN p.name LIKE '%undo%' OR p.name LIKE '%rollback%' THEN 'UNDO'
                        WHEN p.name LIKE '%process%' OR p.name LIKE '%session%' OR p.name LIKE '%connect%' THEN 'SESSIONS'
                        WHEN p.name LIKE '%backup%' OR p.name LIKE '%recovery%' OR p.name LIKE '%rman%' THEN 'BACKUP'
                        WHEN p.name LIKE '%network%' OR p.name LIKE '%listener%' OR p.name LIKE '%tcp%' THEN 'NETWORK'
                        ELSE 'OTHER'
                    END as category
                FROM v$parameter p
                WHERE p.name NOT LIKE 'nls_%'
                ORDER BY p.name"
                        .to_string(),
                ),
            ),
            (
                "parameter_details".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    p.name,
                    p.num as ordinal,
                    p.update_comment,
                    CASE WHEN p.value != p.default_value THEN 'TRUE' ELSE 'FALSE' END as is_modified,
                    CASE
                        WHEN UPPER(p.name) IN ('MEMORY_TARGET', 'MEMORY_MAX_TARGET', 'SGA_TARGET', 'SGA_MAX_SIZE',
                                               'PGA_AGGREGATE_TARGET', 'PGA_AGGREGATE_LIMIT') THEN 'HIGH'
                        WHEN UPPER(p.name) LIKE '%PARALLEL%' OR UPPER(p.name) LIKE '%CPU%'
                             OR UPPER(p.name) LIKE '%OPTIMIZER%' THEN 'MEDIUM'
                        WHEN UPPER(p.name) LIKE '%AUDIT%' OR UPPER(p.name) LIKE '%PASSWORD%'
                             OR UPPER(p.name) LIKE '%ENCRYPT%' THEN 'HIGH'
                        ELSE 'LOW'
                    END as performance_impact,
                    CASE
                        WHEN UPPER(p.name) LIKE '%AUDIT%' OR UPPER(p.name) LIKE '%PASSWORD%'
                             OR UPPER(p.name) LIKE '%ENCRYPT%' OR UPPER(p.name) LIKE '%SECURITY%' THEN 'HIGH'
                        WHEN UPPER(p.name) LIKE '%REMOTE%' OR UPPER(p.name) LIKE '%NETWORK%' THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as security_impact
                FROM v$parameter p
                ORDER BY p.name"
                        .to_string(),
                ),
            ),
            (
                "spfile_parameters".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    sp.name,
                    sp.value as spfile_value,
                    sp.display_value as spfile_display_value,
                    sp.isspecified,
                    sp.ordinal,
                    CASE sp.sid
                        WHEN 0 THEN 'SPFILE'
                        ELSE 'MEMORY'
                    END as scope
                FROM v$spparameter sp
                WHERE sp.isspecified = 'TRUE'
                ORDER BY sp.name, sp.sid"
                        .to_string(),
                ),
            ),
            (
                "instance_info".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    i.instance_name,
                    i.host_name,
                    i.version,
                    i.startup_time,
                    i.status,
                    i.database_status,
                    i.instance_role,
                    d.name as database_name,
                    d.database_role,
                    d.log_mode,
                    d.flashback_on,
                    d.force_logging,
                    NVL((SELECT value FROM v$parameter WHERE name = 'cpu_count'), 0) as cpu_count,
                    NVL((SELECT ROUND(value/1024/1024) FROM v$parameter WHERE name = 'memory_target'), 0) as memory_target_mb,
                    NVL((SELECT ROUND(value/1024/1024) FROM v$parameter WHERE name = 'sga_target'), 0) as sga_target_mb,
                    NVL((SELECT ROUND(value/1024/1024) FROM v$parameter WHERE name = 'pga_aggregate_target'), 0) as pga_target_mb
                FROM v$instance i, v$database d"
                        .to_string(),
                ),
            ),
            (
                "memory_parameters".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    'SGA_TARGET' as param_name,
                    sg.value as current_bytes,
                    sg.value/1024/1024 as current_mb
                FROM v$parameter sg WHERE sg.name = 'sga_target'
                UNION ALL
                SELECT
                    'PGA_AGGREGATE_TARGET' as param_name,
                    pg.value as current_bytes,
                    pg.value/1024/1024 as current_mb
                FROM v$parameter pg WHERE pg.name = 'pga_aggregate_target'
                UNION ALL
                SELECT
                    'MEMORY_TARGET' as param_name,
                    mt.value as current_bytes,
                    mt.value/1024/1024 as current_mb
                FROM v$parameter mt WHERE mt.name = 'memory_target'"
                        .to_string(),
                ),
            ),
            (
                "deprecated_parameters".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    p.name,
                    p.value,
                    p.description
                FROM v$parameter p
                WHERE p.isdeprecated = 'TRUE'
                    AND p.value IS NOT NULL
                ORDER BY p.name"
                        .to_string(),
                ),
            ),
            (
                "character_sets".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    'DATABASE_CHARACTER_SET' as param_type,
                    value$ as value
                FROM sys.props$ WHERE name = 'NLS_CHARACTERSET'
                UNION ALL
                SELECT
                    'NATIONAL_CHARACTER_SET' as param_type,
                    value$ as value
                FROM sys.props$ WHERE name = 'NLS_NCHAR_CHARACTERSET'"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Oracle database parameter information and analysis", "parameters", SyncFrequency::Low);
}
