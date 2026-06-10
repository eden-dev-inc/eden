use super::*;
impl MetadataCollection for OracleRedoLogInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "core_redo_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    l.sequence# as current_sequence,
                    COUNT(DISTINCT lg.group#) as total_log_groups,
                    COUNT(DISTINCT CASE WHEN lg.status = 'CURRENT' THEN lg.group# END) as active_log_groups,
                    COUNT(DISTINCT CASE WHEN lg.status = 'INACTIVE' THEN lg.group# END) as inactive_log_groups,
                    MAX(CASE WHEN lg.status = 'CURRENT' THEN lg.group# END) as current_log_group,
                    MAX(lg.bytes) as log_file_size,
                    COALESCE(rs.value, 0) as redo_size_today
                FROM v$log lg
                JOIN v$logfile lf ON lg.group# = lf.group#
                LEFT JOIN v$log l ON lg.status = 'CURRENT'
                LEFT JOIN v$sysstat rs ON rs.name = 'redo size'
                GROUP BY l.sequence#, rs.value"
                        .to_string(),
                ),
            ),
            (
                "redo_performance".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    (SELECT value FROM v$sysstat WHERE name = 'redo writes') /
                    GREATEST((SELECT value FROM v$sysstat WHERE name = 'redo write time'), 1) * 10 as avg_redo_write_time,
                    CASE
                        WHEN (SELECT value FROM v$sysstat WHERE name = 'redo buffer allocation retries') > 0
                        THEN 100 * (1 - ((SELECT value FROM v$sysstat WHERE name = 'redo buffer allocation retries') /
                                         GREATEST((SELECT value FROM v$sysstat WHERE name = 'redo entries'), 1)))
                        ELSE 100
                    END as log_buffer_hit_ratio,
                    COALESCE((
                        SELECT (SYSDATE - MAX(first_time)) * 86400
                        FROM v$log_history
                        WHERE first_time >= SYSDATE - 1/24
                    ), 0) as time_since_last_switch
                FROM dual"
                        .to_string(),
                ),
            ),
            (
                "log_switch_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as switches_last_hour,
                    CASE
                        WHEN COUNT(*) > 0
                        THEN 24.0 / COUNT(*)
                        ELSE 0
                    END as log_switch_frequency
                FROM v$log_history
                WHERE first_time >= SYSDATE - 1/24"
                        .to_string(),
                ),
            ),
            (
                "scn_info".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    (SELECT current_scn FROM v$database) as current_scn,
                    (SELECT checkpoint_change# FROM v$database) as checkpoint_scn,
                    (SELECT current_scn FROM v$database) - (SELECT checkpoint_change# FROM v$database) as scn_gap
                FROM dual"
                        .to_string(),
                ),
            ),
            (
                "archive_info".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COALESCE((
                        SELECT (SYSDATE - MAX(completion_time)) * 86400
                        FROM v$archived_log
                        WHERE dest_id = 1 AND completion_time IS NOT NULL
                    ), 0) as archive_lag_seconds,
                    (SELECT COUNT(*)
                     FROM v$log
                     WHERE archived = 'NO' AND status != 'CURRENT') as pending_archive_count
                FROM dual"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle redo log activity metrics", "redo_log", SyncFrequency::Medium);
}

use crate::api::lib::query::QueryInput;
