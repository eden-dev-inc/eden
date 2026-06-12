use super::*;
impl MetadataCollection for OracleWaitEventInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "wait_event_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as total_wait_events,
                    SUM(time_waited_micro) as total_time_waited_us,
                    SUM(total_waits) as total_waits,
                    AVG(average_wait) as avg_wait_time_us,
                    MAX(time_waited_micro) as max_wait_time_us
                FROM v$system_event
                WHERE wait_class != 'Idle'"
                        .to_string(),
                ),
            ),
            (
                "wait_class_summary".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    wait_class,
                    SUM(total_waits) as total_waits,
                    SUM(time_waited_micro) as time_waited_us,
                    AVG(average_wait) as avg_wait_us
                FROM v$system_event
                WHERE wait_class != 'Idle'
                GROUP BY wait_class
                ORDER BY time_waited_us DESC"
                        .to_string(),
                ),
            ),
            (
                "session_waits".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as sessions_waiting
                FROM v$session_wait
                WHERE wait_class != 'Idle'
                   AND state = 'WAITING'"
                        .to_string(),
                ),
            ),
            (
                "db_time_stats".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    SUM(CASE WHEN stat_name = 'DB time' THEN value ELSE 0 END) as db_time_us,
                    SUM(CASE WHEN stat_name = 'CPU used by this session' THEN value ELSE 0 END) as cpu_time_us
                FROM v$sys_time_model
                WHERE stat_name IN ('DB time', 'CPU used by this session')"
                        .to_string(),
                ),
            ),
            (
                "background_waits".to_string(),
                crate::metadata::stc::utils::query(
                    "SELECT
                    COUNT(*) as background_wait_events
                FROM v$system_event
                WHERE wait_class = 'System I/O'
                   OR event LIKE '%background%'
                   OR event LIKE '%LGWR%'
                   OR event LIKE '%DBWR%'"
                        .to_string(),
                ),
            ),
        ])
    }
    crate::impl_metadata_collection_boilerplate!("Return Oracle wait event and timing metrics", "waits", SyncFrequency::High);
}
