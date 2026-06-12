use super::*;

impl OracleRedoLogInfo {
    pub(crate) async fn collect_detailed_metrics_if_needed(
        core_info: &OracleRedoLogInfo,
        context: OracleAsync,
    ) -> ResultEP<Option<OracleRedoDetailedMetrics>> {
        let needs_log_group_details = core_info.pending_archive_count > 0 || core_info.log_buffer_hit_ratio < 95.0;
        let needs_archive_details = core_info.archive_lag_seconds > Self::HIGH_ARCHIVE_LAG_THRESHOLD;
        let needs_switch_history = core_info.switches_last_hour > Self::HIGH_SWITCH_FREQUENCY_THRESHOLD;
        let needs_wait_events = core_info.avg_redo_write_time > 10.0 || core_info.log_buffer_hit_ratio < 90.0;

        if !crate::metadata::stc::utils::should_collect(&[
            needs_log_group_details,
            needs_archive_details,
            needs_switch_history,
            needs_wait_events,
        ]) {
            return Ok(None);
        }

        let mut detailed_metrics = OracleRedoDetailedMetrics {
            log_groups: Vec::new(),
            archive_destinations: None,
            recent_log_switches: None,
            redo_wait_events: None,
        };

        let log_groups_input = crate::metadata::stc::utils::query(
            "SELECT
                lg.group# as group_number,
                lg.thread# as thread_number,
                lg.sequence# as sequence_number,
                lg.bytes as size_bytes,
                lg.status,
                lg.archived,
                lf.member as file_path,
                lf.type as file_type,
                lf.is_recovery_dest_file
            FROM v$log lg
            JOIN v$logfile lf ON lg.group# = lf.group#
            ORDER BY lg.group#, lf.member"
                .to_string(),
        );

        crate::metadata::stc::utils::assign_optional_vec(
            &mut detailed_metrics.log_groups,
            &log_groups_input,
            context.clone(),
            Self::QUERY_TIMEOUT,
            "log_groups",
            Self::parse_log_groups,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_archive_details,
            &mut detailed_metrics.archive_destinations,
            || {
                crate::metadata::stc::utils::query(
                    "SELECT
                    dest_id,
                    dest_name,
                    destination,
                    status,
                    binding,
                    target,
                    archiver,
                    schedule,
                    process,
                    error,
                    fail_sequence,
                    fail_block,
                    fail_date
                FROM v$archive_dest
                WHERE status != 'INACTIVE'
                ORDER BY dest_id"
                        .to_string(),
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "archive_destinations",
            Self::parse_archive_destinations,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_switch_history,
            &mut detailed_metrics.recent_log_switches,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                        thread#,
                        sequence#,
                        first_change#,
                        next_change#,
                        first_time,
                        next_time,
                        (next_change# - first_change#) as changes,
                        ROUND((next_time - first_time) * 86400, 2) as duration_seconds
                    FROM v$log_history
                    WHERE first_time >= SYSDATE - 1/24
                    ORDER BY first_time DESC"
                        .to_string(),
                    Self::MAX_DETAILED_RESULTS,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "log_switches",
            Self::parse_log_switches,
        )
        .await?;

        crate::metadata::stc::utils::assign_optional_if(
            needs_wait_events,
            &mut detailed_metrics.redo_wait_events,
            || {
                crate::metadata::stc::utils::query_with_limit(
                    "SELECT
                    event,
                    total_waits,
                    total_timeouts,
                    time_waited,
                    average_wait,
                    time_waited_fg,
                    ROUND((time_waited / SUM(time_waited) OVER()) * 100, 2) as pct_of_total_time
                FROM v$system_event
                WHERE wait_class = 'Commit'
                   OR event LIKE '%redo%'
                   OR event LIKE '%log%'
                   AND time_waited > 0
                ORDER BY time_waited DESC"
                        .to_string(),
                    20,
                )
            },
            context.clone(),
            Self::QUERY_TIMEOUT,
            "redo_wait_events",
            Self::parse_redo_wait_events,
        )
        .await?;

        Ok(Some(detailed_metrics))
    }
}
