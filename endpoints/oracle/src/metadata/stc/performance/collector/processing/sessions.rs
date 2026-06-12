use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn process_session_statistics(session_rows: &[Row], blocking_rows: &[Row]) -> ResultEP<SessionStatistics> {
        let mut session_stats = SessionStatistics::default();
        let mut sessions_by_status = HashMap::new();
        let mut sessions_by_wait_class = HashMap::new();
        let mut long_running_sessions = Vec::new();

        for row in session_rows {
            let sid = row.get_u32("sid")?;
            let serial = row.get_u32("serial#")?;
            let username = row.get_string("username")?;
            let program = row.get_string("program")?;
            let status = row.get_string("status")?;
            let logon_time = row.get_datetime("logon_time")?;
            let sql_id = row.get_opt_string("sql_id")?;
            let wait_class = row.get_opt_string("wait_class")?;
            let event = row.get_opt_string("event")?;
            let _seconds_in_wait = row.get_u64("seconds_in_wait")?;
            let cpu_time = row.get_u64("cpu_time")?;
            let is_active = row.get_u32("is_active")? > 0;
            let is_blocked = row.get_u32("is_blocked")? > 0;

            *sessions_by_status.entry(status.clone()).or_insert(0) += 1;

            if let Some(ref wc) = wait_class {
                *sessions_by_wait_class.entry(wc.clone()).or_insert(0) += 1;
            }

            session_stats.total_sessions += 1;
            if is_active {
                session_stats.active_sessions += 1;
            } else {
                session_stats.inactive_sessions += 1;
            }
            if is_blocked {
                session_stats.blocked_sessions += 1;
            }
            if wait_class.is_some() && wait_class.as_ref().map(|wc| wc != "Idle").unwrap_or(false) {
                session_stats.waiting_sessions += 1;
            }

            let runtime_seconds = Utc::now().signed_duration_since(logon_time.as_datetime()).num_seconds() as u64;
            if runtime_seconds > 3600 && is_active {
                let long_session = LongRunningSession {
                    sid,
                    serial,
                    username,
                    program,
                    sql_id,
                    status,
                    logon_time,
                    runtime_seconds,
                    cpu_time,
                    wait_class,
                    wait_event: event,
                };
                long_running_sessions.push(long_session);
            }
        }

        session_stats.sessions_by_status = sessions_by_status;
        session_stats.sessions_by_wait_class = sessions_by_wait_class;
        session_stats.long_running_sessions = long_running_sessions;

        for row in blocking_rows {
            let blocking_sid = row.get_u32("blocking_sid")?;
            let blocked_sid = row.get_u32("blocked_sid")?;
            let blocking_username = row.get_string("blocking_username")?;
            let blocked_username = row.get_string("blocked_username")?;
            let lock_type = row.get_string("lock_type")?;
            let lock_mode = row.get_string("lock_mode")?;
            let object_name = row.get_opt_string("object_name")?;
            let block_time_seconds = row.get_u64("block_time_seconds")?;
            let blocking_sql_id = row.get_opt_string("blocking_sql_id")?;
            let blocked_sql_id = row.get_opt_string("blocked_sql_id")?;

            let blocking_session = BlockingSession {
                blocking_sid,
                blocked_sid,
                blocking_username,
                blocked_username,
                lock_type,
                lock_mode,
                object_name,
                block_time_seconds,
                blocking_sql_id,
                blocked_sql_id,
            };

            session_stats.blocking_sessions.push(blocking_session);
        }

        Ok(session_stats)
    }
}
