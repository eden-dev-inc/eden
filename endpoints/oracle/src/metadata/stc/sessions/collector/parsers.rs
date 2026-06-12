use super::*;

impl OracleSessionInfo {
    pub(crate) fn parse_resource_sessions(rows: Vec<Row>) -> ResultEP<Vec<OracleResourceSession>> {
        map_rows(rows, |row| {
            Ok(OracleResourceSession {
                sid: row.get_i32("sid")?,
                serial_number: row.get_i32("serial#")?,
                username: row.get_string("username")?,
                program: row.get_opt_string("program")?,
                machine: row.get_opt_string("machine")?,
                os_user: row.get_opt_string("osuser")?,
                status: row.get_string("status")?,
                session_duration: row.get_f64("session_duration")?,
                pga_used_mem: row.get_opt_u64("pga_used_mem")?,
                pga_alloc_mem: row.get_opt_u64("pga_alloc_mem")?,
                pga_freeable_mem: row.get_opt_u64("pga_freeable_mem")?,
                temp_space_used: row.get_opt_u64("temp_space_used")?,
                sql_id: row.get_opt_string("sql_id")?,
                event: row.get_opt_string("event")?,
                wait_class: row.get_opt_string("wait_class")?,
                seconds_in_wait: row.get_opt_i32("seconds_in_wait")?,
                blocking_session: row.get_opt_i32("blocking_session")?,
            })
        })
    }

    pub(crate) fn parse_long_sessions(rows: Vec<Row>) -> ResultEP<Vec<OracleLongSession>> {
        map_rows(rows, |row| {
            Ok(OracleLongSession {
                sid: row.get_i32("sid")?,
                serial_number: row.get_i32("serial#")?,
                username: row.get_string("username")?,
                program: row.get_opt_string("program")?,
                machine: row.get_opt_string("machine")?,
                os_user: row.get_opt_string("osuser")?,
                status: row.get_string("status")?,
                logon_time: row.get_datetime("logon_time")?,
                session_duration: row.get_f64("session_duration")?,
                last_call_et: row.get_i32("last_call_et")?,
                sql_id: row.get_opt_string("sql_id")?,
                sql_text: row.get_opt_string("sql_text")?,
                event: row.get_opt_string("event")?,
                wait_class: row.get_opt_string("wait_class")?,
            })
        })
    }

    pub(crate) fn parse_blocked_session_details(rows: Vec<Row>) -> ResultEP<Vec<OracleBlockedSessionDetails>> {
        map_rows(rows, |row| {
            Ok(OracleBlockedSessionDetails {
                blocked_sid: row.get_i32("blocked_sid")?,
                blocked_serial: row.get_i32("blocked_serial")?,
                blocked_username: row.get_string("blocked_username")?,
                blocked_program: row.get_opt_string("blocked_program")?,
                blocking_sid: row.get_i32("blocking_sid")?,
                blocking_serial: row.get_i32("blocking_serial")?,
                blocking_username: row.get_string("blocking_username")?,
                blocking_program: row.get_opt_string("blocking_program")?,
                wait_event: row.get_opt_string("wait_event")?,
                seconds_in_wait: row.get_opt_i32("seconds_in_wait")?,
                blocked_sql_id: row.get_opt_string("blocked_sql_id")?,
                blocking_sql_id: row.get_opt_string("blocking_sql_id")?,
                blocked_sql_text: row.get_opt_string("blocked_sql_text")?,
                blocking_sql_text: row.get_opt_string("blocking_sql_text")?,
            })
        })
    }

    pub(crate) fn parse_failed_logins(rows: Vec<Row>) -> ResultEP<Vec<OracleFailedLogin>> {
        map_rows(rows, |row| {
            Ok(OracleFailedLogin {
                username: row.get_string("username")?,
                terminal: row.get_opt_string("terminal")?,
                timestamp: row.get_datetime("timestamp")?,
                return_code: row.get_i32("returncode")?,
                client_id: row.get_opt_string("client_id")?,
                attempt_count: row.get_u64("attempt_count")?,
            })
        })
    }

    pub(crate) fn parse_user_session_stats(rows: Vec<Row>) -> ResultEP<Vec<OracleUserSessionStats>> {
        map_rows(rows, |row| {
            Ok(OracleUserSessionStats {
                username: row.get_string("username")?,
                session_count: row.get_u64("session_count")?,
                active_count: row.get_u64("active_count")?,
                inactive_count: row.get_u64("inactive_count")?,
                avg_duration: row.get_f64("avg_duration")?,
                max_duration: row.get_f64("max_duration")?,
            })
        })
    }

    pub(crate) fn parse_program_session_stats(rows: Vec<Row>) -> ResultEP<Vec<OracleProgramSessionStats>> {
        map_rows(rows, |row| {
            Ok(OracleProgramSessionStats {
                program: row.get_string("program")?,
                session_count: row.get_u64("session_count")?,
                active_count: row.get_u64("active_count")?,
                unique_users: row.get_u64("unique_users")?,
                unique_machines: row.get_u64("unique_machines")?,
            })
        })
    }
}
