use super::*;

impl OracleTransactionInfo {
    pub(crate) fn parse_transaction_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTransactionDetails>> {
        map_rows(rows, |row| {
            Ok(OracleTransactionDetails {
                sid: row.get_u64("sid")?,
                serial: row.get_u64("serial#")?,
                username: row.get_string("username")?,
                program: row.get_string("program")?,
                machine: row.get_string("machine")?,
                start_time: row.get_string("start_time")?,
                duration_seconds: row.get_u64("duration_seconds")?,
                status: row.get_string("status")?,
                sql_id: row.get_opt_string("sql_id")?,
                sql_text: row.get_opt_string("sql_text")?,
                undo_blocks: row.get_u64("undo_blocks")?,
                undo_records: row.get_u64("undo_records")?,
                transaction_type: row.get_string("transaction_type")?,
                lock_wait: row.get_string("lock_wait")?,
                blocking_session: row.get_opt_u64("blocking_session")?,
                issue_severity: row.get_string("issue_severity")?,
            })
        })
    }

    pub(crate) fn parse_lock_details(rows: Vec<Row>) -> ResultEP<Vec<OracleLockDetails>> {
        map_rows(rows, |row| {
            Ok(OracleLockDetails {
                holding_sid: row.get_u64("holding_sid")?,
                waiting_sid: row.get_u64("waiting_sid")?,
                lock_type: row.get_string("lock_type")?,
                mode_held: row.get_string("mode_held")?,
                mode_requested: row.get_string("mode_requested")?,
                object_name: row.get_string("object_name")?,
                object_type: row.get_string("object_type")?,
                wait_time_seconds: row.get_u64("wait_time_seconds")?,
                blocking_sql_id: row.get_opt_string("blocking_sql_id")?,
                waiting_sql_id: row.get_opt_string("waiting_sql_id")?,
                request_time: row.get_string("request_time")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_session_details(rows: Vec<Row>) -> ResultEP<Vec<OracleSessionDetails>> {
        map_rows(rows, |row| {
            Ok(OracleSessionDetails {
                sid: row.get_u64("sid")?,
                serial: row.get_u64("serial")?,
                username: row.get_string("username")?,
                status: row.get_string("status")?,
                program: row.get_string("program")?,
                machine: row.get_string("machine")?,
                logon_time: row.get_string("logon_time")?,
                last_call_et: row.get_u64("last_call_et")?,
                sql_id: row.get_opt_string("sql_id")?,
                blocking_session: row.get_opt_u64("blocking_session")?,
                wait_class: row.get_opt_string("wait_class")?,
                wait_event: row.get_opt_string("wait_event")?,
                wait_time_seconds: row.get_u64("wait_time_seconds")?,
                session_type: row.get_string("session_type")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_undo_details(rows: Vec<Row>) -> ResultEP<Vec<OracleUndoDetails>> {
        map_rows(rows, |row| {
            Ok(OracleUndoDetails {
                segment_name: row.get_string("segment_name")?,
                segment_id: row.get_u64("segment_id")?,
                status: row.get_string("status")?,
                tablespace_name: row.get_string("tablespace_name")?,
                size_bytes: row.get_u64("size_bytes")?,
                blocks_used: row.get_u64("blocks_used")?,
                blocks_total: row.get_u64("blocks_total")?,
                usage_percent: row.get_f64("usage_percent")?,
                active_transactions: row.get_u64("active_transactions")?,
                optimal_size: row.get_u64("optimal_size")?,
                shrinks: row.get_u64("shrinks")?,
                extends: row.get_u64("extends")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_deadlock_details(rows: Vec<Row>) -> ResultEP<Vec<OracleDeadlockDetails>> {
        map_rows(rows, |row| {
            Ok(OracleDeadlockDetails {
                detection_time: row.get_string("detection_time")?,
                session1_sid: row.get_u64("session1_sid")?,
                session2_sid: row.get_u64("session2_sid")?,
                object_name: row.get_string("object_name")?,
                deadlock_type: row.get_string("deadlock_type")?,
                resolution: row.get_string("resolution")?,
                sql_id1: row.get_opt_string("sql_id1")?,
                sql_id2: row.get_opt_string("sql_id2")?,
            })
        })
    }
}
