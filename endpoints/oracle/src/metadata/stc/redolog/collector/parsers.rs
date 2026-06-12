use super::*;

impl OracleRedoLogInfo {
    pub(crate) fn parse_log_groups(rows: Vec<Row>) -> ResultEP<Vec<OracleLogGroup>> {
        map_rows(rows, |row| {
            Ok(OracleLogGroup {
                group_number: row.get_i32("group_number")?,
                thread_number: row.get_i32("thread_number")?,
                sequence_number: row.get_u64("sequence_number")?,
                size_bytes: row.get_u64("size_bytes")?,
                status: row.get_string("status")?,
                archived: row.get_string("archived")?,
                file_path: row.get_string("file_path")?,
                file_type: row.get_opt_string("file_type")?,
                is_recovery_dest_file: row.get_opt_string("is_recovery_dest_file")?,
            })
        })
    }

    pub(crate) fn parse_archive_destinations(rows: Vec<Row>) -> ResultEP<Vec<OracleArchiveDestination>> {
        map_rows(rows, |row| {
            Ok(OracleArchiveDestination {
                dest_id: row.get_i32("dest_id")?,
                dest_name: row.get_opt_string("dest_name")?,
                destination: row.get_opt_string("destination")?,
                status: row.get_string("status")?,
                binding: row.get_opt_string("binding")?,
                target: row.get_opt_string("target")?,
                archiver: row.get_opt_string("archiver")?,
                schedule: row.get_opt_string("schedule")?,
                process: row.get_opt_string("process")?,
                error: row.get_opt_string("error")?,
                fail_sequence: row.get_opt_i32("fail_sequence")?,
                fail_block: row.get_opt_i32("fail_block")?,
                fail_date: row.get_opt_datetime("fail_date")?,
            })
        })
    }

    pub(crate) fn parse_log_switches(rows: Vec<Row>) -> ResultEP<Vec<OracleLogSwitch>> {
        map_rows(rows, |row| {
            Ok(OracleLogSwitch {
                thread_number: row.get_i32("thread#")?,
                sequence_number: row.get_u64("sequence#")?,
                first_change: row.get_u64("first_change#")?,
                next_change: row.get_u64("next_change#")?,
                first_time: row.get_datetime("first_time")?,
                next_time: row.get_opt_datetime("next_time")?,
                changes: row.get_u64("changes")?,
                duration_seconds: row.get_f64("duration_seconds")?,
            })
        })
    }

    pub(crate) fn parse_redo_wait_events(rows: Vec<Row>) -> ResultEP<Vec<OracleRedoWaitEvent>> {
        map_rows(rows, |row| {
            Ok(OracleRedoWaitEvent {
                event: row.get_string("event")?,
                total_waits: row.get_u64("total_waits")?,
                total_timeouts: row.get_u64("total_timeouts")?,
                time_waited: row.get_f64("time_waited")?,
                average_wait: row.get_f64("average_wait")?,
                time_waited_fg: row.get_f64("time_waited_fg")?,
                pct_of_total_time: row.get_f64("pct_of_total_time")?,
            })
        })
    }
}
