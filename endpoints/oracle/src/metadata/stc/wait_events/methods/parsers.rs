use super::*;

impl OracleWaitEventInfo {
    pub(crate) fn parse_wait_event_details(rows: Vec<Row>) -> ResultEP<Vec<OracleWaitEventDetails>> {
        map_rows(rows, |row| {
            let sessions_waiting = 0u64;
            Ok(OracleWaitEventDetails {
                event_name: row.get_string("event")?,
                wait_class: row.get_string("wait_class")?,
                total_waits: row.get_u64("total_waits")?,
                time_waited_us: row.get_u64("time_waited_us")?,
                avg_wait_us: row.get_f64("avg_wait_us")?,
                max_wait_us: row.get_u64("max_wait_us")?,
                time_waited_percent: row.get_f64("time_waited_percent")?,
                waits_per_sec: row.get_f64("waits_per_sec")?,
                avg_wait_ms: row.get_f64("avg_wait_ms")?,
                sessions_waiting,
                rank_by_time: row.get_u64("rank_by_time")?,
                rank_by_waits: row.get_u64("rank_by_waits")?,
                issue_severity: row.get_string("issue_severity")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_wait_class_details(rows: Vec<Row>) -> ResultEP<Vec<OracleWaitClassDetails>> {
        map_rows(rows, |row| {
            Ok(OracleWaitClassDetails {
                wait_class: row.get_string("wait_class")?,
                total_waits: row.get_u64("total_waits")?,
                time_waited_us: row.get_u64("time_waited_us")?,
                avg_wait_us: row.get_f64("avg_wait_us")?,
                time_waited_percent: row.get_f64("time_waited_percent")?,
                event_count: row.get_u64("event_count")?,
                sessions_waiting: row.get_u64("sessions_waiting")?,
                description: row.get_string("description")?,
            })
        })
    }

    pub(crate) fn parse_session_wait_details(rows: Vec<Row>) -> ResultEP<Vec<OracleSessionWaitDetails>> {
        map_rows(rows, |row| {
            Ok(OracleSessionWaitDetails {
                sid: row.get_u64("sid")?,
                serial: row.get_u64("serial#")?,
                username: row.get_string("username")?,
                program: row.get_string("program")?,
                machine: row.get_string("machine")?,
                wait_event: row.get_string("wait_event")?,
                wait_class: row.get_string("wait_class")?,
                wait_time_seconds: row.get_u64("wait_time")?,
                seconds_in_wait: row.get_u64("seconds_in_wait")?,
                state: row.get_string("state")?,
                p1: row.get_u64("p1")?,
                p2: row.get_u64("p2")?,
                p3: row.get_u64("p3")?,
                sql_id: row.get_opt_string("sql_id")?,
                blocking_session: row.get_opt_u64("blocking_session")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_wait_trend_details(rows: Vec<Row>) -> ResultEP<Vec<OracleWaitTrendDetails>> {
        map_rows(rows, |row| {
            Ok(OracleWaitTrendDetails {
                snapshot_time: row.get_string("snapshot_time")?,
                event_name: row.get_string("event_name")?,
                waits: row.get_u64("waits")?,
                time_waited_us: row.get_u64("time_waited_us")?,
                avg_wait_us: row.get_f64("avg_wait_us")?,
                waits_per_sec: row.get_f64("waits_per_sec")?,
                trend: row.get_string("trend")?,
            })
        })
    }

    #[allow(dead_code)]
    pub(crate) fn parse_io_wait_details(rows: Vec<Row>) -> ResultEP<Vec<OracleIOWaitDetails>> {
        map_rows(rows, |row| {
            Ok(OracleIOWaitDetails {
                io_type: row.get_string("io_type")?,
                event_name: row.get_string("event_name")?,
                total_waits: row.get_u64("total_waits")?,
                time_waited_us: row.get_u64("time_waited_us")?,
                avg_wait_us: row.get_f64("avg_wait_us")?,
                avg_io_size_bytes: row.get_u64("avg_io_size_bytes")?,
                io_requests_per_sec: row.get_f64("io_requests_per_sec")?,
                throughput_mb_per_sec: row.get_f64("throughput_mb_per_sec")?,
                io_time_percent: row.get_f64("io_time_percent")?,
            })
        })
    }
}
