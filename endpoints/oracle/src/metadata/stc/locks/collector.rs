use super::*;
use function_name::named;

impl OracleLockInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut lock_info = OracleLockInfo::default();
        let requests = self.request();
        lock_info.collection_timestamp = DateTimeWrapper::from(Utc::now());

        if let Some(row) = run_single_row(&requests, "lock_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.total_active_locks = row.get_u64("total_active_locks")?;
            lock_info.blocking_locks = row.get_u64("blocking_locks")?;
            lock_info.waiting_sessions = row.get_u64("waiting_sessions")?;
            lock_info.avg_lock_wait_time = row.get_f64("avg_lock_wait_time")?;
            lock_info.max_lock_wait_time = row.get_f64("max_lock_wait_time")?;
            lock_info.total_lock_wait_time = row.get_f64("total_lock_wait_time")?;
        }

        if let Some(row) = run_single_row(&requests, "lock_types", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.row_level_locks = row.get_u64("row_level_locks")?;
            lock_info.table_level_locks = row.get_u64("table_level_locks")?;
            lock_info.ddl_locks = row.get_u64("ddl_locks")?;
            lock_info.system_locks = row.get_u64("system_locks")?;
            lock_info.library_cache_locks = row.get_u64("library_cache_locks")?;
            lock_info.dictionary_cache_locks = row.get_u64("dictionary_cache_locks")?;
            lock_info.other_locks = row.get_u64("other_locks")?;
        }

        if let Some(row) = run_single_row(&requests, "lock_modes", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.null_locks = row.get_u64("null_locks")?;
            lock_info.row_share_locks = row.get_u64("row_share_locks")?;
            lock_info.row_exclusive_locks = row.get_u64("row_exclusive_locks")?;
            lock_info.share_locks = row.get_u64("share_locks")?;
            lock_info.share_row_exclusive_locks = row.get_u64("share_row_exclusive_locks")?;
            lock_info.exclusive_locks = row.get_u64("exclusive_locks")?;
        }

        lock_info.blocking_chains =
            Self::parse_blocking_chains(run_named_query(&requests, "blocking_chains", context.clone(), Self::QUERY_TIMEOUT).await?)?;

        lock_info.lock_conflicts =
            Self::parse_lock_conflicts(run_named_query(&requests, "lock_conflicts", context.clone(), Self::QUERY_TIMEOUT).await?)?;

        if let Some(row) = run_single_row(&requests, "deadlock_info", context.clone(), Self::QUERY_TIMEOUT).await? {
            lock_info.total_deadlocks = row.get_u64("total_deadlocks")?;
        }

        lock_info.contended_objects =
            Self::parse_contended_objects(run_named_query(&requests, "contended_objects", context.clone(), Self::QUERY_TIMEOUT).await?)?;

        lock_info.high_wait_sessions =
            Self::parse_high_wait_sessions(run_named_query(&requests, "high_wait_sessions", context.clone(), Self::QUERY_TIMEOUT).await?)?;

        if let Some(row) = run_single_row(&requests, "session_counts", context.clone(), Self::QUERY_TIMEOUT).await? {
            let total_user_sessions = row.get_u64("total_user_sessions")?;
            lock_info.blocked_sessions = row.get_u64("blocked_sessions")?;

            if total_user_sessions > 0 {
                lock_info.blocked_session_percentage = ratio_percentage(lock_info.blocked_sessions, total_user_sessions);
            }
        }

        lock_info.calculate_derived_metrics();

        Ok(lock_info)
    }

    pub(crate) fn calculate_derived_metrics(&mut self) {
        let total_lock_attempts = self.total_active_locks + self.waiting_sessions;
        if total_lock_attempts > 0 {
            self.lock_efficiency_ratio = ratio_percentage(total_lock_attempts.saturating_sub(self.waiting_sessions), total_lock_attempts);
        }

        self.contention_severity = if self.blocked_sessions == 0 && self.waiting_sessions == 0 {
            ContentionSeverity::None
        } else if self.blocked_session_percentage <= 5.0 && self.avg_lock_wait_time <= 10.0 {
            ContentionSeverity::Low
        } else if self.blocked_session_percentage <= 15.0 && self.avg_lock_wait_time <= 30.0 {
            ContentionSeverity::Medium
        } else if self.blocked_session_percentage <= 30.0 && self.avg_lock_wait_time <= 60.0 {
            ContentionSeverity::High
        } else {
            ContentionSeverity::Critical
        };

        self.performance_impact_score = self.blocked_session_percentage * 2.0;
        if self.avg_lock_wait_time > 0.0 {
            self.performance_impact_score += (self.avg_lock_wait_time / 60.0) * 20.0;
        }

        self.performance_impact_score += (self.blocking_chains.len() as f64) * 5.0;
        self.performance_impact_score += (self.contended_objects.len() as f64) * 2.0;
        self.performance_impact_score = self.performance_impact_score.min(100.0);

        for hotspot in &mut self.contended_objects {
            hotspot.contention_score =
                Self::calculate_contention_score(hotspot.waiting_lock_count, hotspot.avg_wait_seconds, hotspot.unique_sessions);
        }
    }

    fn calculate_contention_score(waiting_locks: u64, avg_wait: f64, unique_sessions: u64) -> f64 {
        let mut score = 0.0;
        score += (waiting_locks as f64) * 10.0;
        score += avg_wait * 2.0;
        score += (unique_sessions as f64) * 5.0;
        score.min(100.0)
    }

    fn parse_chain_session(row: &Row, prefix: &str) -> ResultEP<OracleSessionInfo> {
        let sid = format!("{prefix}_sid");
        let serial = format!("{prefix}_serial");
        let username = format!("{prefix}_username");
        let schema = format!("{prefix}_schema");
        let os_user = format!("{prefix}_osuser");
        let machine = format!("{prefix}_machine");
        let program = format!("{prefix}_program");
        let sql = format!("{prefix}_sql");

        Ok(OracleSessionInfo {
            sid: row.get_u32(&sid)?,
            serial_number: row.get_u32(&serial)?,
            username: row.get_string(&username)?,
            schema_name: row.get_string(&schema)?,
            os_user: row.get_opt_string(&os_user)?,
            machine: row.get_opt_string(&machine)?,
            program: row.get_opt_string(&program)?,
            current_sql: row.get_opt_string(&sql)?,
        })
    }

    fn parse_blocking_chains(rows: Vec<Row>) -> ResultEP<Vec<OracleBlockingChain>> {
        map_rows(rows, |row| {
            Ok(OracleBlockingChain {
                blocked_session: Self::parse_chain_session(&row, "blocked")?,
                blocking_session: Self::parse_chain_session(&row, "blocking")?,
                wait_time_centiseconds: row.get_u64("wait_time_cs")?,
                seconds_in_wait: row.get_u64("seconds_in_wait")?,
                wait_event: row.get_opt_string("wait_event")?,
                object_name: row.get_opt_string("object_name")?,
                object_type: row.get_opt_string("object_type")?,
                lock_type: row.get_opt_string("lock_type")?,
                lock_mode_held: row.get_u32("lock_mode_held")?,
                lock_mode_requested: row.get_u32("lock_mode_requested")?,
            })
        })
    }

    fn parse_lock_conflicts(rows: Vec<Row>) -> ResultEP<Vec<OracleLockConflict>> {
        map_rows(rows, |row| {
            Ok(OracleLockConflict {
                waiting_sid: row.get_u32("waiting_sid")?,
                holding_sid: row.get_u32("holding_sid")?,
                lock_type: row.get_string("lock_type")?,
                lock_id1: row.get_u64("id1")?,
                lock_id2: row.get_u64("id2")?,
                mode_held: row.get_u32("mode_held")?,
                mode_requested: row.get_u32("mode_requested")?,
                blocking_mode: row.get_u32("blocking_mode")?,
                object_owner: row.get_opt_string("object_owner")?,
                object_name: row.get_opt_string("object_name")?,
                object_type: row.get_opt_string("object_type")?,
                seconds_in_wait: row.get_u64("seconds_in_wait")?,
                wait_event: row.get_opt_string("wait_event")?,
            })
        })
    }

    fn parse_contended_objects(rows: Vec<Row>) -> ResultEP<Vec<OracleContentionHotspot>> {
        map_rows(rows, |row| {
            Ok(OracleContentionHotspot {
                owner: row.get_string("owner")?,
                object_name: row.get_string("object_name")?,
                object_type: row.get_string("object_type")?,
                total_lock_count: row.get_u64("lock_count")?,
                waiting_lock_count: row.get_u64("waiting_count")?,
                avg_wait_seconds: row.get_f64("avg_wait_seconds")?,
                max_wait_seconds: row.get_f64("max_wait_seconds")?,
                unique_sessions: row.get_u64("unique_sessions")?,
                contention_score: 0.0,
            })
        })
    }

    fn parse_high_wait_sessions(rows: Vec<Row>) -> ResultEP<Vec<OracleSessionLockInfo>> {
        map_rows(rows, |row| {
            Ok(OracleSessionLockInfo {
                session_info: OracleSessionInfo {
                    sid: row.get_u32("sid")?,
                    serial_number: row.get_u32("serial#")?,
                    username: row.get_string("username")?,
                    schema_name: row.get_string("schemaname")?,
                    os_user: row.get_opt_string("osuser")?,
                    machine: row.get_opt_string("machine")?,
                    program: row.get_opt_string("program")?,
                    current_sql: row.get_opt_string("current_sql")?,
                },
                seconds_in_wait: row.get_u64("seconds_in_wait")?,
                wait_event: row.get_opt_string("wait_event")?,
                p1_text: row.get_opt_string("p1text")?,
                p1: row.get_opt_u64("p1")?,
                p2_text: row.get_opt_string("p2text")?,
                p2: row.get_opt_u64("p2")?,
                blocking_session: row.get_opt_i32("blocking_session")?.map(|value| value.max(0) as u32),
                row_wait_obj: row.get_opt_i32("row_wait_obj#")?.map(|value| value.max(0) as u32),
                row_wait_file: row.get_opt_i32("row_wait_file#")?.map(|value| value.max(0) as u32),
                row_wait_block: row.get_opt_i32("row_wait_block#")?.map(|value| value.max(0) as u32),
                row_wait_row: row.get_opt_i32("row_wait_row#")?.map(|value| value.max(0) as u32),
            })
        })
    }
}
