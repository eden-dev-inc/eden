use super::*;

impl OracleLockInfo {
    pub fn contention_status(&self) -> &str {
        match self.contention_severity {
            ContentionSeverity::None => "No Contention",
            ContentionSeverity::Low => "Low Contention",
            ContentionSeverity::Medium => "Medium Contention",
            ContentionSeverity::High => "High Contention",
            ContentionSeverity::Critical => "Critical Contention",
        }
    }

    pub fn requires_immediate_attention(&self) -> bool {
        matches!(self.contention_severity, ContentionSeverity::High | ContentionSeverity::Critical)
            || self.blocked_session_percentage > 20.0
            || self.avg_lock_wait_time > 60.0
            || !self.blocking_chains.is_empty()
    }

    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.blocked_session_percentage > 20.0 {
            recommendations.push("High percentage of blocked sessions detected - investigate blocking chains".to_string());
        }

        if self.avg_lock_wait_time > 30.0 {
            recommendations.push("High average lock wait time - consider optimizing SQL statements".to_string());
        }

        if !self.blocking_chains.is_empty() {
            recommendations.push(format!(
                "Active blocking chains detected ({}) - review and potentially kill blocking sessions",
                self.blocking_chains.len()
            ));
        }

        if !self.contended_objects.is_empty() {
            let top_contended = &self.contended_objects[0];
            recommendations.push(format!(
                "High contention on object {}.{} - consider partitioning or redesign",
                top_contended.owner, top_contended.object_name
            ));
        }

        if self.total_deadlocks > 0 {
            recommendations.push("Deadlocks detected - review application logic and transaction ordering".to_string());
        }

        if self.lock_efficiency_ratio < 80.0 {
            recommendations.push("Low lock efficiency - consider reducing transaction times".to_string());
        }

        if self.performance_impact_score > 50.0 {
            recommendations.push("High performance impact from locks - immediate investigation recommended".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Lock contention is within normal parameters".to_string());
        }

        recommendations
    }

    pub fn get_top_blocking_sessions(&self) -> Vec<&OracleBlockingChain> {
        let mut chains = self.blocking_chains.iter().collect::<Vec<_>>();
        chains.sort_by(|a, b| b.seconds_in_wait.cmp(&a.seconds_in_wait));
        chains.into_iter().take(5).collect()
    }

    pub fn get_most_contended_objects(&self) -> Vec<&OracleContentionHotspot> {
        let mut objects = self.contended_objects.iter().collect::<Vec<_>>();
        objects.sort_by(|a, b| b.contention_score.partial_cmp(&a.contention_score).unwrap_or(std::cmp::Ordering::Equal));
        objects.into_iter().take(5).collect()
    }

    pub fn lock_mode_description(mode: u32) -> &'static str {
        match mode {
            1 => "Null (1)",
            2 => "Row Share (2)",
            3 => "Row Exclusive (3)",
            4 => "Share (4)",
            5 => "Share Row Exclusive (5)",
            6 => "Exclusive (6)",
            _ => "Unknown",
        }
    }

    pub fn format_wait_time(seconds: u64) -> String {
        if seconds < 60 {
            format!("{seconds}s")
        } else if seconds < 3600 {
            format!("{}m {}s", seconds / 60, seconds % 60)
        } else {
            format!("{}h {}m {}s", seconds / 3600, (seconds % 3600) / 60, seconds % 60)
        }
    }

    pub fn lock_type_description(lock_type: &str) -> &'static str {
        match lock_type {
            "TX" => "Transaction Lock",
            "TM" => "Table Lock",
            "UL" => "User Lock",
            "DX" => "Distributed Transaction Lock",
            "CF" => "Control File Lock",
            "IS" => "Instance State Lock",
            "FS" => "File Set Lock",
            "IR" => "Instance Recovery Lock",
            "RT" => "Redo Thread Lock",
            "TS" => "Temp Segment Lock",
            "TD" => "DDL Lock",
            "TC" => "Thread Checkpoint Lock",
            "TT" => "Temp Table Lock",
            "MR" => "Media Recovery Lock",
            "JQ" => "Job Queue Lock",
            "WL" => "Being Written Redo Log Lock",
            "PF" => "Password File Lock",
            "PI" => "Parallel Slaves Lock",
            "PR" => "Process Startup Lock",
            "PS" => "Parallel Slaves Synchronization Lock",
            "RE" => "USE_ROW_ENQUEUE Lock",
            "RW" => "Row Wait Lock",
            "SQ" => "Sequence Number Lock",
            "ST" => "Space Transaction Lock",
            "SV" => "Sequence Number Value Lock",
            "TA" => "Generic Enqueue Lock",
            "TL" => "Redo Log Lock",
            "TO" => "Redo Log Group Lock",
            "TP" => "Redo Log File Lock",
            "TW" => "Redo Log Buffer Lock",
            "UN" => "User Name Lock",
            "US" => "Undo Segment DDL Lock",
            "WS" => "Write Info Lock",
            _ => "Other Lock",
        }
    }

    pub fn is_lock_escalation_occurring(&self) -> bool {
        if self.row_level_locks > 0 {
            let table_to_row_ratio = self.table_level_locks as f64 / self.row_level_locks as f64;
            table_to_row_ratio > 0.1
        } else {
            false
        }
    }

    pub fn get_lock_distribution(&self) -> HashMap<String, u64> {
        HashMap::from([
            ("Row Level (TX)".to_string(), self.row_level_locks),
            ("Table Level (TM)".to_string(), self.table_level_locks),
            ("DDL".to_string(), self.ddl_locks),
            ("System".to_string(), self.system_locks),
            ("Library Cache".to_string(), self.library_cache_locks),
            ("Dictionary Cache".to_string(), self.dictionary_cache_locks),
            ("Other".to_string(), self.other_locks),
        ])
    }

    pub fn get_lock_mode_distribution(&self) -> HashMap<String, u64> {
        HashMap::from([
            ("Null (1)".to_string(), self.null_locks),
            ("Row Share (2)".to_string(), self.row_share_locks),
            ("Row Exclusive (3)".to_string(), self.row_exclusive_locks),
            ("Share (4)".to_string(), self.share_locks),
            ("Share Row Exclusive (5)".to_string(), self.share_row_exclusive_locks),
            ("Exclusive (6)".to_string(), self.exclusive_locks),
        ])
    }

    pub fn lock_contention_ratio(&self) -> f64 {
        if self.total_active_locks > 0 {
            ratio_percentage(self.waiting_sessions, self.total_active_locks)
        } else {
            0.0
        }
    }

    pub fn get_longest_waiting_sessions(&self) -> Vec<&OracleSessionLockInfo> {
        let mut sessions = self.high_wait_sessions.iter().collect::<Vec<_>>();
        sessions.sort_by(|a, b| b.seconds_in_wait.cmp(&a.seconds_in_wait));
        sessions.into_iter().take(10).collect()
    }

    pub fn has_critical_blocking_chains(&self) -> bool {
        self.blocking_chains.iter().any(|chain| chain.seconds_in_wait > 300)
    }

    pub fn average_blocking_chain_length(&self) -> f64 {
        if self.blocking_chains.is_empty() {
            0.0
        } else {
            self.blocking_chains.len() as f64
        }
    }
}
