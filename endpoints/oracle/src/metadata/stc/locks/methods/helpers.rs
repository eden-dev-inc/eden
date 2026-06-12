use super::*;

impl ContentionSeverity {
    pub fn color_code(&self) -> &'static str {
        match self {
            ContentionSeverity::None => "#28a745",
            ContentionSeverity::Low => "#ffc107",
            ContentionSeverity::Medium => "#fd7e14",
            ContentionSeverity::High => "#dc3545",
            ContentionSeverity::Critical => "#6f42c1",
        }
    }

    pub fn severity_score(&self) -> u8 {
        match self {
            ContentionSeverity::None => 0,
            ContentionSeverity::Low => 20,
            ContentionSeverity::Medium => 50,
            ContentionSeverity::High => 80,
            ContentionSeverity::Critical => 100,
        }
    }
}

impl OracleBlockingChain {
    pub fn formatted_wait_time(&self) -> String {
        OracleLockInfo::format_wait_time(self.seconds_in_wait)
    }

    pub fn is_long_running(&self) -> bool {
        self.seconds_in_wait > 60
    }

    pub fn summary(&self) -> String {
        format!(
            "Session {} blocking session {} for {}",
            self.blocking_session.sid,
            self.blocked_session.sid,
            self.formatted_wait_time()
        )
    }
}

impl OracleLockConflict {
    pub fn lock_mode_descriptions(&self) -> (String, String, String) {
        (
            format!("Held: {}", OracleLockInfo::lock_mode_description(self.mode_held)),
            format!("Requested: {}", OracleLockInfo::lock_mode_description(self.mode_requested)),
            format!("Blocking: {}", OracleLockInfo::lock_mode_description(self.blocking_mode)),
        )
    }

    pub fn formatted_wait_time(&self) -> String {
        OracleLockInfo::format_wait_time(self.seconds_in_wait)
    }
}

impl OracleContentionHotspot {
    pub fn contention_level(&self) -> &'static str {
        if self.contention_score >= 80.0 {
            "Critical"
        } else if self.contention_score >= 60.0 {
            "High"
        } else if self.contention_score >= 40.0 {
            "Medium"
        } else if self.contention_score >= 20.0 {
            "Low"
        } else {
            "Minimal"
        }
    }

    pub fn object_identifier(&self) -> String {
        format!("{}.{}", self.owner, self.object_name)
    }

    pub fn waiting_percentage(&self) -> f64 {
        if self.total_lock_count > 0 {
            ratio_percentage(self.waiting_lock_count, self.total_lock_count)
        } else {
            0.0
        }
    }
}

impl OracleSessionLockInfo {
    pub fn formatted_wait_time(&self) -> String {
        OracleLockInfo::format_wait_time(self.seconds_in_wait)
    }

    pub fn is_blocked(&self) -> bool {
        self.blocking_session.is_some()
    }

    pub fn session_identifier(&self) -> String {
        format!("{}:{}", self.session_info.sid, self.session_info.serial_number)
    }

    pub fn wait_details(&self) -> String {
        if let Some(event) = &self.wait_event {
            format!("Event: {event}")
        } else {
            "No wait event".to_string()
        }
    }
}
