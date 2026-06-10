use super::*;
impl OracleSessionInfo {
    /// Checks if session utilization is high
    pub fn is_session_utilization_high(&self, threshold_pct: f64) -> bool {
        self.session_utilization_pct > threshold_pct
    }

    /// Checks if there are many long running sessions
    pub fn has_many_long_sessions(&self, threshold_duration: f64) -> bool {
        self.longest_session_duration > threshold_duration
    }

    /// Checks if there are blocked sessions
    pub fn has_blocked_sessions(&self) -> bool {
        self.sessions_waiting_for_locks > 0
    }

    /// Checks if there are security concerns
    pub fn has_security_concerns(&self, threshold_failed: u64) -> bool {
        self.failed_logins_last_hour > threshold_failed
    }

    /// Checks if there's high resource usage
    pub fn has_high_resource_usage(&self, _pga_threshold: u64, temp_threshold: u64) -> bool {
        self.high_pga_sessions > 0 || self.sessions_using_temp > temp_threshold
    }

    /// Gets average session duration in hours
    pub fn avg_session_duration_hours(&self) -> f64 {
        self.avg_session_duration / 3600.0
    }

    /// Gets longest session duration in hours
    pub fn longest_session_duration_hours(&self) -> f64 {
        self.longest_session_duration / 3600.0
    }

    /// Gets total PGA used in GB
    pub fn total_pga_used_gb(&self) -> f64 {
        bytes_to_gb(self.total_pga_used)
    }

    /// Gets total temp space used in GB
    pub fn total_temp_space_used_gb(&self) -> f64 {
        bytes_to_gb(self.total_temp_space_used)
    }

    /// Gets active session percentage
    pub fn active_session_percentage(&self) -> f64 {
        ratio_percentage(self.active_user_sessions, self.total_user_sessions)
    }

    /// Gets session turnover rate (connections per hour)
    pub fn session_turnover_rate(&self) -> f64 {
        (self.new_sessions_last_hour + self.disconnected_sessions_last_hour) as f64 / 2.0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleSessionHealthSummary {
        OracleSessionHealthSummary {
            utilization_health: status_by_high_threshold(self.session_utilization_pct, 85.0, 95.0),
            performance_health: status_by_flags(
                self.has_many_long_sessions(14400.0) || self.high_pga_sessions > 2,
                self.has_blocked_sessions() || self.has_high_resource_usage(5, 10),
            ),
            security_health: status_by_count(self.failed_logins_last_hour, 5, 20),
            connection_health: status_by_flags(self.killed_sessions > 0 || self.session_turnover_rate() > 50.0, self.killed_sessions > 10),
            resource_health: status_by_flags(
                self.total_pga_used_gb() > 5.0 || self.total_temp_space_used_gb() > 2.0,
                self.total_pga_used_gb() > 10.0 || self.total_temp_space_used_gb() > 5.0,
            ),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_session_health() {
        let session_info = OracleSessionInfo {
            total_user_sessions: 80,
            active_user_sessions: 60,
            session_utilization_pct: 88.0,
            longest_session_duration: 21_600.0,
            sessions_waiting_for_locks: 3,
            killed_sessions: 2,
            high_pga_sessions: 4,
            failed_logins_last_hour: 8,
            unique_users: 25,
            unique_programs: 8,
            total_pga_used: 6_442_450_944,
            ..OracleSessionInfo::default()
        };

        let health = session_info.health_summary();
        assert!(matches!(health.utilization_health, SessionHealthStatus::Warning));
        assert!(matches!(health.performance_health, SessionHealthStatus::Critical));
        assert!(matches!(health.security_health, SessionHealthStatus::Warning));

        assert!(session_info.total_user_sessions > 0);
    }
}
