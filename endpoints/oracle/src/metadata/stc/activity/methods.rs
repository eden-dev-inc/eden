use super::*;
impl OracleActivityInfo {
    /// Calculates the percentage of sessions that are currently active
    pub fn active_session_percentage(&self) -> f64 {
        ratio_percentage(self.active_sessions, self.total_sessions)
    }

    /// Checks if there are long-running SQL statements
    pub fn has_long_running_sql(&self, threshold_seconds: f64) -> bool {
        self.longest_sql_duration > threshold_seconds
    }

    /// Checks if there are long-running transactions
    pub fn has_long_running_transactions(&self, threshold_seconds: f64) -> bool {
        self.longest_transaction_duration > threshold_seconds
    }

    /// Checks if there are blocking sessions
    pub fn has_blocking_sessions(&self) -> bool {
        self.blocking_sessions_count > 0
    }

    /// Checks if session limit is being approached
    pub fn is_approaching_session_limit(&self, threshold_percentage: f64) -> bool {
        self.session_utilization_pct > threshold_percentage
    }

    /// Checks if PGA memory usage is high
    pub fn is_pga_memory_high(&self, threshold_percentage: f64) -> bool {
        self.pga_usage_percentage() > threshold_percentage
    }

    /// Checks if process limit is being approached
    pub fn is_approaching_process_limit(&self, threshold_percentage: f64) -> bool {
        self.process_usage_percentage() > threshold_percentage
    }

    /// Checks if parallel execution servers are heavily utilized
    pub fn is_parallel_servers_busy(&self, threshold_percentage: f64) -> bool {
        self.parallel_server_usage_percentage() > threshold_percentage
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets the current PGA usage percentage
    pub fn pga_usage_percentage(&self) -> f64 {
        ratio_percentage(self.current_pga_used, self.pga_aggregate_limit)
    }

    /// Gets the current process usage percentage
    pub fn process_usage_percentage(&self) -> f64 {
        ratio_percentage(self.process_count, self.process_limit)
    }

    /// Gets the parallel server usage percentage
    pub fn parallel_server_usage_percentage(&self) -> f64 {
        ratio_percentage(self.parallel_servers_active, self.parallel_servers_max)
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleHealthSummary {
        OracleHealthSummary {
            session_health: status_by_high_threshold(self.session_utilization_pct, 80.0, 95.0),
            memory_health: status_by_high_threshold(self.pga_usage_percentage(), 80.0, 95.0),
            process_health: status_by_high_threshold(self.process_usage_percentage(), 80.0, 95.0),
            blocking_health: status_by_count(self.blocking_sessions_count, 0, 10),
            performance_health: status_by_flags(
                self.has_long_running_sql(60.0) || self.waiting_sessions_count > 5,
                self.has_long_running_sql(300.0) || self.waiting_sessions_count > 20,
            ),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_activity_info_calculations_and_health() {
        let activity_info = OracleActivityInfo {
            active_sessions: 50,
            total_sessions: 100,
            max_sessions: 200,
            current_pga_used: 800_000_000,
            pga_aggregate_limit: 1_000_000_000,
            process_count: 80,
            process_limit: 100,
            parallel_servers_active: 4,
            parallel_servers_max: 10,
            blocking_sessions_count: 2,
            longest_sql_duration: 120.0,
            waiting_sessions_count: 3,
            session_utilization_pct: 85.0,
            ..OracleActivityInfo::default()
        };

        assert_eq!(activity_info.active_session_percentage(), 50.0);
        assert_eq!(activity_info.pga_usage_percentage(), 80.0);
        assert_eq!(activity_info.process_usage_percentage(), 80.0);
        assert_eq!(activity_info.parallel_server_usage_percentage(), 40.0);
        assert!(activity_info.is_approaching_session_limit(40.0));
        assert!(activity_info.is_parallel_servers_busy(30.0));
        assert!(!activity_info.is_approaching_process_limit(90.0));

        let health = activity_info.health_summary();
        assert!(matches!(health.session_health, HealthStatus::Warning));
        assert!(matches!(health.performance_health, HealthStatus::Warning));
    }
}
