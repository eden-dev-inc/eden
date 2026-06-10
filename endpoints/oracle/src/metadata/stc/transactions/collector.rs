use super::*;
impl OracleTransactionInfo {
    /// Checks if there are long-running transactions
    pub fn has_long_running_transactions(&self) -> bool {
        self.long_running_transactions > 0
    }

    /// Checks if there are blocking sessions
    pub fn has_blocking_sessions(&self) -> bool {
        self.blocking_sessions > 0
    }

    /// Checks if deadlocks have been detected
    pub fn has_deadlocks(&self) -> bool {
        self.deadlocks_detected > 0
    }

    /// Gets commit to rollback ratio
    pub fn commit_rollback_ratio(&self) -> f64 {
        if self.user_rollbacks > 0 {
            self.user_commits as f64 / self.user_rollbacks as f64
        } else if self.user_commits > 0 {
            f64::INFINITY
        } else {
            0.0
        }
    }

    /// Gets rollback percentage
    pub fn rollback_percentage(&self) -> f64 {
        let total = self.user_commits + self.user_rollbacks;
        ratio_percentage(self.user_rollbacks, total)
    }

    /// Gets average transaction duration in minutes
    pub fn avg_transaction_duration_minutes(&self) -> f64 {
        self.avg_transaction_duration / 60.0
    }

    /// Gets maximum transaction duration in hours
    pub fn max_transaction_duration_hours(&self) -> f64 {
        self.max_transaction_duration / 3600.0
    }

    /// Gets undo segment utilization
    pub fn undo_segment_utilization(&self) -> f64 {
        ratio_percentage(self.active_undo_segments, self.undo_segments)
    }

    /// Gets blocking ratio
    pub fn blocking_ratio(&self) -> f64 {
        ratio_percentage(self.sessions_waiting_locks, self.active_sessions)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleTransactionHealthSummary {
        OracleTransactionHealthSummary {
            transaction_health: status_by_flags(self.long_running_transactions > 5, self.very_long_transactions > 0),
            locking_health: status_by_flags(self.blocking_sessions > 0, self.blocking_sessions > 10 || self.deadlocks_detected > 0),
            rollback_health: status_by_high_threshold(self.rollback_percentage(), 10.0, 20.0),
            undo_health: status_by_high_threshold(self.undo_segment_utilization(), 80.0, 90.0),
        }
    }
}
