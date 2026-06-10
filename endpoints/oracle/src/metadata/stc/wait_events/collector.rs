use super::*;
impl OracleWaitEventInfo {
    /// Checks if there are sessions currently waiting
    pub fn has_waiting_sessions(&self) -> bool {
        self.sessions_waiting > 0
    }

    /// Checks if wait time is high relative to CPU time
    pub fn has_high_wait_time(&self, threshold_percent: f64) -> bool {
        self.wait_time_percent > threshold_percent
    }

    /// Gets total database time in seconds
    pub fn db_time_seconds(&self) -> f64 {
        self.db_time_us as f64 / 1_000_000.0
    }

    /// Gets total wait time in seconds
    pub fn total_wait_time_seconds(&self) -> f64 {
        self.total_time_waited_us as f64 / 1_000_000.0
    }

    /// Gets average wait time in milliseconds
    pub fn avg_wait_time_ms(&self) -> f64 {
        self.avg_wait_time_us / 1000.0
    }

    /// Gets maximum wait time in seconds
    pub fn max_wait_time_seconds(&self) -> f64 {
        self.max_wait_time_us as f64 / 1_000_000.0
    }

    /// Gets I/O wait percentage
    pub fn io_wait_percentage(&self) -> f64 {
        ratio_percentage(self.io_wait_time_us, self.total_time_waited_us)
    }

    /// Gets concurrency wait percentage
    pub fn concurrency_wait_percentage(&self) -> f64 {
        ratio_percentage(self.concurrency_wait_time_us, self.total_time_waited_us)
    }

    /// Gets application wait percentage
    pub fn application_wait_percentage(&self) -> f64 {
        ratio_percentage(self.application_wait_time_us, self.total_time_waited_us)
    }

    /// Gets waits per second
    pub fn waits_per_second(&self) -> f64 {
        if self.db_time_seconds() > 0.0 {
            self.total_waits as f64 / self.db_time_seconds()
        } else {
            0.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleWaitEventHealthSummary {
        OracleWaitEventHealthSummary {
            wait_time_health: status_by_high_threshold(self.wait_time_percent, 60.0, 80.0),
            session_health: status_by_count(self.sessions_waiting, 10, 50),
            io_health: status_by_high_threshold(self.io_wait_percentage(), 40.0, 60.0),
            concurrency_health: status_by_high_threshold(self.concurrency_wait_percentage(), 15.0, 30.0),
        }
    }
}
