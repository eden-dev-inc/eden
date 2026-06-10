use super::*;
impl OraclePerformanceStats {
    /// Get formatted CPU utilization string
    pub fn cpu_utilization_formatted(&self) -> String {
        format!("{:.1}%", self.system_stats.cpu_utilization)
    }

    /// Get formatted memory utilization string
    pub fn memory_utilization_formatted(&self) -> String {
        format!(
            "SGA: {:.1}% | PGA: {:.1}%",
            self.memory_utilization.sga_stats.utilization_pct, self.memory_utilization.pga_stats.utilization_pct
        )
    }

    /// Get top 5 wait events by time
    pub fn top_wait_events(&self) -> Vec<&WaitEventStat> {
        self.wait_events.iter().take(5).collect()
    }

    /// Get critical alerts count
    pub fn critical_alerts_count(&self) -> usize {
        self.alerts.iter().filter(|alert| matches!(alert.severity, AlertSeverity::Critical)).count()
    }

    /// Get performance status based on overall score
    pub fn performance_status(&self) -> &'static str {
        match self.performance_analysis.overall_score {
            score if score >= 90.0 => "Excellent",
            score if score >= 80.0 => "Good",
            score if score >= 70.0 => "Fair",
            score if score >= 60.0 => "Poor",
            _ => "Critical",
        }
    }

    /// Get performance status color for UI
    pub fn performance_status_color(&self) -> &'static str {
        match self.performance_analysis.overall_score {
            score if score >= 90.0 => "#28a745", // Green
            score if score >= 80.0 => "#6f42c1", // Purple
            score if score >= 70.0 => "#ffc107", // Yellow
            score if score >= 60.0 => "#fd7e14", // Orange
            _ => "#dc3545",                      // Red
        }
    }
}

impl SqlStatistic {
    /// Get formatted execution time
    pub fn execution_time_formatted(&self) -> String {
        if self.avg_elapsed_time > 1_000_000.0 {
            format!("{:.2}s", self.avg_elapsed_time / 1_000_000.0)
        } else if self.avg_elapsed_time > 1_000.0 {
            format!("{:.2}ms", self.avg_elapsed_time / 1_000.0)
        } else {
            format!("{:.0}μs", self.avg_elapsed_time)
        }
    }

    /// Get formatted buffer gets
    pub fn buffer_gets_formatted(&self) -> String {
        if self.buffer_gets > 1_000_000 {
            format!("{:.1}M", self.buffer_gets as f64 / 1_000_000.0)
        } else if self.buffer_gets > 1_000 {
            format!("{:.1}K", self.buffer_gets as f64 / 1_000.0)
        } else {
            self.buffer_gets.to_string()
        }
    }

    /// Get performance rating color
    pub fn performance_rating_color(&self) -> &'static str {
        match self.performance_rating {
            SqlPerformanceRating::Excellent => "#28a745",
            SqlPerformanceRating::Good => "#6f42c1",
            SqlPerformanceRating::Fair => "#ffc107",
            SqlPerformanceRating::Poor => "#fd7e14",
            SqlPerformanceRating::Critical => "#dc3545",
        }
    }
}

impl WaitEventStat {
    /// Get formatted wait time
    pub fn wait_time_formatted(&self) -> String {
        if self.average_wait_time > 1000.0 {
            format!("{:.2}s", self.average_wait_time / 100.0)
        } else if self.average_wait_time > 10.0 {
            format!("{:.0}ms", self.average_wait_time * 10.0)
        } else {
            format!("{:.1}ms", self.average_wait_time * 10.0)
        }
    }

    /// Get severity color
    pub fn severity_color(&self) -> &'static str {
        match self.severity {
            WaitEventSeverity::Low => "#28a745",
            WaitEventSeverity::Medium => "#ffc107",
            WaitEventSeverity::High => "#fd7e14",
            WaitEventSeverity::Critical => "#dc3545",
        }
    }
}

impl PerformanceBottleneck {
    /// Get severity color
    pub fn severity_color(&self) -> &'static str {
        match self.severity {
            BottleneckSeverity::Minor => "#6c757d",
            BottleneckSeverity::Moderate => "#ffc107",
            BottleneckSeverity::Major => "#fd7e14",
            BottleneckSeverity::Critical => "#dc3545",
        }
    }
}

impl PerformanceAlert {
    /// Get alert severity color
    pub fn severity_color(&self) -> &'static str {
        match self.severity {
            AlertSeverity::Info => "#17a2b8",
            AlertSeverity::Warning => "#ffc107",
            AlertSeverity::Error => "#fd7e14",
            AlertSeverity::Critical => "#dc3545",
        }
    }

    /// Get formatted duration
    pub fn duration_formatted(&self) -> String {
        if self.duration_seconds > 3600 {
            format!("{:.1}h", self.duration_seconds as f64 / 3600.0)
        } else if self.duration_seconds > 60 {
            format!("{}m", self.duration_seconds / 60)
        } else {
            format!("{}s", self.duration_seconds)
        }
    }
}
