use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn rate_sql_performance(avg_elapsed_time: f64, buffer_gets: u64, executions: u64) -> SqlPerformanceRating {
        let avg_elapsed_ms = avg_elapsed_time / 1000.0;
        let buffer_gets_per_exec = if executions > 0 {
            buffer_gets as f64 / executions as f64
        } else {
            0.0
        };

        match (avg_elapsed_ms, buffer_gets_per_exec) {
            (ms, _) if ms > 10000.0 => SqlPerformanceRating::Critical,
            (ms, bg) if ms > 1000.0 || bg > 100000.0 => SqlPerformanceRating::Poor,
            (ms, bg) if ms > 100.0 || bg > 10000.0 => SqlPerformanceRating::Fair,
            (ms, bg) if ms > 10.0 || bg > 1000.0 => SqlPerformanceRating::Good,
            _ => SqlPerformanceRating::Excellent,
        }
    }

    pub(crate) fn classify_wait_event_severity(event_name: &str, pct_db_time: f64) -> WaitEventSeverity {
        match event_name {
            name if name.contains("CPU") => {
                if pct_db_time > 50.0 {
                    WaitEventSeverity::Critical
                } else if pct_db_time > 25.0 {
                    WaitEventSeverity::High
                } else {
                    WaitEventSeverity::Medium
                }
            }
            _ => {
                if pct_db_time > 15.0 {
                    WaitEventSeverity::High
                } else if pct_db_time > 5.0 {
                    WaitEventSeverity::Medium
                } else {
                    WaitEventSeverity::Low
                }
            }
        }
    }

    pub(crate) fn classify_wait_event_category(wait_class: &str) -> WaitEventCategory {
        match wait_class {
            "Administrative" => WaitEventCategory::Administrative,
            "Application" => WaitEventCategory::Application,
            "Cluster" => WaitEventCategory::Cluster,
            "Commit" => WaitEventCategory::Commit,
            "Concurrency" => WaitEventCategory::Concurrency,
            "Configuration" => WaitEventCategory::Configuration,
            "Idle" => WaitEventCategory::Idle,
            "Network" => WaitEventCategory::Network,
            "Scheduler" => WaitEventCategory::Scheduler,
            "System I/O" => WaitEventCategory::SystemIo,
            "User I/O" => WaitEventCategory::UserIo,
            _ => WaitEventCategory::Other,
        }
    }
}
