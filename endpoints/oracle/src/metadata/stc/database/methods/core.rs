use super::*;

impl OracleDatabaseStats {
    pub fn space_utilization_percentage(&self) -> f64 {
        ratio_percentage(self.used_space, self.database_size)
    }

    pub fn pga_utilization_percentage(&self) -> f64 {
        ratio_percentage(self.pga_used, self.pga_aggregate_target)
    }

    pub fn is_buffer_cache_healthy(&self, threshold: f64) -> bool {
        self.buffer_cache_hit_ratio >= threshold
    }

    pub fn is_library_cache_healthy(&self, threshold: f64) -> bool {
        self.library_cache_hit_ratio >= threshold
    }

    pub fn is_soft_parse_ratio_healthy(&self, threshold: f64) -> bool {
        self.soft_parse_ratio >= threshold
    }

    pub fn has_high_cpu_usage(&self, threshold: f64) -> bool {
        self.cpu_usage_percentage > threshold
    }

    pub fn has_io_bottlenecks(&self) -> bool {
        if self.logical_reads_per_sec > 0.0 {
            let physical_to_logical_ratio = self.physical_reads_per_sec / self.logical_reads_per_sec;
            physical_to_logical_ratio > 0.2
        } else {
            false
        }
    }

    pub fn top_wait_event(&self) -> Option<&OracleWaitEventStats> {
        self.top_wait_events
            .iter()
            .max_by(|a, b| a.time_waited.partial_cmp(&b.time_waited).unwrap_or(std::cmp::Ordering::Equal))
    }

    pub fn has_significant_waits(&self, threshold_percentage: f64) -> bool {
        self.top_wait_events.iter().any(|event| event.pct_of_total_time > threshold_percentage)
    }

    pub fn commit_success_rate(&self) -> f64 {
        self.user_commit_percentage
    }

    pub fn is_database_healthy(&self) -> OracleDatabaseHealth {
        let mut issues = Vec::new();

        if !self.is_buffer_cache_healthy(95.0) {
            issues.push(format!("Low buffer cache hit ratio: {:.2}%", self.buffer_cache_hit_ratio));
        }
        if !self.is_library_cache_healthy(95.0) {
            issues.push(format!("Low library cache hit ratio: {:.2}%", self.library_cache_hit_ratio));
        }
        if !self.is_soft_parse_ratio_healthy(90.0) {
            issues.push(format!("Low soft parse ratio: {:.2}%", self.soft_parse_ratio));
        }
        if self.has_high_cpu_usage(80.0) {
            issues.push(format!("High CPU usage: {:.2}%", self.cpu_usage_percentage));
        }
        if self.has_io_bottlenecks() {
            issues.push("Potential I/O bottlenecks detected".to_string());
        }

        let space_util = self.space_utilization_percentage();
        if space_util > 85.0 {
            issues.push(format!("High space utilization: {:.2}%", space_util));
        }

        let pga_util = self.pga_utilization_percentage();
        if pga_util > 80.0 {
            issues.push(format!("High PGA utilization: {:.2}%", pga_util));
        }

        if self.has_significant_waits(10.0)
            && let Some(top_event) = self.top_wait_event()
        {
            issues.push(format!(
                "Significant wait event: {} ({:.2}% of total wait time)",
                top_event.event, top_event.pct_of_total_time
            ));
        }

        OracleDatabaseHealth {
            overall_status: if issues.is_empty() {
                DatabaseHealthStatus::Healthy
            } else if issues.len() <= 2 {
                DatabaseHealthStatus::Warning
            } else {
                DatabaseHealthStatus::Critical
            },
            issues,
            buffer_cache_hit_ratio: self.buffer_cache_hit_ratio,
            library_cache_hit_ratio: self.library_cache_hit_ratio,
            soft_parse_ratio: self.soft_parse_ratio,
            space_utilization_pct: space_util,
            pga_utilization_pct: pga_util,
            cpu_usage_pct: self.cpu_usage_percentage,
            top_wait_event_name: self.top_wait_event().map(|e| e.event.clone()),
            top_wait_event_pct: self.top_wait_event().map(|e| e.pct_of_total_time).unwrap_or(0.0),
        }
    }

    pub fn uptime_human_readable(&self) -> String {
        let total_seconds = self.uptime_seconds as i64;
        let days = total_seconds / 86400;
        let hours = (total_seconds % 86400) / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        if days > 0 {
            format!("{days}d {hours}h {minutes}m {seconds}s")
        } else if hours > 0 {
            format!("{hours}h {minutes}m {seconds}s")
        } else if minutes > 0 {
            format!("{minutes}m {seconds}s")
        } else {
            format!("{seconds}s")
        }
    }

    pub fn format_bytes(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{size:.2} {}", UNITS[unit_index])
        }
    }

    pub fn database_size_human_readable(&self) -> String {
        Self::format_bytes(self.database_size)
    }

    pub fn used_space_human_readable(&self) -> String {
        Self::format_bytes(self.used_space)
    }

    pub fn free_space_human_readable(&self) -> String {
        Self::format_bytes(self.free_space)
    }

    pub fn sga_size_human_readable(&self) -> String {
        Self::format_bytes(self.sga_size)
    }

    pub fn pga_target_human_readable(&self) -> String {
        Self::format_bytes(self.pga_aggregate_target)
    }

    pub fn pga_used_human_readable(&self) -> String {
        Self::format_bytes(self.pga_used)
    }
}
