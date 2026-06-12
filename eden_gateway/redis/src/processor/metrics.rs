//! Redis processor metric calculations.

use super::*;

pub(crate) struct RedisPipelineMetrics;

impl RedisPipelineMetrics {
    #[inline]
    pub(crate) fn request_bytes(bytes_read: u64, cmd_count: u64) -> u32 {
        let per_command = bytes_read / cmd_count.max(1);
        per_command.min(u64::from(u32::MAX)) as u32
    }

    #[inline]
    pub(crate) fn response_bytes(total_bytes_written: u64, cmd_count: u64) -> u32 {
        let per_command = total_bytes_written / cmd_count.max(1);
        per_command.min(u64::from(u32::MAX)) as u32
    }

    #[inline]
    pub(crate) fn per_command_latency_us(duration_us: u64, cmd_count: u64) -> u64 {
        duration_us / cmd_count
    }

    #[inline]
    pub(crate) fn marks_slow(duration_us: u64, cmd_count: u64, slow_threshold_us: u64) -> bool {
        slow_threshold_us > 0 && cmd_count <= 1 && duration_us >= slow_threshold_us
    }

    #[inline]
    pub(crate) fn queue_conflict_timeout_duration(max_timeout_ms: u64) -> Duration {
        Duration::from_millis(if max_timeout_ms == 0 { DEFAULT_MAX_TIMEOUT } else { max_timeout_ms })
    }
}
