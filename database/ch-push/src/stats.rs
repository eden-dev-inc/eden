use std::sync::atomic::AtomicU64;

/// Tracks events that failed to be written after all retries.
#[derive(Debug, Default)]
pub struct DeadLetterStats {
    pub events_dropped: AtomicU64,
    pub anti_patterns_dropped: AtomicU64,
    pub retry_attempts: AtomicU64,
    pub successful_retries: AtomicU64,
    pub flush_failures: AtomicU64,
    pub event_batch_size: AtomicU64,
    pub pattern_batch_size: AtomicU64,
    pub signal_batch_size: AtomicU64,
    pub anti_pattern_batch_size: AtomicU64,
    pub circuit_state: AtomicU64,
}
