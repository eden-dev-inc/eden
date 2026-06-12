//! Per-connection read-your-writes session affinity (DW-5).
//!
//! When a dual-write succeeds on the authoritative side but fails on the
//! secondary, reads for the affected table should be routed to the
//! authoritative side until the replay catches up. This module tracks
//! which tables have recent secondary failures and provides a routing
//! override check.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default duration to pin reads to the authoritative side after a
/// secondary write failure.
const DEFAULT_AFFINITY_WINDOW: Duration = Duration::from_secs(5);

/// Per-connection tracker for tables with recent secondary write failures.
///
/// SA-1: Also integrates with the replay queue's depth counter. When the
/// replay queue has pending entries, reads are always overridden to the
/// authoritative side — this naturally clears when all replays complete,
/// instead of relying solely on the fixed time window.
pub struct SessionAffinityTracker {
    /// Maps table name → most recent secondary failure timestamp.
    failed_tables: HashMap<String, Instant>,
    /// How long to keep read affinity after a failure.
    affinity_window: Duration,
    /// SA-1: Shared counter from the replay queue. When > 0, override all reads.
    replay_queue_depth: Option<Arc<AtomicU64>>,
}

impl Default for SessionAffinityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAffinityTracker {
    /// Create a new tracker with the default affinity window.
    pub fn new() -> Self {
        Self {
            failed_tables: HashMap::new(),
            affinity_window: DEFAULT_AFFINITY_WINDOW,
            replay_queue_depth: None,
        }
    }

    /// SA-1: Wire in the replay queue's depth counter.
    ///
    /// When set, `should_override_read` returns `true` whenever the queue
    /// has pending entries — providing a natural, completion-based override
    /// instead of relying only on the fixed time window.
    pub fn set_replay_queue_depth(&mut self, depth: Arc<AtomicU64>) {
        self.replay_queue_depth = Some(depth);
    }

    /// Record that a secondary write failed for the given table.
    pub fn record_failure(&mut self, table: &str) {
        self.failed_tables.insert(table.to_string(), Instant::now());
    }

    /// Check whether reads should be overridden to the authoritative side.
    ///
    /// Returns `true` if:
    /// - SA-1: The replay queue has pending entries (queue depth > 0), OR
    /// - The given table (or any table, if `None`) has a recent failure
    ///   within the affinity window.
    pub fn should_override_read(&mut self, table: Option<&str>) -> bool {
        // SA-1: If the replay queue has pending entries, always override.
        if let Some(ref depth) = self.replay_queue_depth
            && depth.load(Ordering::Relaxed) > 0
        {
            return true;
        }

        // Garbage-collect expired entries first.
        self.gc_expired();

        match table {
            Some(t) => self.failed_tables.contains_key(t),
            None => !self.failed_tables.is_empty(),
        }
    }

    /// Remove entries older than the affinity window.
    fn gc_expired(&mut self) {
        let cutoff = Instant::now() - self.affinity_window;
        self.failed_tables.retain(|_, ts| *ts > cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_no_failures_no_override() {
        let mut tracker = SessionAffinityTracker::new();
        assert!(!tracker.should_override_read(Some("users")));
        assert!(!tracker.should_override_read(None));
    }

    #[test]
    fn test_failure_triggers_override() {
        let mut tracker = SessionAffinityTracker::new();
        tracker.record_failure("users");
        assert!(tracker.should_override_read(Some("users")));
        assert!(!tracker.should_override_read(Some("orders")));
        // None means "any table"
        assert!(tracker.should_override_read(None));
    }

    #[test]
    fn test_expired_failure_cleared() {
        let mut tracker = SessionAffinityTracker {
            failed_tables: HashMap::new(),
            affinity_window: Duration::from_millis(10),
            replay_queue_depth: None,
        };
        tracker.record_failure("users");
        thread::sleep(Duration::from_millis(20));
        assert!(!tracker.should_override_read(Some("users")));
    }

    #[test]
    fn test_queue_depth_overrides_read() {
        let depth = Arc::new(AtomicU64::new(0));
        let mut tracker = SessionAffinityTracker::new();
        tracker.set_replay_queue_depth(Arc::clone(&depth));

        // No pending entries — no override.
        assert!(!tracker.should_override_read(Some("users")));

        // Pending entries — override even for unknown tables.
        depth.store(5, Ordering::Relaxed);
        assert!(tracker.should_override_read(Some("users")));
        assert!(tracker.should_override_read(None));

        // Queue drained — back to normal.
        depth.store(0, Ordering::Relaxed);
        assert!(!tracker.should_override_read(Some("users")));
    }
}
