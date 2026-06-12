//! Rate limiting and deduplication utilities.

use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Sliding window rate limiter.
///
/// Tracks timestamps of allowed events and enforces a maximum count per window.
pub struct RateLimiter {
    window: Duration,
    max_per_window: usize,
    timestamps: Mutex<VecDeque<Instant>>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    pub fn new(window: Duration, max_per_window: usize) -> Self {
        Self {
            window,
            max_per_window,
            timestamps: Mutex::new(VecDeque::new()),
        }
    }

    /// Check if an event is allowed at the given instant.
    ///
    /// Returns `true` if the event is allowed (under the rate limit),
    /// and records the timestamp. Returns `false` if rate limited.
    pub fn allow_at(&self, now: Instant) -> bool {
        let mut timestamps = self.timestamps.lock();

        // Prune old timestamps outside the window
        while let Some(front) = timestamps.front().copied() {
            if now.duration_since(front) > self.window {
                timestamps.pop_front();
            } else {
                break;
            }
        }

        if timestamps.len() >= self.max_per_window {
            return false;
        }

        timestamps.push_back(now);
        true
    }

    /// Check if an event would be allowed (without recording it).
    pub fn would_allow(&self, now: Instant) -> bool {
        let timestamps = self.timestamps.lock();
        let valid_count = timestamps.iter().filter(|&&ts| now.duration_since(ts) <= self.window).count();
        valid_count < self.max_per_window
    }
}

/// Time-based deduplicator.
///
/// Tracks keys and their timestamps, preventing duplicate events within a window.
pub struct Deduplicator {
    window: Duration,
    entries: Mutex<HashMap<String, Instant>>,
}

impl Deduplicator {
    /// Create a new deduplicator with the given window.
    pub fn new(window: Duration) -> Self {
        Self { window, entries: Mutex::new(HashMap::new()) }
    }

    /// Check if a key is allowed at the given instant.
    ///
    /// Returns `true` if this is the first occurrence of the key within the window,
    /// and records the timestamp. Returns `false` if it's a duplicate.
    pub fn allow_at(&self, key: &str, now: Instant) -> bool {
        let mut entries = self.entries.lock();

        // Prune expired entries
        entries.retain(|_, timestamp| now.duration_since(*timestamp) <= self.window);

        // Check if key exists and is still within window
        if entries.get(key).is_some_and(|timestamp| now.duration_since(*timestamp) <= self.window) {
            return false;
        }

        entries.insert(key.to_string(), now);
        true
    }

    /// Clear all entries (useful for testing or reset).
    pub fn clear(&self) {
        self.entries.lock().clear();
    }

    /// Get the number of active entries.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_up_to_max() {
        let limiter = RateLimiter::new(Duration::from_secs(60), 3);
        let now = Instant::now();

        assert!(limiter.allow_at(now));
        assert!(limiter.allow_at(now));
        assert!(limiter.allow_at(now));
        assert!(!limiter.allow_at(now)); // 4th should be blocked
    }

    #[test]
    fn test_rate_limiter_window_expiry() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 2);
        let start = Instant::now();

        assert!(limiter.allow_at(start));
        assert!(limiter.allow_at(start));
        assert!(!limiter.allow_at(start));

        // Simulate time passing beyond window
        let later = start + Duration::from_millis(150);
        assert!(limiter.allow_at(later)); // Should be allowed again
    }

    #[test]
    fn test_deduplicator_blocks_repeat() {
        let dedup = Deduplicator::new(Duration::from_secs(60));
        let now = Instant::now();

        assert!(dedup.allow_at("key1", now));
        assert!(!dedup.allow_at("key1", now)); // Duplicate blocked
        assert!(dedup.allow_at("key2", now)); // Different key allowed
    }

    #[test]
    fn test_deduplicator_window_expiry() {
        let dedup = Deduplicator::new(Duration::from_millis(100));
        let start = Instant::now();

        assert!(dedup.allow_at("key", start));
        assert!(!dedup.allow_at("key", start));

        // Simulate time passing beyond window
        let later = start + Duration::from_millis(150);
        assert!(dedup.allow_at("key", later)); // Should be allowed again
    }

    #[test]
    fn test_deduplicator_clear() {
        let dedup = Deduplicator::new(Duration::from_secs(60));
        let now = Instant::now();

        dedup.allow_at("key", now);
        assert!(!dedup.is_empty());

        dedup.clear();
        assert!(dedup.is_empty());
        assert!(dedup.allow_at("key", now)); // Should be allowed after clear
    }
}
