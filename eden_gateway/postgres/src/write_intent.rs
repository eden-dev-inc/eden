//! Per-connection write intent log for dual-write tracking (DW-1/DW-2/DW-5).
//!
//! Records every dual-write attempt so that:
//! - Failed secondary writes can be identified for replay (DW-2)
//! - The session affinity tracker knows which tables diverged (DW-5)
//! - Compensation decisions can reference what was sent (DW-1)

use bytes::Bytes;
use std::time::Instant;

/// Which side is authoritative for this write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativeSide {
    Old,
    New,
}

/// A record of a dual-write attempt.
#[derive(Debug, Clone)]
pub struct WriteIntent {
    /// Raw PG wire bytes that were sent.
    pub pg_bytes: Bytes,
    /// Extracted table name, if available.
    pub table_name: Option<String>,
    /// Which side was authoritative.
    pub authoritative_side: AuthoritativeSide,
    /// Whether the authoritative write succeeded.
    pub authoritative_ok: bool,
    /// Whether the secondary write succeeded.
    pub secondary_ok: bool,
    /// When the write was attempted.
    pub timestamp: Instant,
}

/// Per-connection log of dual-write intents.
///
/// Kept lightweight — only retains intents from the current
/// dual-write session. Cleared on migration deactivation.
pub struct SessionWriteLog {
    intents: Vec<WriteIntent>,
}

impl Default for SessionWriteLog {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionWriteLog {
    pub fn new() -> Self {
        Self { intents: Vec::new() }
    }

    /// Record a completed dual-write attempt.
    pub fn record(&mut self, intent: WriteIntent) {
        self.intents.push(intent);
    }

    /// Get intents where the secondary failed (for replay consideration).
    pub fn failed_secondary_intents(&self) -> impl Iterator<Item = &WriteIntent> {
        self.intents.iter().filter(|i| i.authoritative_ok && !i.secondary_ok)
    }

    /// Total number of recorded intents.
    pub fn len(&self) -> usize {
        self.intents.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.intents.is_empty()
    }

    /// Discard all intents (e.g., on migration deactivation).
    pub fn clear(&mut self) {
        self.intents.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_log() {
        let log = SessionWriteLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.failed_secondary_intents().count(), 0);
    }

    #[test]
    fn test_record_and_query() {
        let mut log = SessionWriteLog::new();
        log.record(WriteIntent {
            pg_bytes: Bytes::from_static(b"INSERT"),
            table_name: Some("users".to_string()),
            authoritative_side: AuthoritativeSide::Old,
            authoritative_ok: true,
            secondary_ok: false,
            timestamp: Instant::now(),
        });
        log.record(WriteIntent {
            pg_bytes: Bytes::from_static(b"UPDATE"),
            table_name: Some("orders".to_string()),
            authoritative_side: AuthoritativeSide::Old,
            authoritative_ok: true,
            secondary_ok: true,
            timestamp: Instant::now(),
        });

        assert_eq!(log.len(), 2);
        let failed: Vec<_> = log.failed_secondary_intents().collect();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn test_clear() {
        let mut log = SessionWriteLog::new();
        log.record(WriteIntent {
            pg_bytes: Bytes::from_static(b"DELETE"),
            table_name: None,
            authoritative_side: AuthoritativeSide::New,
            authoritative_ok: true,
            secondary_ok: false,
            timestamp: Instant::now(),
        });
        assert!(!log.is_empty());
        log.clear();
        assert!(log.is_empty());
    }
}
