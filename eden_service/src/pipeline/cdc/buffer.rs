//! Change buffer for CDC row events.
//!
//! Accumulates row changes from the WAL consumer and flushes them in batches
//! based on size threshold or time interval.

use serde_json::Value;
use std::collections::HashMap;
use tokio::time::Instant;

/// Type of change captured from WAL.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

/// A single row change decoded from the WAL stream.
#[derive(Debug, Clone)]
pub struct RowChange {
    /// The source table (schema-qualified, e.g., "public.orders").
    pub table: String,
    /// The type of change.
    pub kind: ChangeKind,
    /// Column values as a map of column name → JSON value.
    pub columns: HashMap<String, Value>,
    /// For UPDATE: the old column values (only for columns in REPLICA IDENTITY).
    pub old_columns: Option<HashMap<String, Value>>,
}

/// A batch of row changes ready to be written to the destination.
#[derive(Debug, Clone)]
pub struct ChangeBatch {
    /// The accumulated row changes.
    pub changes: Vec<RowChange>,
    /// The LSN to confirm after this batch is successfully written.
    pub confirm_lsn: String,
}

/// Buffers row changes and flushes based on size or time.
pub struct ChangeBuffer {
    changes: Vec<RowChange>,
    max_batch_size: u32,
    flush_interval_ms: u64,
    last_flush: Instant,
    /// The highest LSN seen so far (to confirm after flush).
    pending_lsn: Option<String>,
}

impl ChangeBuffer {
    pub fn new(max_batch_size: u32, flush_interval_ms: u64) -> Self {
        Self {
            changes: Vec::with_capacity(max_batch_size as usize),
            max_batch_size,
            flush_interval_ms,
            last_flush: Instant::now(),
            pending_lsn: None,
        }
    }

    /// Add a row change to the buffer.
    pub fn push(&mut self, change: RowChange, lsn: String) {
        self.changes.push(change);
        self.pending_lsn = Some(lsn);
    }

    /// Check if the buffer should be flushed (size or time threshold reached).
    pub fn should_flush(&self) -> bool {
        if self.changes.is_empty() {
            return false;
        }
        self.changes.len() >= self.max_batch_size as usize || self.last_flush.elapsed().as_millis() >= self.flush_interval_ms as u128
    }

    /// Take the current batch and reset the buffer.
    ///
    /// Returns `None` if the buffer is empty.
    pub fn take_batch(&mut self) -> Option<ChangeBatch> {
        if self.changes.is_empty() {
            return None;
        }

        let confirm_lsn = self.pending_lsn.take().unwrap_or_default();
        let changes = std::mem::take(&mut self.changes);
        self.changes = Vec::with_capacity(self.max_batch_size as usize);
        self.last_flush = Instant::now();

        Some(ChangeBatch { changes, confirm_lsn })
    }

    /// Number of changes currently buffered.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Time until the next time-based flush, if applicable.
    pub fn time_until_flush(&self) -> std::time::Duration {
        let elapsed = self.last_flush.elapsed();
        let interval = std::time::Duration::from_millis(self.flush_interval_ms);
        interval.saturating_sub(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_change(table: &str, kind: ChangeKind) -> RowChange {
        RowChange {
            table: table.to_string(),
            kind,
            columns: {
                let mut m = HashMap::new();
                m.insert("id".to_string(), json!(1));
                m
            },
            old_columns: None,
        }
    }

    #[test]
    fn test_buffer_size_trigger() {
        let mut buf = ChangeBuffer::new(3, 60_000);
        assert!(!buf.should_flush());

        buf.push(make_change("t", ChangeKind::Insert), "0/1".into());
        buf.push(make_change("t", ChangeKind::Insert), "0/2".into());
        assert!(!buf.should_flush());

        buf.push(make_change("t", ChangeKind::Insert), "0/3".into());
        assert!(buf.should_flush());

        let batch = buf.take_batch().expect("batch");
        assert_eq!(batch.changes.len(), 3);
        assert_eq!(batch.confirm_lsn, "0/3");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_buffer_empty() {
        let mut buf = ChangeBuffer::new(10, 5000);
        assert!(buf.take_batch().is_none());
        assert!(!buf.should_flush());
    }

    #[test]
    fn test_buffer_time_trigger() {
        let mut buf = ChangeBuffer::new(1000, 0); // 0ms flush interval = immediate
        buf.push(make_change("t", ChangeKind::Update), "0/1".into());
        assert!(buf.should_flush());
    }
}
